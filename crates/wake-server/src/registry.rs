//! In-memory UID → address registry populated by `D2R_HB` heartbeats,
//! plus a per-UID session-state map populated by `M2D_Q_R` issuance.
//!
//! Lazy stale-on-lookup — entries are never proactively swept. A registry
//! lookup that finds an entry older than `stale_after` reads as `None`,
//! which is what the wake-handler treats as "camera not registered".
//!
//! Session-state is the bookkeeping the camera-side handshake needs:
//! when the middleman issues a `M2D_Q_R`, it stores the `(token, ac)`
//! pair we put in the reply so the register loop can echo back the same
//! `ac` in the subsequent `R2D_R_R` (live-verify proved cameras
//! anchor to that value).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Hard cap on each in-memory map (registered cameras + session
/// anchors). At design scale the realistic deployment has 1-100
/// cameras; 1024 leaves headroom for unusual fleets without giving an
/// attacker who can reach the wake-server's UDP ports an unbounded
/// memory-amplification vector. Inserts above the cap are refused
/// (logged at `warn` once per overflow).
pub const MAX_MAP_ENTRIES: usize = 1024;

/// Token + ac pair issued during `M2D_Q_R` and echoed in `R2D_R_R`.
#[derive(Debug, Clone, Copy)]
pub struct SessionAnchor {
	/// Echoed back by the camera in `D2R_R`.
	pub token: u64,
	/// Echoed back by the server in `R2D_R_R`.
	pub ac: u32,
	/// When this anchor was created — used for housekeeping (rotate on
	/// re-issue, drop on stale).
	pub issued_at: Instant,
}

/// Shared session-anchor map. Keyed by camera UID (long form including
/// the firmware suffix).
#[derive(Default)]
pub struct SessionAnchors {
	by_uid: Mutex<HashMap<String, SessionAnchor>>,
}

impl SessionAnchors {
	/// Empty constructor for [`std::sync::Arc::new`] sites.
	pub fn new() -> Self {
		Self::default()
	}

	/// Issue or refresh the `(token, ac)` pair for a given UID. Called
	/// during `M2D_Q_R`. Repeated calls overwrite — the camera always
	/// uses the latest pair from the most-recent `M2D_Q_R` it saw.
	///
	/// Returns `true` if the anchor was stored (or refreshed an
	/// existing entry), `false` if the map was already at
	/// [`MAX_MAP_ENTRIES`] AND the UID was new — in that case the
	/// caller should log a warn and drop the request rather than
	/// growing memory unbounded under a flood.
	pub fn issue(&self, uid: &str, token: u64, ac: u32, now: Instant) -> bool {
		let mut map = self.by_uid.lock().expect("SessionAnchors mutex poisoned");
		// Refreshing an existing UID is always allowed (no size growth).
		// Only new inserts are bounded.
		if map.len() >= MAX_MAP_ENTRIES && !map.contains_key(uid) {
			return false;
		}
		map.insert(
			uid.to_string(),
			SessionAnchor {
				token,
				ac,
				issued_at: now,
			},
		);
		true
	}

	/// Drop entries past `stale_after` and return the evicted UIDs.
	/// Mirrors [`CameraRegistry::purge_stale`]. Lazy: callers invoke
	/// from a hot path (e.g. heartbeat handler) so the map size never
	/// drifts past the rate of stale entries created.
	pub fn purge_stale(&self, now: Instant, stale_after: Duration) -> Vec<String> {
		let mut map = self.by_uid.lock().expect("SessionAnchors mutex poisoned");
		let stale: Vec<String> = map
			.iter()
			.filter(|(_, a)| now.saturating_duration_since(a.issued_at) > stale_after)
			.map(|(uid, _)| uid.clone())
			.collect();
		for uid in &stale {
			map.remove(uid);
		}
		stale
	}

	/// Look up the anchor for a UID. Returns `None` if no `M2D_Q_R` has
	/// been issued for this UID, OR if the anchor is older than the
	/// supplied TTL (caller decides the policy).
	pub fn lookup(&self, uid: &str, now: Instant, stale_after: Duration) -> Option<SessionAnchor> {
		self.by_uid
			.lock()
			.expect("SessionAnchors mutex poisoned")
			.get(uid)
			.copied()
			.filter(|a| now.saturating_duration_since(a.issued_at) <= stale_after)
	}

	/// Total number of anchors currently held; observability helper.
	pub fn len(&self) -> usize {
		self.by_uid
			.lock()
			.expect("SessionAnchors mutex poisoned")
			.len()
	}

	/// True when no anchors are held (clippy `len_without_is_empty`).
	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}
}

/// Snapshot of a camera's most recent heartbeat — what the wake handler needs
/// to forward `R2C_T` toward the device.
#[derive(Debug, Clone)]
pub struct CameraEntry {
	/// Source address the heartbeat arrived from; where wake replies are sent.
	pub addr: SocketAddr,
	/// Camera-supplied session token echoed back to operators / future ops
	/// (currently observability only).
	pub token: u64,
	/// Wall-clock-free timestamp used for stale-on-lookup eviction.
	pub last_seen: Instant,
	/// When this UID first appeared (or reappeared after eviction).
	/// Refreshes leave it alone, so it reads as "connected since".
	pub first_seen: Instant,
}

/// Concurrent UID → `CameraEntry` table. Cheap to share across tasks; all
/// access is mutex-guarded internally.
#[derive(Default)]
pub struct CameraRegistry {
	cameras: Mutex<HashMap<String, CameraEntry>>,
}

impl CameraRegistry {
	/// Empty registry; populated as `D2R_HB` heartbeats arrive.
	pub fn new() -> Self {
		Self::default()
	}

	/// Insert or overwrite. Refreshes `last_seen` to `now`. Returns
	/// `Some(true)` if the UID was newly registered, `Some(false)` if
	/// this refreshed an existing entry, and `None` if the map was
	/// full ([`MAX_MAP_ENTRIES`]) and the UID was new — in that case
	/// the caller should log a warn and drop the request. The cap
	/// stops a flood of D2R_HB-with-random-UIDs from growing memory
	/// unbounded.
	pub fn upsert(&self, uid: &str, addr: SocketAddr, token: u64, now: Instant) -> Option<bool> {
		let mut map = self.cameras.lock().expect("CameraRegistry mutex poisoned");
		// Refreshes never grow the map; only new inserts are bounded.
		if map.len() >= MAX_MAP_ENTRIES && !map.contains_key(uid) {
			return None;
		}
		let first_seen = map.get(uid).map_or(now, |e| e.first_seen);
		let prev = map.insert(
			uid.to_string(),
			CameraEntry {
				addr,
				token,
				last_seen: now,
				first_seen,
			},
		);
		Some(prev.is_none())
	}

	/// Drop entries past `stale_after` and return the evicted UIDs.
	/// Callers log info on each eviction so operators see deregistration.
	/// Lazy: the registry has no background sweeper, so call this from
	/// hot paths (heartbeat / connect handlers) whenever there's
	/// something to log against.
	pub fn purge_stale(&self, now: Instant, stale_after: Duration) -> Vec<String> {
		let mut map = self.cameras.lock().expect("CameraRegistry mutex poisoned");
		let stale: Vec<String> = map
			.iter()
			.filter(|(_, e)| now.saturating_duration_since(e.last_seen) > stale_after)
			.map(|(uid, _)| uid.clone())
			.collect();
		for uid in &stale {
			map.remove(uid);
		}
		stale
	}

	/// Lookup honouring `stale_after`. Entries older than `stale_after`
	/// from `now` read as `None`.
	///
	/// **Prefix-match fallback:** Argus firmware reports UIDs in their
	/// long form (config UID + 4-char firmware suffix) on `D2R_HB`, while
	/// `C2R_C` from bairelay-itself / neolink sends the short configured
	/// UID. If exact match fails, fall back to scanning for any stored
	/// UID that **starts with** the requested UID — that handles the
	/// short→long mismatch without requiring operators to know about the
	/// suffix.
	pub fn lookup_fresh(
		&self,
		uid: &str,
		now: Instant,
		stale_after: Duration,
	) -> Option<CameraEntry> {
		let map = self.cameras.lock().expect("CameraRegistry mutex poisoned");
		let fresh = |e: &CameraEntry| now.saturating_duration_since(e.last_seen) <= stale_after;
		if let Some(e) = map.get(uid).filter(|e| fresh(e)) {
			return Some(e.clone());
		}
		map.iter()
			.find(|(stored, e)| stored.starts_with(uid) && fresh(e))
			.map(|(_, e)| e.clone())
	}

	/// Reverse lookup: find the UID + entry whose `D2R_HB` source IP
	/// matches `ip`. Used by the push-listener: the camera's
	/// motion-time TCP connection to `pushx.reolink.com` carries the
	/// same source IP as its UDP heartbeats — different ephemeral port,
	/// same host — so an IP match is the link from "TCP accept on :443"
	/// to "this UID's CameraHandle". Stale entries are excluded.
	pub fn lookup_by_ip(
		&self,
		ip: std::net::IpAddr,
		now: Instant,
		stale_after: Duration,
	) -> Option<(String, CameraEntry)> {
		let map = self.cameras.lock().expect("CameraRegistry mutex poisoned");
		map.iter()
			.find(|(_, e)| {
				e.addr.ip() == ip && now.saturating_duration_since(e.last_seen) <= stale_after
			})
			.map(|(uid, e)| (uid.clone(), e.clone()))
	}

	/// Total registered UID count; used by tests + observability.
	pub fn len(&self) -> usize {
		self.cameras
			.lock()
			.expect("CameraRegistry mutex poisoned")
			.len()
	}

	/// Convenience pair to `len()`; clippy expects it whenever `len()` exists.
	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::net::SocketAddr;

	fn addr(s: &str) -> SocketAddr {
		s.parse().unwrap()
	}
	fn now() -> Instant {
		Instant::now()
	}

	#[test]
	fn upsert_then_lookup_returns_entry() {
		let reg = CameraRegistry::new();
		let t = now();
		reg.upsert("UID1", addr("10.0.0.1:5000"), 42, t);
		let e = reg
			.lookup_fresh("UID1", t, Duration::from_secs(60))
			.unwrap();
		assert_eq!(e.addr, addr("10.0.0.1:5000"));
		assert_eq!(e.token, 42);
	}

	#[test]
	fn lookup_missing_returns_none() {
		let reg = CameraRegistry::new();
		assert!(reg
			.lookup_fresh("NOPE", now(), Duration::from_secs(60))
			.is_none());
	}

	#[test]
	fn upsert_overwrites_address_and_token() {
		let reg = CameraRegistry::new();
		let t = now();
		reg.upsert("UID1", addr("10.0.0.1:5000"), 42, t);
		let t2 = t + Duration::from_secs(1);
		reg.upsert("UID1", addr("10.0.0.2:6000"), 99, t2);
		let e = reg
			.lookup_fresh("UID1", t2, Duration::from_secs(60))
			.unwrap();
		assert_eq!(e.addr, addr("10.0.0.2:6000"));
		assert_eq!(e.token, 99);
	}

	#[test]
	fn lookup_returns_none_past_ttl() {
		let reg = CameraRegistry::new();
		let inserted = now();
		reg.upsert("UID1", addr("10.0.0.1:5000"), 1, inserted);
		let later = inserted + Duration::from_secs(120);
		let stale = Duration::from_secs(60);
		assert!(reg.lookup_fresh("UID1", later, stale).is_none());
	}

	#[test]
	fn lookup_at_ttl_boundary_returns_some() {
		let reg = CameraRegistry::new();
		let t = now();
		reg.upsert("UID1", addr("10.0.0.1:5000"), 1, t);
		let stale = Duration::from_secs(60);
		let on_edge = t + stale;
		assert!(reg.lookup_fresh("UID1", on_edge, stale).is_some());
	}

	#[test]
	fn is_empty_returns_true_when_unpopulated_then_false_after_upsert() {
		let reg = CameraRegistry::new();
		assert!(reg.is_empty());
		reg.upsert("UID1", addr("10.0.0.1:5000"), 1, now());
		assert!(!reg.is_empty());
	}

	#[test]
	fn lookup_by_ip_finds_entry_when_ip_matches() {
		let reg = CameraRegistry::new();
		let t = now();
		reg.upsert("UID1", addr("10.0.0.1:5000"), 1, t);
		reg.upsert("UID2", addr("10.0.0.2:6000"), 2, t);
		let (uid, entry) = reg
			.lookup_by_ip("10.0.0.2".parse().unwrap(), t, Duration::from_secs(60))
			.expect("entry for 10.0.0.2");
		assert_eq!(uid, "UID2");
		assert_eq!(entry.token, 2);
	}

	#[test]
	fn lookup_by_ip_returns_none_when_no_ip_matches() {
		let reg = CameraRegistry::new();
		reg.upsert("UID1", addr("10.0.0.1:5000"), 1, now());
		assert!(reg
			.lookup_by_ip("10.0.0.9".parse().unwrap(), now(), Duration::from_secs(60))
			.is_none());
	}

	#[test]
	fn lookup_by_ip_skips_stale_entries() {
		let reg = CameraRegistry::new();
		let t = now();
		reg.upsert("UID1", addr("10.0.0.1:5000"), 1, t);
		let later = t + Duration::from_secs(120);
		assert!(reg
			.lookup_by_ip("10.0.0.1".parse().unwrap(), later, Duration::from_secs(60))
			.is_none());
	}

	#[test]
	fn upsert_sets_first_seen_on_insert_and_preserves_it_on_refresh() {
		let reg = CameraRegistry::new();
		let t = now();
		reg.upsert("UID1", addr("10.0.0.1:1"), 1, t);
		let later = t + Duration::from_secs(30);
		reg.upsert("UID1", addr("10.0.0.1:1"), 1, later);
		let e = reg
			.lookup_fresh("UID1", later, Duration::from_secs(60))
			.unwrap();
		assert_eq!(e.first_seen, t, "refresh must not move first_seen");
		assert_eq!(e.last_seen, later);
	}

	#[test]
	fn len_tracks_distinct_uids() {
		let reg = CameraRegistry::new();
		let t = now();
		reg.upsert("A", addr("10.0.0.1:1"), 1, t);
		reg.upsert("B", addr("10.0.0.2:2"), 2, t);
		reg.upsert("A", addr("10.0.0.3:3"), 3, t);
		assert_eq!(reg.len(), 2);
	}

	#[test]
	fn upsert_returns_some_true_on_first_insert_some_false_on_refresh() {
		let reg = CameraRegistry::new();
		let t = now();
		assert_eq!(reg.upsert("UID1", addr("10.0.0.1:1"), 1, t), Some(true));
		assert_eq!(reg.upsert("UID1", addr("10.0.0.1:1"), 2, t), Some(false));
		assert_eq!(reg.upsert("UID2", addr("10.0.0.2:2"), 3, t), Some(true));
	}

	/// Once the registry is at MAX_MAP_ENTRIES, new UIDs are rejected
	/// (returns None). Refreshes of existing UIDs still succeed —
	/// otherwise legitimate cameras would lose their slot under
	/// attack.
	#[test]
	fn upsert_rejects_new_uid_when_at_capacity_but_allows_refresh() {
		let reg = CameraRegistry::new();
		let t = now();
		// Fill to capacity.
		for i in 0..MAX_MAP_ENTRIES {
			let uid = format!("UID{i}");
			assert_eq!(
				reg.upsert(&uid, addr(&format!("10.0.0.{}:1", i % 250 + 1)), 1, t),
				Some(true)
			);
		}
		// New UID is rejected.
		assert_eq!(reg.upsert("OVERFLOW", addr("10.0.0.1:1"), 1, t), None);
		// Existing UID can still refresh (legitimate camera survives).
		assert_eq!(
			reg.upsert("UID0", addr("10.0.0.1:1"), 99, t + Duration::from_secs(1)),
			Some(false)
		);
	}

	#[test]
	fn purge_stale_evicts_only_past_ttl_and_returns_uids() {
		let reg = CameraRegistry::new();
		let t = now();
		reg.upsert("OLD", addr("10.0.0.1:1"), 1, t);
		reg.upsert("FRESH", addr("10.0.0.2:2"), 2, t + Duration::from_secs(50));
		let later = t + Duration::from_secs(80);
		let evicted = reg.purge_stale(later, Duration::from_secs(60));
		assert_eq!(evicted, vec!["OLD".to_string()]);
		assert_eq!(reg.len(), 1);
		assert!(reg
			.lookup_fresh("FRESH", later, Duration::from_secs(60))
			.is_some());
	}

	#[test]
	fn purge_stale_is_noop_when_nothing_is_stale() {
		let reg = CameraRegistry::new();
		let t = now();
		reg.upsert("UID1", addr("10.0.0.1:1"), 1, t);
		assert!(reg.purge_stale(t, Duration::from_secs(60)).is_empty());
		assert_eq!(reg.len(), 1);
	}
}
