//! Shared tail of every out-of-session motion edge.
//!
//! Two subsystems detect motion while the Baichuan session is down: the
//! push listener (TCP connect to `pushx.reolink.com`) and the wake
//! server's UDP alarm handler. Both end the same way — publish
//! `status/motion=on`, hold a wake-lock so the connect loop brings the
//! camera up, then publish a fallback `status/motion=off`. That tail
//! lives here so the two paths cannot drift.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use bairelay_mqtt::{SharedMqttClient, StatusPublisher};

use crate::camera::CameraHandle;

/// Spawn the motion pipeline for `handle` in a detached task so the
/// caller's accept / recv loop returns to listening immediately.
/// Detached tasks are bounded by `hold` plus the cancel token, so they
/// can't outlive shutdown. With no MQTT client configured the
/// wake-lock alone still drives the connect loop.
pub fn fire(
	handle: Arc<CameraHandle>,
	mqtt: Option<SharedMqttClient>,
	topic_prefix: &str,
	hold: Duration,
	cancel: CancellationToken,
) {
	let Some(mqtt) = mqtt else {
		spawn_wake_only(handle, hold, cancel);
		return;
	};
	let topic_prefix = topic_prefix.to_string();
	tokio::spawn(async move {
		fire_motion(handle, mqtt, topic_prefix, hold, cancel).await;
	});
}

/// Resolve a `(name → CameraHandle)` map entry from a camera UID seen on
/// the wire. Cameras report a long-form UID (configured UID + 4-char
/// firmware suffix) on `D2R_HB` and the short form in the alarm header,
/// so the match runs in both directions — either side may be the
/// prefix. Determinism mirrors
/// [`crate::push_listener::match_camera_by_uid`]: the longest configured
/// UID wins, never the HashMap-iteration-order accident.
pub(crate) fn resolve_camera<'a>(
	cameras: &'a HashMap<String, Arc<CameraHandle>>,
	wire_uid: &str,
) -> Option<&'a Arc<CameraHandle>> {
	cameras
		.values()
		.filter(|h| {
			h.config().uid.as_deref().is_some_and(|cfg_uid| {
				wire_uid.starts_with(cfg_uid) || cfg_uid.starts_with(wire_uid)
			})
		})
		.max_by_key(|h| h.config().uid.as_deref().map_or(0, str::len))
}

/// Adapter that lets the wake server's UDP alarm handler reach the
/// motion pipeline without the wake-server crate knowing about camera
/// handles or MQTT. The hold window is each camera's own
/// `motion_wake_hold_secs`.
pub struct AlarmMotionSink {
	cameras: Arc<HashMap<String, Arc<CameraHandle>>>,
	mqtt: Option<SharedMqttClient>,
	topic_prefix: String,
	cancel: CancellationToken,
}

impl AlarmMotionSink {
	pub fn new(
		cameras: Arc<HashMap<String, Arc<CameraHandle>>>,
		mqtt: Option<SharedMqttClient>,
		topic_prefix: String,
		cancel: CancellationToken,
	) -> Self {
		Self {
			cameras,
			mqtt,
			topic_prefix,
			cancel,
		}
	}
}

impl bairelay_wake_server::AlarmSink for AlarmMotionSink {
	fn on_alarm(&self, uid: &str, counter: u32) {
		let Some(handle) = resolve_camera(&self.cameras, uid) else {
			tracing::debug!(
				uid = %uid,
				"alarm UID has no camera handle with a matching configured UID"
			);
			return;
		};
		tracing::info!(
			camera = %handle.name(),
			uid = %uid,
			counter,
			"Motion alarm from camera (treating UDP alarm packet as motion edge)"
		);
		let hold = Duration::from_secs_f64(handle.config().motion_wake_hold_secs);
		fire(
			Arc::clone(handle),
			self.mqtt.clone(),
			&self.topic_prefix,
			hold,
			self.cancel.clone(),
		);
	}
}

/// Fire the motion event: publish `status/motion=on`, hold a wake-lock
/// for `hold`, then publish a fallback `status/motion=off` so HA never
/// gets stuck on `on` if the in-session `motion_listener` never picks
/// up the live `Stop`. Idempotent w.r.t. the in-session publisher —
/// duplicate `motion=off` is harmless (HA dedups).
async fn fire_motion(
	handle: Arc<CameraHandle>,
	mqtt: SharedMqttClient,
	topic_prefix: String,
	hold: Duration,
	cancel: CancellationToken,
) {
	let _guard = handle.wake_lock().acquire();
	let publisher = StatusPublisher::new(&mqtt, &topic_prefix, handle.name());
	if let Err(e) = publisher.publish_motion(true).await {
		tracing::warn!(camera = %handle.name(), error = %e, "motion: publish_motion(true) failed");
	}
	handle.status_cache().set_motion(true);

	// Hold for `hold`, bailing on cancel — same primitive used by the
	// camera reconnect path so the contract lives in one place.
	crate::run_support::sleep_or_cancel(hold, &cancel).await;

	// Cap the fallback publish so a wedged broker (Ctrl+C race against
	// detached fire_motion tasks) can't hold the runtime open during
	// shutdown. 1 s is generous for an alive broker; on shutdown, a
	// dead broker hits the timeout and we move on quietly.
	const FALLBACK_PUBLISH_TIMEOUT: Duration = Duration::from_secs(1);
	match tokio::time::timeout(FALLBACK_PUBLISH_TIMEOUT, publisher.publish_motion(false)).await {
		Ok(Ok(())) => {
			handle.status_cache().set_motion(false);
		}
		Ok(Err(e)) => {
			tracing::warn!(camera = %handle.name(), error = %e, "motion: fallback publish_motion(false) failed");
		}
		Err(_) => {
			tracing::debug!(
				camera = %handle.name(),
				"motion: fallback publish_motion(false) timed out (likely shutdown)"
			);
		}
	}
}

/// Wake-lock-only path used when no MQTT client is configured. Holds
/// the lock for the same window so the connect loop has time to land.
fn spawn_wake_only(handle: Arc<CameraHandle>, hold: Duration, cancel: CancellationToken) {
	tokio::spawn(async move {
		let _guard = handle.wake_lock().acquire();
		crate::run_support::sleep_or_cancel(hold, &cancel).await;
	});
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::config::test_helpers::minimal_camera_config;

	fn handle_with_uid(name: &str, uid: Option<&str>) -> Arc<CameraHandle> {
		let mut cfg = minimal_camera_config(name);
		cfg.uid = uid.map(str::to_string);
		Arc::new(CameraHandle::new(cfg, CancellationToken::new(), None))
	}

	#[test]
	fn resolve_camera_matches_short_wire_uid_against_long_config_uid() {
		// Alarm headers carry the 16-char sticker UID; an operator may
		// have configured the 20-char firmware form.
		let mut cameras: HashMap<String, Arc<CameraHandle>> = HashMap::new();
		cameras.insert(
			"backyard_2".into(),
			handle_with_uid("backyard_2", Some("9527000960V21RLKABCD")),
		);
		let h = resolve_camera(&cameras, "9527000960V21RLK").expect("match");
		assert_eq!(h.name(), "backyard_2");
	}

	#[test]
	fn resolve_camera_matches_long_wire_uid_against_short_config_uid() {
		let mut cameras: HashMap<String, Arc<CameraHandle>> = HashMap::new();
		cameras.insert(
			"backyard_2".into(),
			handle_with_uid("backyard_2", Some("9527000960V21RLK")),
		);
		let h = resolve_camera(&cameras, "9527000960V21RLKABCD").expect("match");
		assert_eq!(h.name(), "backyard_2");
	}

	#[test]
	fn resolve_camera_returns_none_on_unrelated_uid() {
		let mut cameras: HashMap<String, Arc<CameraHandle>> = HashMap::new();
		cameras.insert(
			"backyard_3".into(),
			handle_with_uid("backyard_3", Some("9527000960WLCGYZ")),
		);
		assert!(resolve_camera(&cameras, "9527000960V21RLK").is_none());
	}

	#[test]
	fn resolve_camera_prefers_longest_configured_uid() {
		let mut cameras: HashMap<String, Arc<CameraHandle>> = HashMap::new();
		cameras.insert("short".into(), handle_with_uid("short", Some("UID-A")));
		cameras.insert("long".into(), handle_with_uid("long", Some("UID-AB")));
		let h = resolve_camera(&cameras, "UID-AB-FW1234").expect("match");
		assert_eq!(h.name(), "long");
	}
}
