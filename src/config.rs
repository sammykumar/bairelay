use bairelay_mqtt::discovery::Feature;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

// ── Enums ──────────────────────────────────────────────────────────────

#[derive(Debug, Default, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TlsClientAuth {
	#[default]
	None,
	Request,
	/// Bairelay accepts both `"require"` (the bairelay/clap-style spelling)
	/// and `"required"` (neolink's spelling) for drop-in compat.
	#[serde(alias = "required")]
	Require,
}

#[derive(Debug, Default, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StreamConfig {
	None,
	#[default]
	#[serde(alias = "both")]
	All,
	Main,
	Sub,
	Extern,
}

#[derive(Debug, Default, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DiscoveryMethod {
	Local,
	Remote,
	Map,
	#[default]
	Relay,
	Cellular,
}

#[derive(Debug, Default, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MaxEncryption {
	None,
	#[default]
	Aes,
	BcEncrypt,
}

// ── Top-level Config ───────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
	#[serde(rename = "bind", default = "default_bind_addr")]
	pub bind_addr: String,

	#[serde(default = "default_bind_port")]
	pub bind_port: u16,

	/// Path to a single PEM file containing the server's cert chain and
	/// private key (concatenated). Neolink-compat. When set, bairelay
	/// spawns a TLS listener on `tls_bind_port`.
	#[serde(default)]
	pub certificate: Option<String>,

	/// Client-cert mode for the TLS listener. Neolink-compat: accepts
	/// `"none"` / `"request"` / `"require"`, plus `"required"` as a serde
	/// alias for `Require`. Has no effect when `certificate` is unset.
	#[serde(default)]
	pub tls_client_auth: TlsClientAuth,

	/// TCP port for the TLS listener. Bairelay extension (neolink runs
	/// TLS on the same port and replaces plain). Default 8555 when
	/// `certificate` is set; ignored otherwise.
	#[serde(default)]
	pub tls_bind_port: Option<u16>,

	/// PEM bundle containing CA cert(s) used to verify client certs in
	/// `tls_client_auth = "request" | "require"` modes. Bairelay
	/// extension; required when `tls_client_auth` is non-`none`.
	#[serde(default)]
	pub tls_client_ca: Option<String>,

	#[serde(default)]
	pub users: Vec<UserConfig>,

	#[serde(default)]
	pub mqtt: Option<MqttServerConfig>,

	/// Optional `[wake_server]` block. Disabled by default; when `enable =
	/// true` the orchestrator spawns the local BcUdp wake server alongside
	/// RTSP and MQTT. Operators must also redirect DNS for
	/// `p2p*.reolink.com` to the bairelay box for cameras to register.
	#[serde(default)]
	pub wake_server: Option<bairelay_wake_server::WakeServerConfig>,

	/// Optional `[push_listener]` block. Disabled by default. When enabled,
	/// bairelay binds a TCP listener on `bind_addr:bind_port` and treats
	/// every incoming connection from a registered camera's IP as a
	/// motion event — fires `status/motion=on` and acquires a
	/// `motion_wake_hold_secs` wake-lock so the connect loop reconnects.
	/// Pairs with a DNS hijack of `pushx.reolink.com` → bairelay's IP at
	/// the operator's resolver. See `docs/cloud-interception.md` § Part II.
	#[serde(default)]
	pub push_listener: Option<PushListenerConfig>,

	/// Neolink-compat: tokio-console debugging knob. Bairelay does not
	/// integrate `tokio-console`; verbosity is driven by `RUST_LOG`
	/// (or `-v` / `-vv` / `-vvv` on the CLI). Accepted so old configs
	/// parse cleanly; produces a one-line warning at startup.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub tokio_console: Option<bool>,

	/// Grace window in seconds before an idle `StreamSource` is pruned by
	/// the watchdog. `0` restores the legacy instantaneous-prune behaviour
	/// shipped pre-Phase-2G; the default `30` smooths rapid RTSP
	/// disconnect/reconnect so clients don't pay a fresh `start_video`
	/// handshake on every reconnect. See `docs/architecture.md`.
	///
	/// **Invariant: keep this ≤ each camera's `idle_disconnect_timeout_secs`.**
	/// Otherwise an idle `StreamSource` outlives the underlying Baichuan
	/// session — a fast reconnect that lands during the gap finds a cached
	/// source pointing at a disconnected camera and pays a stale-broadcast
	/// then cold-wake penalty before recovering. The 30 s default sits
	/// strictly below the 45 s `idle_disconnect_timeout_secs` default by
	/// design. If an operator inverts the relationship,
	/// [`resolve_idle_disconnect_timeout`] silently clamps the per-camera
	/// value to `prune_grace + 15 s` and
	/// [`warn_idle_timeout_below_prune_floor`] logs a one-line warning at
	/// startup so the override is visible.
	///
	/// The watchdog sweeps every 30 s, so an idle source is actually
	/// dropped between `grace` and `grace + 30 s` after its last
	/// subscriber leaves. Don't expect sub-sweep precision.
	#[serde(default = "default_prune_grace_secs")]
	pub stream_prune_grace_secs: u64,

	pub cameras: Vec<CameraConfig>,
}

impl Default for Config {
	fn default() -> Self {
		Self {
			bind_addr: default_bind_addr(),
			bind_port: default_bind_port(),
			certificate: None,
			tls_client_auth: TlsClientAuth::default(),
			tls_bind_port: None,
			tls_client_ca: None,
			users: Vec::new(),
			mqtt: None,
			wake_server: None,
			push_listener: None,
			tokio_console: None,
			stream_prune_grace_secs: default_prune_grace_secs(),
			cameras: Vec::new(),
		}
	}
}

// ── PushListenerConfig ─────────────────────────────────────────────────

/// TCP listener that observes the camera's motion-time HTTPS
/// connect attempt to `pushx.reolink.com` and treats it as a motion
/// event. Operator-side DNS hijack for `pushx.reolink.com` →
/// `push_listener_addr` is the prerequisite. Listener doesn't
/// terminate TLS — the connection's existence is the signal; we
/// close immediately.
///
/// Field names are deliberately prefixed (`push_listener_addr` /
/// `push_listener_port`) rather than `bind_addr` / `bind_port` so an
/// operator pasting them into the wrong scope can't silently shadow
/// the top-level RTSP `bind_addr` and so the placement scanner has
/// no ambiguous keys to special-case.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PushListenerConfig {
	#[serde(default)]
	pub enable: bool,

	/// Bind address. `None` falls back (in priority order) to the
	/// `[wake_server]` bind, then the top-level `bind_addr`. Override
	/// when the wake-server box runs another HTTPS service (HA, nginx).
	#[serde(default)]
	pub push_listener_addr: Option<String>,

	/// Bind port. Default `443` matches `pushx.reolink.com`'s port —
	/// override when something else owns 443 on this host.
	#[serde(default = "default_push_listener_port")]
	pub push_listener_port: u16,

	/// How long a single push event holds the wake-lock. The connect
	/// loop has this long to establish a Baichuan session before the
	/// camera goes back to sleep. Default 30 s; lower values miss
	/// slow reconnects, higher values hold the camera awake longer.
	#[serde(default = "default_motion_wake_hold_secs")]
	pub motion_wake_hold_secs: f64,
}

impl Default for PushListenerConfig {
	fn default() -> Self {
		Self {
			enable: false,
			push_listener_addr: None,
			push_listener_port: default_push_listener_port(),
			motion_wake_hold_secs: default_motion_wake_hold_secs(),
		}
	}
}

fn default_push_listener_port() -> u16 {
	443
}

// ── UserConfig ─────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UserConfig {
	#[serde(alias = "username")]
	pub name: String,
	#[serde(alias = "password", default)]
	pub pass: String,
}

// ── MqttServerConfig ───────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MqttServerConfig {
	#[serde(alias = "server")]
	pub broker_addr: String,

	#[serde(default = "default_mqtt_port")]
	pub port: u16,

	#[serde(default)]
	pub credentials: Option<(String, String)>,

	#[serde(default)]
	pub ca: Option<String>,

	#[serde(default)]
	pub client_auth: Option<(String, String)>,

	/// Root segment for all bairelay-owned MQTT topics
	/// (`{topic_prefix}/{cam}/status`, etc.) and the driver of HA
	/// discovery identifier/unique_id strings. Default `"bairelay"`.
	/// Set to `"neolink"` for drop-in migration from an existing
	/// neolink deployment — every topic path and HA identifier flips
	/// in lockstep. See `docs/architecture.md`.
	#[serde(default = "default_topic_prefix")]
	pub topic_prefix: String,

	/// Home Assistant MQTT discovery settings. Presence of the
	/// `[mqtt.discovery]` table enables discovery; absence disables
	/// it entirely. Matches neolink's shape. See
	/// `docs/architecture.md`.
	#[serde(default)]
	pub discovery: Option<MqttDiscoveryConfig>,
}

// ── MqttDiscoveryConfig ────────────────────────────────────────────────

/// Configuration for publishing Home Assistant MQTT discovery
/// payloads. Operator must commit to a `topic` (HA's discovery
/// prefix — conventionally `"homeassistant"` but not hard-coded) and
/// may narrow `features` from the default full set.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MqttDiscoveryConfig {
	/// HA discovery root topic. Required; no default. Must be a
	/// single segment — no embedded slashes.
	pub topic: String,

	/// Set of HA discovery feature families to emit. Defaults to
	/// the full [`Feature::ALL`] set. Per-camera `enable_*` flags
	/// and live capability detection further gate what actually
	/// goes on the wire.
	#[serde(default = "default_features")]
	pub features: HashSet<Feature>,
}

fn default_features() -> HashSet<Feature> {
	Feature::ALL.iter().copied().collect()
}

// ── CameraConfig ───────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CameraConfig {
	pub name: String,

	#[serde(default)]
	pub address: Option<String>,

	#[serde(default)]
	pub uid: Option<String>,

	pub username: String,

	#[serde(alias = "pass", default)]
	pub password: Option<String>,

	#[serde(default, alias = "channel")]
	pub channel_id: u8,

	#[serde(default)]
	pub stream: StreamConfig,

	#[serde(default)]
	pub discovery: DiscoveryMethod,

	#[serde(default)]
	pub max_encryption: MaxEncryption,

	#[serde(default, alias = "idle", alias = "idle_disc")]
	pub idle_disconnect: bool,

	/// Seconds to wait after the last wake lock drops before the camera
	/// is told to sleep. `None` uses the 45 s default. A `[cameras.pause]
	/// timeout = N` (Neolink compat) overrides this at startup.
	#[serde(default)]
	pub idle_disconnect_timeout_secs: Option<f64>,

	/// Seconds the motion-driven wake lock is held after a `MotionStop`
	/// event before being released. Spec §4.2 calls this the "configurable
	/// timeout after motion stops"; this knob exposes it. Default 30 s.
	/// Lower values make battery cameras sleep sooner after motion ends;
	/// raise to keep RTSP / preview warm for a few extra seconds.
	#[serde(default = "default_motion_wake_hold_secs")]
	pub motion_wake_hold_secs: f64,

	/// Seconds after a session teardown during which UDP alarm packets
	/// from this camera are ignored. Cameras emit an alarm burst as part
	/// of tearing the Baichuan session down; without this window that
	/// burst re-arms the wake lock, which reconnects, which tears down
	/// again — a loop that never lets a battery camera sleep. Default
	/// 30 s; measured teardown bursts land 8–10 s after the disconnect.
	/// Set to 0 to disable (pre-0.3.5 behaviour).
	#[serde(default = "default_alarm_deaf_after_disconnect_secs")]
	pub alarm_deaf_after_disconnect_secs: f64,

	#[serde(default = "default_true", alias = "enable")]
	pub enabled: bool,

	#[serde(default)]
	pub mqtt: MqttConfig,

	#[serde(default)]
	pub pause: PauseConfig,

	/// Per-camera RTSP access control allowlist. Empty means "any
	/// authenticated user (or anonymous, when no auth is configured)".
	/// Task 24 will add validation that each entry matches a globally
	/// configured `[[users]]` name.
	#[serde(default)]
	pub permitted_users: Vec<String>,

	// ── Neolink migration compat (deprecated; warn + ignore) ──────────
	// Each Option<_> exists only so old configs parse cleanly under
	// `deny_unknown_fields`. `warn_neolink_compat_fields` emits a one-
	// line message per set field at startup pointing operators at the
	// bairelay equivalent (or stating that the knob is out of scope).
	#[serde(default, skip_serializing_if = "Option::is_none", alias = "verbose")]
	pub debug: Option<bool>,
	#[serde(default, skip_serializing_if = "Option::is_none", alias = "print")]
	pub print_format: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none", alias = "time")]
	pub update_time: Option<bool>,
	#[serde(
		default,
		skip_serializing_if = "Option::is_none",
		alias = "duration",
		alias = "buffer"
	)]
	pub buffer_duration: Option<u64>,
	#[serde(default, skip_serializing_if = "Option::is_none", alias = "splash")]
	pub use_splash: Option<bool>,
	#[serde(default, skip_serializing_if = "Option::is_none", alias = "pattern")]
	pub splash_pattern: Option<String>,
	#[serde(
		default,
		skip_serializing_if = "Option::is_none",
		alias = "retries",
		alias = "max_retries"
	)]
	pub max_discovery_retries: Option<u64>,
	#[serde(
		default,
		skip_serializing_if = "Option::is_none",
		alias = "push",
		alias = "push_noti"
	)]
	pub push_notifications: Option<bool>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub strict: Option<bool>,
}

impl Default for CameraConfig {
	fn default() -> Self {
		Self {
			name: String::new(),
			address: None,
			uid: None,
			username: String::new(),
			password: None,
			channel_id: 0,
			stream: StreamConfig::default(),
			discovery: DiscoveryMethod::default(),
			max_encryption: MaxEncryption::default(),
			idle_disconnect: false,
			idle_disconnect_timeout_secs: None,
			motion_wake_hold_secs: default_motion_wake_hold_secs(),
			alarm_deaf_after_disconnect_secs: default_alarm_deaf_after_disconnect_secs(),
			enabled: true,
			mqtt: MqttConfig::default(),
			pause: PauseConfig::default(),
			permitted_users: Vec::new(),
			debug: None,
			print_format: None,
			update_time: None,
			buffer_duration: None,
			use_splash: None,
			splash_pattern: None,
			max_discovery_retries: None,
			push_notifications: None,
			strict: None,
		}
	}
}

// ── MqttConfig (per-camera) ───────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MqttConfig {
	#[serde(default = "default_true")]
	pub enable_motion: bool,

	#[serde(default = "default_true")]
	pub enable_light: bool,

	#[serde(default = "default_true")]
	pub enable_battery: bool,

	#[serde(default = "default_2000")]
	pub battery_update: u64,

	#[serde(default = "default_true")]
	pub enable_preview: bool,

	#[serde(default = "default_2000")]
	pub preview_update: u64,

	/// Max width (px) the published preview JPEG is downscaled to before
	/// encoding. Aspect ratio is preserved and images are never enlarged.
	/// Bounds the payload so a full-res snapshot can't overflow the
	/// broker's max packet size and wedge the shared MQTT connection.
	#[serde(default = "default_preview_max_width")]
	pub preview_max_width: u32,

	/// JPEG quality (1..=100) the preview is re-encoded at after
	/// downscaling. Lower trims payload size further.
	#[serde(default = "default_preview_jpeg_quality")]
	pub preview_jpeg_quality: u8,

	/// Hard cap (bytes) on the encoded preview JPEG. If a rendered preview
	/// still exceeds this it is dropped (not published) so one oversize
	/// frame can't take down the connection for every camera.
	#[serde(default = "default_preview_max_bytes")]
	pub preview_max_bytes: usize,

	#[serde(default)]
	pub enable_floodlight: bool,

	#[serde(default = "default_2000")]
	pub floodlight_update: u64,

	#[serde(default)]
	pub enable_pir: bool,

	/// Neolink-compat: per-camera HA discovery override. Bairelay uses
	/// a global `[mqtt.discovery]` table only; this per-camera knob is
	/// accepted with a warning and has no effect. Operators should
	/// move the `topic` / `features` to the global table.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub discovery: Option<MqttDiscoveryConfig>,
}

impl Default for MqttConfig {
	fn default() -> Self {
		MqttConfig {
			enable_motion: true,
			enable_light: true,
			enable_battery: true,
			battery_update: 2000,
			enable_preview: true,
			preview_update: 2000,
			preview_max_width: 1280,
			preview_jpeg_quality: 75,
			preview_max_bytes: 262144,
			enable_floodlight: false,
			floodlight_update: 2000,
			discovery: None,
			enable_pir: false,
		}
	}
}

impl From<&MqttConfig> for bairelay_mqtt::discovery::CameraEnableFlags {
	/// Map the per-camera `[cameras.mqtt]` toggles onto the
	/// discovery-side `CameraEnableFlags`. Features without a
	/// per-camera flag (`camera`, `ir`, `reboot`, `pt`, `siren`) pass
	/// through unconditionally — gating is delegated to the global
	/// `features` config and live capability detection.
	fn from(m: &MqttConfig) -> Self {
		Self {
			motion: m.enable_motion,
			battery: m.enable_battery,
			floodlight: m.enable_floodlight,
			light: m.enable_light,
			pir: m.enable_pir,
		}
	}
}

impl From<&MqttConfig> for crate::preview_overlay::PreviewLimits {
	/// Carry the per-camera preview size-bounding knobs into the render
	/// path. Both preview publish sites build this from `[cameras.mqtt]`
	/// so the downscale + guard apply identically.
	fn from(m: &MqttConfig) -> Self {
		Self {
			max_width: m.preview_max_width,
			jpeg_quality: m.preview_jpeg_quality,
			max_bytes: m.preview_max_bytes,
		}
	}
}

// ── PauseConfig ────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PauseConfig {
	#[serde(default = "default_true")]
	pub bridge_gaps: bool,

	#[serde(default = "default_gap_threshold")]
	pub gap_threshold_secs: f64,

	#[serde(default = "default_true")]
	pub preview_overlay: bool,

	// ── Neolink migration compat (deprecated; warn + ignore) ─────────
	// These fields were valid in neolink's `[cameras.pause]` block
	// under the motion-driven pause model. Bairelay's gap-bridging
	// framing has no direct equivalent, so they are accepted to keep
	// old configs parsing but produce a migration warning at startup
	// and have no runtime effect.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub on_motion: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub on_client: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub on_disconnect: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub motion_timeout: Option<f64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub mode: Option<String>,

	/// Neolink migration compat: `timeout` was the pause-on-motion
	/// post-motion delay. Bairelay has no equivalent in the gap-
	/// bridging model, so we reuse this key to set the idle-disconnect
	/// grace period (the closest analogue — "seconds the camera lingers
	/// before sleeping"). Overrides the camera's
	/// `idle_disconnect_timeout_secs` if that field is unset.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub timeout: Option<f64>,
}

impl Default for PauseConfig {
	fn default() -> Self {
		PauseConfig {
			bridge_gaps: true,
			gap_threshold_secs: default_gap_threshold(),
			preview_overlay: true,
			on_motion: None,
			on_client: None,
			on_disconnect: None,
			motion_timeout: None,
			mode: None,
			timeout: None,
		}
	}
}

// ── Default value functions ────────────────────────────────────────────

fn default_bind_addr() -> String {
	"0.0.0.0".to_string()
}

fn default_bind_port() -> u16 {
	8554
}

fn default_mqtt_port() -> u16 {
	1883
}

fn default_topic_prefix() -> String {
	"bairelay".to_string()
}

const fn default_true() -> bool {
	true
}

const fn default_2000() -> u64 {
	2000
}

const fn default_preview_max_width() -> u32 {
	1280
}

const fn default_preview_jpeg_quality() -> u8 {
	75
}

const fn default_preview_max_bytes() -> usize {
	262144
}

fn default_gap_threshold() -> f64 {
	// Argus 4K-HEVC cameras deliver each GOP as a sub-second burst then
	// idle briefly before the next; observed inter-burst wallclock
	// varies 700–1400 ms even on a healthy stream. A 1 s threshold
	// misfired during normal operation, Bridging synth_pts then raced
	// past the camera's PTS, and clients (mpv, HA) saw 1 s backward
	// timestamp jumps + audio underruns every ~2 s. 3 s leaves headroom
	// for normal bursting while still catching multi-second silences
	// (camera waking, reconnecting).
	3.0
}

const fn default_prune_grace_secs() -> u64 {
	30
}

const fn default_motion_wake_hold_secs() -> f64 {
	30.0
}

/// Deaf window applied to UDP alarm packets after a session teardown.
/// 30 s is ~3x the 8-10 s teardown-burst latency measured on Argus
/// units, so it swallows the self-inflicted burst with margin while
/// leaving the camera able to report genuine motion the rest of the
/// time it is asleep.
const fn default_alarm_deaf_after_disconnect_secs() -> f64 {
	30.0
}

// ── Parsing ────────────────────────────────────────────────────────────

/// Top-level scalar config keys. Used by [`validate_top_level_key_placement`]
/// to detect operator typos where one of these is pasted *after* a `[section]`
/// header — TOML scopes the key to the open section instead of the document
/// root, and it then either fails serde with a cryptic "unknown field" or
/// (worse, before this layer existed) silently disappeared.
///
/// Includes serde aliases (e.g. `bind` for `bind_addr`) so an operator who
/// types either form gets caught.
const TOP_LEVEL_SCALAR_KEYS: &[&str] = &[
	"bind",
	"bind_addr",
	"bind_port",
	"certificate",
	"tls_bind_port",
	"tls_client_auth",
	"tls_client_ca",
	"tokio_console",
	"stream_prune_grace_secs",
];

/// Walk a parsed TOML document and flag any scalar key that lives inside
/// a non-root table but matches a known top-level Config field. Catches
/// the common typo:
///
/// ```toml
/// [mqtt]
/// broker_addr = "..."
///
/// certificate = "/path"   # ← lands inside [mqtt], not at the root
/// ```
///
/// Errors with one line per misplaced key naming the section path and
/// telling the operator how to fix. Called from [`parse_config`] before
/// serde deserialisation so the message is friendlier than serde's
/// "unknown field" or, worse, a silent drop on a struct that is missing
/// `deny_unknown_fields`.
pub fn validate_top_level_key_placement(value: &toml::Value) -> Result<(), String> {
	let toml::Value::Table(root) = value else {
		// Not a table at the document root — `parse_config` will surface
		// the real error from serde. Don't double-report.
		return Ok(());
	};
	let mut misplaced: Vec<(String, String)> = Vec::new();
	for (key, child) in root {
		walk_for_misplaced(key, child, &mut misplaced);
	}
	if misplaced.is_empty() {
		return Ok(());
	}
	let mut msg = String::from(
		"config: misplaced top-level keys (TOML scopes scalars to the most-recently-opened table; \
		 these belong at the document root, before any [section] header):\n",
	);
	for (path, key) in &misplaced {
		msg.push_str(&format!(
			"  - `{key}` is inside `[{path}]`. Move it to the document root, before the first [section] header.\n",
		));
	}
	Err(msg)
}

fn walk_for_misplaced(path: &str, value: &toml::Value, out: &mut Vec<(String, String)>) {
	match value {
		toml::Value::Table(t) => {
			for (k, v) in t {
				match v {
					toml::Value::Table(_) => {
						walk_for_misplaced(&format!("{path}.{k}"), v, out);
					}
					toml::Value::Array(arr) => {
						for (i, item) in arr.iter().enumerate() {
							if matches!(item, toml::Value::Table(_)) {
								walk_for_misplaced(&format!("{path}.{k}[{i}]"), item, out);
							}
						}
					}
					_ => {
						if TOP_LEVEL_SCALAR_KEYS.contains(&k.as_str()) {
							out.push((path.to_string(), k.clone()));
						}
					}
				}
			}
		}
		toml::Value::Array(arr) => {
			for (i, item) in arr.iter().enumerate() {
				if matches!(item, toml::Value::Table(_)) {
					walk_for_misplaced(&format!("{path}[{i}]"), item, out);
				}
			}
		}
		_ => {}
	}
}

/// Parse a TOML string into a Config. Runs the placement scanner
/// before serde deserialisation so misplaced top-level keys get a
/// dedicated diagnostic instead of serde's "unknown field" error.
pub fn parse_config(toml_str: &str) -> Result<Config, String> {
	let value: toml::Value =
		toml::from_str(toml_str).map_err(|e| format!("Failed to parse config: {e}"))?;
	validate_top_level_key_placement(&value)?;
	value
		.try_into()
		.map_err(|e: toml::de::Error| format!("Failed to parse config: {e}"))
}

// ── Validation ─────────────────────────────────────────────────────────

// ── Test helpers ──────────────────────────────────────────────────────

pub mod test_helpers {
	use super::*;

	pub fn minimal_camera_config(name: &str) -> CameraConfig {
		CameraConfig {
			name: name.to_string(),
			address: Some("192.168.1.1:9000".to_string()),
			username: "admin".to_string(),
			password: Some("test".to_string()),
			..Default::default()
		}
	}

	pub fn two_camera_config() -> Config {
		Config {
			cameras: vec![minimal_camera_config("cam1"), minimal_camera_config("cam2")],
			..Default::default()
		}
	}
}

/// Log a one-line migration warning for each deprecated neolink-style
/// `[cameras.pause]` field present in the parsed config. Called once at
/// startup after `parse_config` and before `validate_config`.
///
/// `on_motion` / `on_client` / `on_disconnect` / `motion_timeout` /
/// `mode` are accepted only so old configs continue to parse; they
/// have no runtime effect in the gap-bridging model.
///
/// `timeout` is kept as a soft alias for
/// [`CameraConfig::idle_disconnect_timeout_secs`] — see
/// [`resolve_idle_disconnect_timeout`].
pub fn warn_deprecated_pause_fields(config: &Config) {
	for cam in &config.cameras {
		let p = &cam.pause;
		if p.on_motion.is_some() {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras.pause] on_motion is deprecated (neolink migration compat); the motion-driven pause model is replaced by upstream gap-bridging. Remove this field."
			);
		}
		if p.on_client.is_some() {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras.pause] on_client is deprecated (neolink migration compat). Remove this field."
			);
		}
		if p.on_disconnect.is_some() {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras.pause] on_disconnect is deprecated (neolink migration compat). Remove this field."
			);
		}
		if p.motion_timeout.is_some() {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras.pause] motion_timeout is deprecated (neolink migration compat). Remove this field."
			);
		}
		if p.mode.is_some() {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras.pause] mode is deprecated (neolink migration compat); bairelay's overlay always draws the PreviewState label. Remove this field."
			);
		}
		if let Some(t) = p.timeout {
			if cam.idle_disconnect_timeout_secs.is_some() {
				tracing::warn!(
					camera = %cam.name,
					"config: [cameras.pause] timeout is deprecated and overridden by idle_disconnect_timeout_secs; remove this field."
				);
			} else {
				tracing::warn!(
					camera = %cam.name,
					seconds = t,
					"config: [cameras.pause] timeout is deprecated (neolink migration compat); mapped to idle_disconnect_timeout_secs. Move the value under [cameras] to silence this warning."
				);
			}
		}
	}
}

/// Log a one-line migration warning for each neolink-only field that
/// bairelay accepts (so old configs parse cleanly under
/// `deny_unknown_fields`) but has no runtime effect, paired with a
/// pointer to the bairelay equivalent. Called once at startup after
/// [`warn_deprecated_pause_fields`].
///
/// Top-level `tokio_console` and per-camera `debug` / `print_format` /
/// `update_time` / `buffer_duration` / `use_splash` / `splash_pattern` /
/// `max_discovery_retries` / `push_notifications` / `strict` are all
/// no-ops in bairelay; the per-camera `[cameras.mqtt] discovery` block
/// is also a no-op (bairelay uses a single global `[mqtt.discovery]`).
pub fn warn_neolink_compat_fields(config: &Config) {
	if config.tokio_console.is_some() {
		tracing::warn!(
			"config: tokio_console is a neolink debugging knob; bairelay drives verbosity via RUST_LOG (or -v / -vv / -vvv on the CLI). Remove this field."
		);
	}
	for cam in &config.cameras {
		if cam.debug.is_some() {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras] debug/verbose is a neolink per-camera flag; bairelay uses a global RUST_LOG (or -v / -vv / -vvv). Remove this field."
			);
		}
		if cam.print_format.is_some() {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras] print_format is a neolink stdout-dump knob; bairelay uses --dump-bcmedia <dir> on the CLI for fixture capture. Remove this field."
			);
		}
		if cam.update_time.is_some() {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras] update_time/time is a neolink connect-time clock-sync knob; bairelay exposes this via the `bairelay set-time <camera>` one-shot command. Remove this field."
			);
		}
		if cam.buffer_duration.is_some() {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras] buffer_duration/duration/buffer is a neolink GStreamer queue knob; bairelay's audio + video pacers handle bursty delivery internally. Remove this field."
			);
		}
		if cam.use_splash.is_some() {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras] use_splash/splash is a neolink stream-overlay toggle; bairelay's nearest equivalent is `[cameras.pause] preview_overlay` (default true), which captions the MQTT preview JPEG. Remove this field."
			);
		}
		if cam.splash_pattern.is_some() {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras] splash_pattern/pattern is a neolink GStreamer test-pattern selector; bairelay only renders CONNECTING / SLEEPING captions. Remove this field."
			);
		}
		if cam.max_discovery_retries.is_some() {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras] max_discovery_retries/retries/max_retries is a neolink discovery cap; bairelay uses exponential backoff with cancellation. Remove this field."
			);
		}
		if cam.push_notifications.is_some() {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras] push_notifications/push/push_noti is a neolink FCM toggle (deferred — spec §10); bairelay does not consume Reolink push at present. Remove this field."
			);
		}
		if cam.strict.is_some() {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras] strict is a neolink media-validation toggle; bairelay always validates parameter sets and NAL whitelists. Remove this field."
			);
		}
		if cam.mqtt.discovery.is_some() {
			tracing::warn!(
				camera = %cam.name,
				"config: [cameras.mqtt] discovery is a neolink per-camera HA discovery override; bairelay uses a single global [mqtt.discovery] table. Move topic/features there. Remove this field."
			);
		}
	}
}

/// Resolve the effective idle-disconnect grace period for a camera,
/// honouring the deprecated `[cameras.pause] timeout` alias. Falls back
/// to 45 s if neither is set.
///
/// **Runtime floor.** When the configured value is **strictly shorter**
/// than `prune_grace`, this returns `prune_grace + 15 s` instead — the
/// invariant on `Config::stream_prune_grace_secs` says a cached
/// `StreamSource` must not outlive the Baichuan session that feeds it,
/// so we clamp silently at resolution time and let
/// [`warn_idle_timeout_below_prune_floor`] log the operator-facing
/// warning once at startup. Equal values do not trigger the clamp:
/// at the moment the source prunes, the camera also disconnects, so
/// the windows just touch instead of overlapping. The 45/30 default
/// pair clears the floor with a 15 s margin by design.
pub fn resolve_idle_disconnect_timeout(
	cam: &CameraConfig,
	prune_grace: std::time::Duration,
) -> std::time::Duration {
	let secs = cam
		.idle_disconnect_timeout_secs
		.or(cam.pause.timeout)
		.unwrap_or(45.0);
	let configured = std::time::Duration::from_secs_f64(secs);
	if configured < prune_grace {
		prune_grace + std::time::Duration::from_secs(15)
	} else {
		configured
	}
}

/// Log a one-line config warning for each camera whose configured
/// `idle_disconnect_timeout_secs` (or `pause.timeout` alias) is shorter
/// than the global `stream_prune_grace_secs`. The runtime clamps the
/// value silently via [`resolve_idle_disconnect_timeout`]; this helper
/// surfaces the clamp to operators so they know their explicit config
/// was overridden. Called once at startup, alongside
/// [`warn_deprecated_pause_fields`].
pub fn warn_idle_timeout_below_prune_floor(config: &Config) {
	let prune = std::time::Duration::from_secs(config.stream_prune_grace_secs);
	let safe_floor = prune + std::time::Duration::from_secs(15);
	for cam in &config.cameras {
		let secs = cam
			.idle_disconnect_timeout_secs
			.or(cam.pause.timeout)
			.unwrap_or(45.0);
		let configured = std::time::Duration::from_secs_f64(secs);
		if configured < prune {
			tracing::warn!(
				camera = %cam.name,
				"config: idle_disconnect_timeout_secs ({:.1}s) is shorter than stream_prune_grace_secs ({}s); \
				 clamped at runtime to {}s so the cached StreamSource cannot outlive the Baichuan session. \
				 Raise idle_disconnect_timeout_secs (recommended >= {}s) or lower stream_prune_grace_secs.",
				configured.as_secs_f64(),
				prune.as_secs(),
				safe_floor.as_secs(),
				safe_floor.as_secs(),
			);
		}
	}
}

/// Effective TLS listener port (used when `certificate` is set).
pub const DEFAULT_TLS_BIND_PORT: u16 = 8555;

/// Validate a parsed Config. Returns Ok(()) or Err with a description.
pub fn validate_config(config: &Config) -> Result<(), String> {
	// Plain RTSP listener: bind_port = 0 means "skip plain", which is
	// only valid if the TLS listener will run instead. relaxed
	// the previous unconditional rejection.
	if config.bind_port == 0 && config.certificate.is_none() {
		return Err(
			"bind_port must be greater than 0 unless certificate is set (TLS-only mode)"
				.to_string(),
		);
	}

	// TLS validation. Only meaningful when certificate is set; else the
	// other TLS knobs are silently ignored to mirror neolink's behaviour
	// of starting plain when no cert is present.
	if config.certificate.is_some() {
		let tls_port = config.tls_bind_port.unwrap_or(DEFAULT_TLS_BIND_PORT);
		if tls_port == 0 {
			return Err("tls_bind_port must be greater than 0".to_string());
		}
		if config.bind_port != 0 && config.bind_port == tls_port {
			return Err(format!(
				"bind_port ({}) and tls_bind_port ({}) must differ",
				config.bind_port, tls_port
			));
		}
		if matches!(
			config.tls_client_auth,
			TlsClientAuth::Request | TlsClientAuth::Require
		) && config.tls_client_ca.is_none()
		{
			return Err(format!(
				"tls_client_auth = \"{}\" requires tls_client_ca to be set",
				match config.tls_client_auth {
					TlsClientAuth::Request => "request",
					TlsClientAuth::Require => "require",
					TlsClientAuth::None => unreachable!(),
				}
			));
		}
	} else if matches!(
		config.tls_client_auth,
		TlsClientAuth::Request | TlsClientAuth::Require
	) {
		return Err("tls_client_auth requires certificate to be set".to_string());
	}

	// Validate the MQTT topic prefix: non-empty, alphanumeric/_/- only.
	// Slashes are forbidden because the prefix is a single segment and
	// embedding one would silently shift every child topic.
	if let Some(ref mqtt) = config.mqtt {
		if mqtt.topic_prefix.is_empty() {
			return Err("mqtt.topic_prefix must not be empty".to_string());
		}
		// ASCII-only intentionally — `char::is_alphanumeric` is Unicode
		// (e.g. "café", "中文" pass) but the MQTT topic flows through
		// every downstream consumer (HA discovery, broker ACLs, log
		// formatters) under an ASCII-only assumption.
		if !mqtt
			.topic_prefix
			.chars()
			.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
		{
			return Err(format!(
				"mqtt.topic_prefix '{}' must contain only ASCII alphanumeric characters, underscores, and hyphens",
				mqtt.topic_prefix
			));
		}

		// HA discovery config: if present, the discovery root topic
		// must be a single non-empty segment. Slashes would split the
		// root and silently shift every child config topic out from
		// under HA.
		if let Some(ref d) = mqtt.discovery {
			if d.topic.is_empty() {
				return Err("mqtt.discovery.topic must not be empty".to_string());
			}
			if d.topic.contains('/') {
				return Err(format!(
					"mqtt.discovery.topic '{}' must not contain '/'",
					d.topic
				));
			}
		}
	}

	// Each RTSP user must have a non-empty password. Empty passwords
	// degrade both Basic and Digest auth to "accept any input that
	// hashes/encodes the empty string", which is indistinguishable from
	// disabling auth for that account and easy to trip over
	// accidentally. Refuse at startup so operators notice.
	let mut user_names_seen: HashSet<&str> = HashSet::new();
	for (i, user) in config.users.iter().enumerate() {
		if user.name.trim().is_empty() {
			return Err(format!("User #{i}: name must not be empty"));
		}
		if user.pass.is_empty() {
			return Err(format!(
				"User '{}' has an empty password; set a non-empty pass or remove the user",
				user.name
			));
		}
		if !user_names_seen.insert(user.name.as_str()) {
			return Err(format!("Duplicate user name: '{}'", user.name));
		}
	}

	let mut names = HashSet::new();

	for (i, cam) in config.cameras.iter().enumerate() {
		// No empty names
		if cam.name.trim().is_empty() {
			return Err(format!("Camera #{}: name must not be empty", i));
		}

		// Name must be MQTT-safe: ASCII alphanumeric + `_` + `-`. ASCII
		// is intentional — see the topic_prefix validation above for
		// the rationale (downstream consumers assume ASCII).
		if !cam
			.name
			.chars()
			.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
		{
			return Err(format!(
				"Camera '{}': name must contain only ASCII alphanumeric characters, underscores, and hyphens",
				cam.name
			));
		}

		// No duplicate names
		if !names.insert(&cam.name) {
			return Err(format!("Duplicate camera name: '{}'", cam.name));
		}

		// Password is required
		if cam.password.is_none() {
			return Err(format!("Camera '{}': password is required", cam.name));
		}

		// Camera must have address or uid
		if cam.address.is_none() && cam.uid.is_none() {
			return Err(format!(
				"Camera '{}': either address or uid must be provided",
				cam.name
			));
		}

		// MQTT update intervals >= 500
		if cam.mqtt.battery_update < 500 {
			return Err(format!(
				"Camera '{}': battery_update must be >= 500ms, got {}",
				cam.name, cam.mqtt.battery_update
			));
		}
		if cam.mqtt.preview_update < 500 {
			return Err(format!(
				"Camera '{}': preview_update must be >= 500ms, got {}",
				cam.name, cam.mqtt.preview_update
			));
		}
		if cam.mqtt.preview_max_width < 160 {
			return Err(format!(
				"Camera '{}': preview_max_width must be >= 160, got {}",
				cam.name, cam.mqtt.preview_max_width
			));
		}
		if cam.mqtt.preview_jpeg_quality < 1 || cam.mqtt.preview_jpeg_quality > 100 {
			return Err(format!(
				"Camera '{}': preview_jpeg_quality must be between 1 and 100, got {}",
				cam.name, cam.mqtt.preview_jpeg_quality
			));
		}
		if cam.mqtt.preview_max_bytes < 4096 {
			return Err(format!(
				"Camera '{}': preview_max_bytes must be >= 4096, got {}",
				cam.name, cam.mqtt.preview_max_bytes
			));
		}
		if cam.mqtt.floodlight_update < 500 {
			return Err(format!(
				"Camera '{}': floodlight_update must be >= 500ms, got {}",
				cam.name, cam.mqtt.floodlight_update
			));
		}

		// `Duration::from_secs_f64` panics on NaN, negative, infinite, or
		// otherwise non-representable values. The threshold is consumed
		// inside `CameraHandle::stream_source` long after parse, so any
		// bad value would surface as a late runtime panic. Refuse at
		// startup while the error still has context.
		if cam.pause.bridge_gaps
			&& (!cam.pause.gap_threshold_secs.is_finite() || cam.pause.gap_threshold_secs <= 0.0)
		{
			return Err(format!(
				"Camera '{}': pause.gap_threshold_secs must be a positive finite number (got {})",
				cam.name, cam.pause.gap_threshold_secs,
			));
		}

		// idle_disconnect_timeout_secs: same sanity check as above.
		if let Some(t) = cam.idle_disconnect_timeout_secs {
			if !t.is_finite() || t <= 0.0 {
				return Err(format!(
					"Camera '{}': idle_disconnect_timeout_secs must be a positive finite number (got {})",
					cam.name, t,
				));
			}
		}

		// Neolink pause.timeout migration alias: same sanity check.
		if let Some(t) = cam.pause.timeout {
			if !t.is_finite() || t <= 0.0 {
				return Err(format!(
					"Camera '{}': pause.timeout must be a positive finite number (got {})",
					cam.name, t,
				));
			}
		}

		// motion_wake_hold_secs: same sanity check. 0 is allowed (release
		// the wake lock immediately on Stop), but NaN / negative / infinite
		// would panic in `Duration::from_secs_f64`.
		if !cam.motion_wake_hold_secs.is_finite() || cam.motion_wake_hold_secs < 0.0 {
			return Err(format!(
				"Camera '{}': motion_wake_hold_secs must be a non-negative finite number (got {})",
				cam.name, cam.motion_wake_hold_secs,
			));
		}

		// alarm_deaf_after_disconnect_secs: same sanity check. 0 is
		// allowed (disables the window), but NaN / negative / infinite
		// would panic in `Duration::from_secs_f64`.
		if !cam.alarm_deaf_after_disconnect_secs.is_finite()
			|| cam.alarm_deaf_after_disconnect_secs < 0.0
		{
			return Err(format!(
				"Camera '{}': alarm_deaf_after_disconnect_secs must be a non-negative finite number (got {})",
				cam.name, cam.alarm_deaf_after_disconnect_secs,
			));
		}
	}

	// `[push_listener]` validation. The block itself is optional and the
	// enable=false case is a no-op; everything below is gated on enable.
	if let Some(ref pl) = config.push_listener {
		if pl.enable {
			// `Duration::from_secs_f64` panics on NaN / negative / infinite.
			// 0 is allowed — release the wake-lock immediately.
			if !pl.motion_wake_hold_secs.is_finite() || pl.motion_wake_hold_secs < 0.0 {
				return Err(format!(
					"[push_listener]: motion_wake_hold_secs must be a non-negative finite number (got {})",
					pl.motion_wake_hold_secs,
				));
			}
			// Operator config rejects port 0; the chosen ephemeral port is
			// never surfaced, so DNS-redirect from cameras can't reach it.
			if pl.push_listener_port == 0 {
				return Err(
					"[push_listener]: push_listener_port must be greater than 0".to_string()
				);
			}
			// The push listener depends on the shared `CameraRegistry` being
			// populated by the wake server's `D2R_HB` handler. Without it
			// every `lookup_by_ip` misses silently and motion never fires.
			let wake_enabled = config.wake_server.as_ref().is_some_and(|w| w.enable);
			if !wake_enabled {
				return Err(
					"[push_listener] requires [wake_server] enable = true; the push listener resolves peer IPs against the wake server's heartbeat registry"
						.to_string(),
				);
			}
		}
	}

	// Each entry of `permitted_users` on a camera must reference a globally
	// configured `[[users]]` name. This catches typos at startup rather
	// than at the first failed RTSP authentication.
	let user_names: HashSet<&str> = config.users.iter().map(|u| u.name.as_str()).collect();
	for camera in &config.cameras {
		for pu in &camera.permitted_users {
			if !user_names.contains(pu.as_str()) {
				return Err(format!(
					"camera '{}' permitted_users references unknown user '{}'",
					camera.name, pu
				));
			}
		}
	}

	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn wake_server_block_parses_with_enable_true() {
		let toml = r#"
			bind = "0.0.0.0"
			bind_port = 8554

			[wake_server]
			enable = true

			[[cameras]]
			name = "front"
			address = "10.0.0.10"
			username = "admin"
			password = "x"
		"#;
		let cfg: Config = toml::from_str(toml).expect("parse");
		let ws = cfg.wake_server.expect("wake_server present");
		assert!(ws.enable);
		assert_eq!(ws.middleman_port, 9999);
	}

	#[test]
	fn wake_server_block_absent_yields_none() {
		let toml = r#"
			bind = "0.0.0.0"
			bind_port = 8554

			[[cameras]]
			name = "front"
			address = "10.0.0.10"
			username = "admin"
			password = "x"
		"#;
		let cfg: Config = toml::from_str(toml).expect("parse");
		assert!(cfg.wake_server.is_none());
	}

	#[test]
	fn push_listener_block_parses_defaults() {
		let toml = r#"
			bind = "0.0.0.0"
			bind_port = 8554

			[push_listener]
			enable = true

			[[cameras]]
			name = "front"
			address = "10.0.0.10"
			username = "admin"
			password = "x"
		"#;
		let cfg: Config = toml::from_str(toml).expect("parse");
		let pl = cfg.push_listener.expect("push_listener present");
		assert!(pl.enable);
		assert_eq!(pl.push_listener_port, 443);
		assert!(pl.push_listener_addr.is_none());
		assert!((pl.motion_wake_hold_secs - 30.0).abs() < f64::EPSILON);
	}

	#[test]
	fn push_listener_block_absent_yields_none() {
		let toml = r#"
			bind = "0.0.0.0"
			bind_port = 8554

			[[cameras]]
			name = "front"
			address = "10.0.0.10"
			username = "admin"
			password = "x"
		"#;
		let cfg: Config = toml::from_str(toml).expect("parse");
		assert!(cfg.push_listener.is_none());
	}

	#[test]
	fn push_listener_overrides_addr_and_port() {
		let toml = r#"
			bind = "0.0.0.0"
			bind_port = 8554

			[push_listener]
			enable = true
			push_listener_addr = "10.0.0.5"
			push_listener_port = 8443
			motion_wake_hold_secs = 12.5

			[[cameras]]
			name = "front"
			address = "10.0.0.10"
			username = "admin"
			password = "x"
		"#;
		let cfg: Config = toml::from_str(toml).expect("parse");
		let pl = cfg.push_listener.expect("push_listener present");
		assert_eq!(pl.push_listener_addr.as_deref(), Some("10.0.0.5"));
		assert_eq!(pl.push_listener_port, 8443);
		assert!((pl.motion_wake_hold_secs - 12.5).abs() < f64::EPSILON);
	}

	#[test]
	fn push_listener_rejects_unknown_field() {
		let toml = r#"
			bind = "0.0.0.0"
			bind_port = 8554

			[push_listener]
			enable = true
			totally_made_up = "x"

			[[cameras]]
			name = "front"
			address = "10.0.0.10"
			username = "admin"
			password = "x"
		"#;
		assert!(toml::from_str::<Config>(toml).is_err());
	}
}
