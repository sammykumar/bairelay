//! Per-camera async tasks that run while a camera session is active.
//!
//! Each task is spawned when a camera connects and cancelled when the
//! connection drops (via a session-scoped `CancellationToken`).

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use neolink_core::bc_protocol::{CameraDriver, MotionStatus};

use bairelay_mqtt::{SharedMqttClient, StatusPublisher};
use bairelay_rtsp::buffer::LastFrameBuffer;

use crate::camera::ReconnectBackoff;
use crate::preview_overlay::{OverlayCache, PreviewLimits};
use crate::preview_state::PreviewState;
use crate::status_cache::StatusCache;
use crate::wake_lock::{WakeLockCounter, WakeLockGuard};

// ── Motion Detection ─────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
pub async fn motion_listener(
	camera_name: String,
	bc_camera: Arc<dyn CameraDriver>,
	mqtt: SharedMqttClient,
	topic_prefix: String,
	wake_lock: WakeLockCounter,
	cancel: CancellationToken,
	motion_wake_hold: Duration,
	status_cache: Arc<StatusCache>,
) {
	let publisher = StatusPublisher::new(&mqtt, &topic_prefix, &camera_name);
	// Sustained network flap with N cameras × M retries/min produces a
	// firehose of warns and pointless reconnect attempts. Use the same
	// 1 → 60 s exponential ladder as the camera reconnect path; reset
	// to 1 s every time we successfully subscribe so a transient blip
	// doesn't permanently inflate the retry interval.
	let mut backoff = ReconnectBackoff::new(Duration::from_secs(1), Duration::from_secs(60));

	loop {
		let mut motion_data = match bc_camera.listen_on_motion().await {
			Ok(md) => md,
			Err(e) => {
				tracing::warn!(camera = %camera_name, error = %e, "Failed to start motion listener");
				if !backoff.sleep_with_cancel(&cancel).await {
					break;
				}
				continue;
			}
		};
		// Subscribed cleanly — fast retries again on the next failure.
		backoff.reset();

		let mut wake_guard: Option<WakeLockGuard> = None;
		// Pending wake-lock release deadline. `Some(_)` while a Stop has
		// been observed and we're inside the post-motion hold-down
		// window; cleared by either the timer firing (release) or a
		// fresh Start (re-arm). Polled inline via the select! arm below
		// — no detached `tokio::spawn`, so the count of in-flight
		// "release N seconds from now" futures is at most one per
		// listener at any time, regardless of motion-event flap rate.
		let mut release_at: Option<tokio::time::Instant> = None;

		loop {
			// `sleep_until` requires a concrete deadline. When no
			// release is scheduled, point it 24 h out as a placeholder
			// — the `if release_at.is_some()` guard disables that arm,
			// so the placeholder is never actually reached.
			let release_deadline = release_at
				.unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(86_400));

			tokio::select! {
				_ = cancel.cancelled() => return,
				_ = tokio::time::sleep_until(release_deadline), if release_at.is_some() => {
					// Hold-down expired with no new motion — drop the
					// wake-lock guard so the camera becomes eligible
					// for idle disconnect.
					wake_guard = None;
					release_at = None;
				}
				result = motion_data.next_motion() => {
					match result {
						Ok(MotionStatus::Start(_)) => {
							tracing::info!(camera = %camera_name, "Motion detected");
							// New motion cancels any pending release;
							// re-acquire only if we don't already hold
							// one (a back-to-back Start without an
							// intervening Stop is rare but harmless).
							release_at = None;
							if wake_guard.is_none() {
								wake_guard = Some(wake_lock.acquire());
							}
							let _ = publisher.publish_motion(true).await;
							status_cache.set_motion(true);
						}
						Ok(MotionStatus::Stop(_)) => {
							tracing::info!(camera = %camera_name, "Motion stopped");
							let _ = publisher.publish_motion(false).await;
							status_cache.set_motion(false);
							if wake_guard.is_some() {
								release_at = Some(
									tokio::time::Instant::now() + motion_wake_hold,
								);
							}
						}
						Ok(MotionStatus::NoChange(_)) => {}
						Err(e) => {
							tracing::warn!(camera = %camera_name, error = %e, "Motion listener error");
							break;
						}
					}
				}
			}
		}

		// Inner loop bailed (camera-side error). Re-subscribe with the
		// same exponential ladder; `backoff` was reset on the prior
		// successful subscribe so this is a fresh 1 s start.
		if !backoff.sleep_with_cancel(&cancel).await {
			break;
		}
	}
}

// ── Battery Poller ───────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
pub async fn battery_poller(
	camera_name: String,
	bc_camera: Arc<dyn CameraDriver>,
	mqtt: SharedMqttClient,
	topic_prefix: String,
	interval_ms: u64,
	cancel: CancellationToken,
	status_cache: Arc<StatusCache>,
) {
	let publisher = StatusPublisher::new(&mqtt, &topic_prefix, &camera_name);
	let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));

	loop {
		tokio::select! {
			_ = cancel.cancelled() => break,
			_ = ticker.tick() => {
				match tokio::time::timeout(Duration::from_secs(10), bc_camera.battery_info()).await {
					Ok(Ok(info)) => {
						let level = info.battery_percent.min(100) as u8;
						tracing::debug!(camera = %camera_name, battery = level, "Battery level");
						let _ = publisher.publish_battery_level(level).await;
						status_cache.set_battery_level(level);
					}
					Ok(Err(e)) => {
						tracing::debug!(camera = %camera_name, error = %e, "Battery poll failed");
					}
					Err(_) => {
						tracing::debug!(camera = %camera_name, "Battery poll timed out");
					}
				}
			}
		}
	}
}

// ── Preview Poller ───────────────────────────────────────────────────

/// Periodically publish the buffered JPEG for `status/preview`.
///
/// Refreshes and publishes the camera's preview JPEG.
///
/// Each tick asks the camera for a fresh snapshot via
/// `BcCamera::get_snapshot` **only when the camera is already connected** —
/// i.e. some other wake lock holder (an RTSP session, a `control/wakeup`
/// call, etc.) is keeping the camera awake. The poller itself does NOT
/// acquire a wake lock, so battery cameras aren't kept alive just to feed
/// previews. Matches neolink's `run_passive_task(get_snapshot)` semantics.
///
/// Fresh snapshots are also cached on [`LastFrameBuffer`] so the MQTT
/// `query/preview` command and the RTSP placeholder path both see up-to-
/// date content.
///
/// The rendered preview is downscaled and re-encoded to a bounded size
/// (see [`crate::preview_overlay::rendered_preview`] / `limits`). Any frame
/// that still exceeds `limits.max_bytes` after rendering is dropped with a
/// warn rather than published — a broker that caps message size rejects an
/// oversize PUBLISH and tears down the shared MQTT connection for every
/// camera, so one 1–2 MiB 4K snapshot must never reach the wire.
#[allow(clippy::too_many_arguments)]
pub async fn preview_poller(
	camera_name: String,
	camera: Arc<crate::camera::CameraHandle>,
	last_frame: Arc<LastFrameBuffer>,
	mqtt: SharedMqttClient,
	topic_prefix: String,
	interval_ms: u64,
	mut preview_state_rx: watch::Receiver<PreviewState>,
	preview_overlay_enabled: bool,
	limits: PreviewLimits,
	cancel: CancellationToken,
) {
	const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(10);

	let publisher = StatusPublisher::new(&mqtt, &topic_prefix, &camera_name);
	let overlay_cache = OverlayCache::new();
	let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
	ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
	// Drop the immediate first tick to avoid an unexpected burst on connect.
	ticker.tick().await;

	loop {
		tokio::select! {
			_ = cancel.cancelled() => break,
			_ = ticker.tick() => {
				// If the camera is awake, refresh the cached JPEG from a
				// live snapshot. If it's asleep/reconnecting we skip the
				// snapshot (would force an unwanted wake and defeats
				// idle_disconnect) but still proceed to publish the
				// cached bytes with the current overlay caption — that's
				// the whole point of the overlay: HA dashboards
				// should see SLEEPING / CONNECTING on stale frames so
				// users can tell live from stale.
				if camera.state().is_connected() {
					if let Some(bc) = camera.bc_camera() {
						match tokio::time::timeout(SNAPSHOT_TIMEOUT, bc.get_snapshot()).await {
							Ok(Ok(bytes)) => {
								last_frame.set_jpeg(bytes::Bytes::from(bytes));
							}
							Ok(Err(e)) => {
								tracing::debug!(camera = %camera_name, error = %e, "preview snapshot failed");
							}
							Err(_) => {
								tracing::debug!(camera = %camera_name, "preview snapshot timed out");
							}
						}
					}
				}

				// Publish the current cached JPEG (fresh if we just
				// refreshed above, stale otherwise) with the overlay
				// matching the current PreviewState.
				let Some(jpeg_bytes) = last_frame.jpeg() else {
					// No frame cached yet (startup wake hasn't run, or
					// get_snapshot has never succeeded). Nothing useful
					// to publish; silently skip — the next tick will
					// retry.
					continue;
				};

				// overlay a CONNECTING/SLEEPING caption
				// on the cached JPEG before publishing whenever the per-
				// camera PreviewState is non-Live. Gated by
				// `pause.preview_overlay` (default true) so operators can
				// opt out of the image/ab_glyph dependency at runtime.
				let payload = crate::preview_overlay::rendered_preview(
					jpeg_bytes,
					preview_overlay_enabled,
					*preview_state_rx.borrow_and_update(),
					Some(&overlay_cache),
					limits,
				);

				if !crate::preview_overlay::preview_within_cap(&payload, limits.max_bytes) {
					// Rendering still produced an oversize payload (e.g. an
					// undecodable snapshot that fell through unchanged).
					// Skip it — publishing would risk the broker rejecting
					// the PUBLISH and dropping the connection for every
					// camera on the shared client.
					tracing::warn!(
						camera = %camera_name,
						bytes = payload.len(),
						max_bytes = limits.max_bytes,
						"preview payload exceeds cap after render; skipping publish"
					);
				} else if let Err(e) = publisher.publish_preview(&payload).await {
					// If the broker advertises a lower MaxPacketSize the
					// send is rejected here — log as warn so operators see
					// they may need to raise broker message_size_limit.
					tracing::warn!(
						camera = %camera_name,
						bytes = payload.len(),
						error = %e,
						"preview publish failed"
					);
				}
			}
		}
	}
}

// ── Floodlight Poller ────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
pub async fn floodlight_poller(
	camera_name: String,
	bc_camera: Arc<dyn CameraDriver>,
	mqtt: SharedMqttClient,
	topic_prefix: String,
	interval_ms: u64,
	cancel: CancellationToken,
	status_cache: Arc<StatusCache>,
) {
	let publisher = StatusPublisher::new(&mqtt, &topic_prefix, &camera_name);
	let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));

	loop {
		tokio::select! {
			_ = cancel.cancelled() => break,
			_ = ticker.tick() => {
				match tokio::time::timeout(Duration::from_secs(10), bc_camera.is_floodlight_tasks_enabled()).await {
					Ok(Ok(enabled)) => {
						tracing::debug!(camera = %camera_name, enabled, "Floodlight tasks");
						let _ = publisher.publish_floodlight_tasks_enabled(enabled).await;
						status_cache.set_floodlight_tasks(enabled);
					}
					Ok(Err(e)) => {
						tracing::debug!(camera = %camera_name, error = %e, "Floodlight poll failed");
					}
					Err(_) => {
						tracing::debug!(camera = %camera_name, "Floodlight poll timed out");
					}
				}
			}
		}
	}
}

// ── Floodlight Listener ─────────────────────────────────────────────

pub async fn floodlight_listener(
	camera_name: String,
	bc_camera: Arc<dyn CameraDriver>,
	mqtt: SharedMqttClient,
	topic_prefix: String,
	cancel: CancellationToken,
	status_cache: Arc<StatusCache>,
) {
	let publisher = StatusPublisher::new(&mqtt, &topic_prefix, &camera_name);

	let mut rx = match bc_camera.listen_on_floodlight().await {
		Ok(rx) => rx,
		Err(e) => {
			tracing::debug!(camera = %camera_name, error = %e, "Floodlight listener not supported");
			return;
		}
	};

	loop {
		tokio::select! {
			_ = cancel.cancelled() => break,
			result = rx.recv() => {
				match result {
					Some(status_list) => {
						for flight in status_list.floodlight_status_list.iter() {
							let on = flight.status != 0;
							tracing::debug!(camera = %camera_name, on, "Floodlight state changed");
							let _ = publisher.publish_floodlight(on).await;
							status_cache.set_floodlight(on);
						}
					}
					None => break, // Channel closed
				}
			}
		}
	}
}

// ── PIR Initial Publish ──────────────────────────────────────────────

/// Query and publish PIR state once on connect. After that, PIR state
/// only changes via `control/pir` commands (handled in mqtt_dispatch).
pub async fn publish_pir_state(
	camera_name: String,
	bc_camera: Arc<dyn CameraDriver>,
	mqtt: SharedMqttClient,
	topic_prefix: String,
	status_cache: Arc<StatusCache>,
) {
	let publisher = StatusPublisher::new(&mqtt, &topic_prefix, &camera_name);
	match tokio::time::timeout(Duration::from_secs(10), bc_camera.get_pirstate()).await {
		Ok(Ok(pir_state)) => {
			let enabled = pir_state.enable == 1;
			tracing::debug!(camera = %camera_name, enabled, "PIR state");
			let _ = publisher.publish_pir(enabled).await;
			status_cache.set_pir(enabled);
		}
		Ok(Err(e)) => {
			tracing::debug!(camera = %camera_name, error = %e, "PIR state query failed");
		}
		Err(_) => {
			tracing::debug!(camera = %camera_name, "PIR state query timed out");
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use bairelay_mqtt::test_support::MockHandle;
	use bytes::Bytes;
	use neolink_core::bc_protocol::{CameraDriver, FakeCameraBuilder};

	/// Helper: every status-publishing task takes an `Arc<StatusCache>`
	/// as its final argument. Tests that don't assert on cache contents
	/// pass a fresh default. Wrapped in a helper to keep the call sites
	/// terse and not bind a dependency on the StatusCache constructor
	/// shape into every test.
	fn empty_cache() -> Arc<StatusCache> {
		Arc::new(StatusCache::default())
	}

	/// Poll the mock MQTT handle for up to `budget`, returning `true`
	/// as soon as any captured publish row satisfies `pred`. Factored
	/// out so the five poller/listener tests below don't duplicate the
	/// "sleep 10 ms, re-check, give up after 2 s" scaffolding.
	async fn await_publish_matching<F>(mock: &MockHandle, budget: Duration, pred: F) -> bool
	where
		F: Fn(&(String, Vec<u8>, bool)) -> bool,
	{
		tokio::time::timeout(budget, async {
			loop {
				if mock.published().iter().any(&pred) {
					return true;
				}
				tokio::time::sleep(Duration::from_millis(10)).await;
			}
		})
		.await
		.unwrap_or(false)
	}

	/// `motion_listener` publishes `status/motion = on` to MQTT the first
	/// time it observes a `MotionStatus::Start` from the camera's motion
	/// stream. Tight assertion: the retained-publish tuple with exactly
	/// the expected topic and payload must appear in the mock handle
	/// within 200 ms of the event being pushed into the fake channel.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn motion_listener_publishes_motion_on_start_event() {
		// Script a motion stream: tx stays with the test, rx goes into
		// the fake so the listener pulls from our channel.
		type MotionItem = std::result::Result<
			neolink_core::bc_protocol::MotionStatus,
			neolink_core::bc_protocol::Error,
		>;
		let (motion_tx, motion_rx) = tokio::sync::mpsc::channel::<MotionItem>(8);
		let motion_data = neolink_core::bc_protocol::MotionData::test_new(motion_rx);

		let fake = FakeCameraBuilder::new()
			.with_motion_stream(motion_data)
			.build();
		let driver: Arc<dyn CameraDriver> = fake.clone();

		let cancel = CancellationToken::new();
		let wl = crate::wake_lock::WakeLockCounter::new();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		let task = tokio::spawn(motion_listener(
			"cam1".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			wl,
			cancel.clone(),
			Duration::from_secs(30),
			empty_cache(),
		));

		// Push a Start event and wait briefly for the publish to
		// flow through.
		motion_tx
			.send(Ok(neolink_core::bc_protocol::MotionStatus::Start(
				std::time::Instant::now(),
			)))
			.await
			.unwrap();

		let saw_motion_on = await_publish_matching(&mock, Duration::from_secs(2), |(t, p, r)| {
			t == "bairelay/cam1/status/motion" && p == b"on" && *r
		})
		.await;

		cancel.cancel();
		let _ = task.await;

		assert!(
			saw_motion_on,
			"motion_listener should publish status/motion=on after MotionStatus::Start; observed: {:?}",
			mock.published_topics()
		);
	}

	/// `battery_poller` publishes `status/battery_level = "<percent>"`
	/// on every successful `battery_info()` call. Drives one tick, asserts
	/// on the retained topic + clamped numeric payload, then cancels.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn battery_poller_publishes_level_on_success() {
		use neolink_core::bc::xml::BatteryInfo;

		let fake = FakeCameraBuilder::new()
			.with_battery_info(|| {
				Ok(BatteryInfo {
					battery_percent: 77,
					..Default::default()
				})
			})
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let cancel = CancellationToken::new();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		let task = tokio::spawn(battery_poller(
			"cam1".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			20,
			cancel.clone(),
			empty_cache(),
		));

		let saw_level = await_publish_matching(&mock, Duration::from_secs(2), |(t, p, r)| {
			t == "bairelay/cam1/status/battery_level" && p == b"77" && *r
		})
		.await;

		cancel.cancel();
		let _ = task.await;

		assert!(
			saw_level,
			"battery_poller should publish 77 on status/battery_level; observed: {:?}",
			mock.published()
		);
	}

	/// Transient `battery_info` error on the first poll does not kill the
	/// task — a later successful closure call still produces a publish.
	/// The poller swallows `Err` at debug and keeps ticking.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn battery_poller_tolerates_transient_error_and_recovers() {
		use neolink_core::bc::xml::BatteryInfo;
		use std::sync::atomic::{AtomicU32, Ordering};

		let call = Arc::new(AtomicU32::new(0));
		let call_c = Arc::clone(&call);

		let fake = FakeCameraBuilder::new()
			.with_battery_info(move || {
				let n = call_c.fetch_add(1, Ordering::AcqRel);
				if n == 0 {
					Err(neolink_core::bc_protocol::Error::Other(
						"transient test failure",
					))
				} else {
					Ok(BatteryInfo {
						battery_percent: 33,
						..Default::default()
					})
				}
			})
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let cancel = CancellationToken::new();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		let task = tokio::spawn(battery_poller(
			"cam1".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			20,
			cancel.clone(),
			empty_cache(),
		));

		let saw_level = await_publish_matching(&mock, Duration::from_secs(3), |(t, p, _)| {
			t == "bairelay/cam1/status/battery_level" && p == b"33"
		})
		.await;

		cancel.cancel();
		let _ = task.await;

		assert!(
			saw_level,
			"battery_poller should recover after a transient error; calls={} publishes={:?}",
			call.load(Ordering::Acquire),
			mock.published_topics()
		);
		assert!(
			call.load(Ordering::Acquire) >= 2,
			"closure should have been invoked at least twice (one Err, one Ok)"
		);
	}

	/// `floodlight_listener` forwards each camera-originated event on the
	/// subscription channel to MQTT. Send one FloodlightStatus with
	/// status=1 and assert `status/floodlight = {"state":"on"}`.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn floodlight_listener_publishes_on_event() {
		use neolink_core::bc::xml::{FloodlightStatus, FloodlightStatusList};

		let (tx, rx) = tokio::sync::mpsc::channel::<FloodlightStatusList>(4);
		let fake = FakeCameraBuilder::new().with_floodlight_stream(rx).build();
		let driver: Arc<dyn CameraDriver> = fake;

		let cancel = CancellationToken::new();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		let task = tokio::spawn(floodlight_listener(
			"cam1".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			cancel.clone(),
			empty_cache(),
		));

		tx.send(FloodlightStatusList {
			version: "1.1".to_string(),
			floodlight_status_list: vec![FloodlightStatus {
				channel_id: 0,
				status: 1,
			}],
		})
		.await
		.unwrap();

		let saw_on = await_publish_matching(&mock, Duration::from_secs(2), |(t, p, r)| {
			t == "bairelay/cam1/status/floodlight" && p.as_slice() == br#"{"state":"on"}"# && *r
		})
		.await;

		cancel.cancel();
		let _ = task.await;

		assert!(
			saw_on,
			"floodlight_listener should publish status/floodlight on event; observed: {:?}",
			mock.published()
		);
	}

	/// `floodlight_poller` reads `is_floodlight_tasks_enabled` on every
	/// tick and publishes the result to `status/floodlight_tasks`.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn floodlight_poller_publishes_tasks_enabled_state() {
		let fake = FakeCameraBuilder::new()
			.with_is_floodlight_tasks_enabled(|| Ok(true))
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let cancel = CancellationToken::new();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		let task = tokio::spawn(floodlight_poller(
			"cam1".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			20,
			cancel.clone(),
			empty_cache(),
		));

		let saw = await_publish_matching(&mock, Duration::from_secs(2), |(t, p, r)| {
			t == "bairelay/cam1/status/floodlight_tasks" && p == b"on" && *r
		})
		.await;

		cancel.cancel();
		let _ = task.await;

		assert!(
			saw,
			"floodlight_poller should publish status/floodlight_tasks=on; observed: {:?}",
			mock.published()
		);
	}

	/// `publish_pir_state` (the one-shot PIR reader invoked on connect)
	/// should publish `status/pir = on` when the camera's PIR enable flag
	/// is 1.
	#[tokio::test]
	async fn publish_pir_state_publishes_enabled() {
		use neolink_core::bc::xml::RfAlarmCfg;

		let fake = FakeCameraBuilder::new()
			.with_pirstate(|| {
				Ok(RfAlarmCfg {
					enable: 1,
					..Default::default()
				})
			})
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		publish_pir_state(
			"cam1".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			empty_cache(),
		)
		.await;

		let pub_rows = mock.published();
		assert!(
			pub_rows
				.iter()
				.any(|(t, p, r)| t == "bairelay/cam1/status/pir" && p == b"on" && *r),
			"publish_pir_state should publish status/pir=on; observed: {:?}",
			pub_rows
		);
	}

	fn fake_jpeg() -> Bytes {
		let img = image::RgbImage::from_pixel(32, 32, image::Rgb([64, 64, 64]));
		let mut out = Vec::new();
		image::DynamicImage::ImageRgb8(img)
			.write_to(
				&mut std::io::Cursor::new(&mut out),
				image::ImageFormat::Jpeg,
			)
			.expect("encode fake jpeg");
		Bytes::from(out)
	}

	/// Production-default preview limits for the poller tests.
	fn test_preview_limits() -> PreviewLimits {
		PreviewLimits::from(&crate::config::MqttConfig::default())
	}

	#[test]
	fn overlay_applies_when_enabled_and_not_live() {
		let jpeg = fake_jpeg();
		let rendered = crate::preview_overlay::render(&jpeg, PreviewState::Connecting);
		assert_ne!(rendered, jpeg, "non-Live state must change the bytes");
	}

	#[test]
	fn overlay_skipped_when_disabled() {
		let jpeg = fake_jpeg();
		let preview_overlay_enabled = false;
		let rendered = if preview_overlay_enabled {
			crate::preview_overlay::render(&jpeg, PreviewState::Connecting)
		} else {
			jpeg.clone()
		};
		assert_eq!(rendered, jpeg, "disabled flag must bypass the renderer");
	}

	#[test]
	fn overlay_passthrough_on_live_state() {
		let jpeg = fake_jpeg();
		let rendered = crate::preview_overlay::render(&jpeg, PreviewState::Live);
		assert_eq!(rendered, jpeg, "Live state must be passthrough");
	}

	/// Drive `preview_poller` for one tick against a not-connected
	/// `CameraHandle` whose `LastFrameBuffer` holds a cached JPEG — the
	/// disconnected branch must skip the snapshot call but still
	/// publish the cached bytes with the current overlay caption.
	/// Covers the post-Phase-2H camera-lifetime-spawned preview loop.
	#[tokio::test]
	async fn preview_poller_publishes_cached_jpeg_while_disconnected() {
		use crate::camera::CameraHandle;
		use crate::config::test_helpers::minimal_camera_config;

		// Pre-seed the shared buffer so the "publish stale bytes"
		// branch has content to emit.
		let last_frame = Arc::new(LastFrameBuffer::new());
		last_frame.set_jpeg(fake_jpeg());

		// Minimal camera in its default `Sleeping` / disconnected state.
		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(
			minimal_camera_config("cam-preview-test"),
			cancel.clone(),
			None,
		));

		let (mqtt, mqtt_handle) = bairelay_mqtt::test_support::mock_client();
		let rx = handle.preview_state_rx();

		// Drive the poller: 10ms ticker fires near-immediately;
		// cancel after 100ms — plenty of time for at least one tick.
		let cancel_task = cancel.clone();
		let task = tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(100)).await;
			cancel_task.cancel();
		});

		preview_poller(
			"cam-preview-test".to_string(),
			Arc::clone(&handle),
			Arc::clone(&last_frame),
			mqtt,
			"bairelay".to_string(),
			10, // 10 ms — fast tick for the test
			rx,
			true,
			test_preview_limits(),
			cancel,
		)
		.await;
		let _ = task.await;

		// At least one preview publish must have landed on the expected
		// retained topic. Captures regression where the poller writes to
		// the wrong topic prefix or fails to publish at all.
		let topics = mqtt_handle.published_topics();
		assert!(
			topics
				.iter()
				.any(|t| t == "bairelay/cam-preview-test/status/preview"),
			"preview poller did not publish to status/preview; topics seen: {:?}",
			topics
		);
	}

	/// The publish-site guard drops a payload that still exceeds
	/// `limits.max_bytes` after rendering: with a 10-byte cap, even the
	/// downscaled fake JPEG is over-cap, so NOTHING lands on
	/// `status/preview`. Protects the shared MQTT connection from an
	/// oversize frame taking it down for every camera.
	#[tokio::test]
	async fn preview_poller_skips_publish_when_over_cap() {
		use crate::camera::CameraHandle;
		use crate::config::test_helpers::minimal_camera_config;

		let last_frame = Arc::new(LastFrameBuffer::new());
		last_frame.set_jpeg(fake_jpeg());

		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(
			minimal_camera_config("cam-over-cap"),
			cancel.clone(),
			None,
		));

		let (mqtt, mqtt_handle) = bairelay_mqtt::test_support::mock_client();
		let rx = handle.preview_state_rx();

		// A deliberately tiny cap that any real JPEG blows past. Direct
		// construction bypasses config validation (which floors this at
		// 4096) — that's fine, we're exercising the runtime guard.
		let tiny = PreviewLimits {
			max_width: 1280,
			jpeg_quality: 75,
			max_bytes: 10,
		};

		let cancel_task = cancel.clone();
		let task = tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(100)).await;
			cancel_task.cancel();
		});

		preview_poller(
			"cam-over-cap".to_string(),
			Arc::clone(&handle),
			Arc::clone(&last_frame),
			mqtt,
			"bairelay".to_string(),
			10,
			rx,
			true,
			tiny,
			cancel,
		)
		.await;
		let _ = task.await;

		let topics = mqtt_handle.published_topics();
		assert!(
			!topics
				.iter()
				.any(|t| t == "bairelay/cam-over-cap/status/preview"),
			"over-cap preview must NOT be published; topics seen: {:?}",
			topics
		);
	}

	/// Same shape, but with `preview_overlay_enabled = false` — covers
	/// the bypass branch where the raw cached JPEG is published as-is.
	#[tokio::test]
	async fn preview_poller_honours_overlay_disabled_flag() {
		use crate::camera::CameraHandle;
		use crate::config::test_helpers::minimal_camera_config;

		let last_frame = Arc::new(LastFrameBuffer::new());
		last_frame.set_jpeg(fake_jpeg());

		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(
			minimal_camera_config("cam-preview-noovl"),
			cancel.clone(),
			None,
		));

		let mqtt = SharedMqttClient::for_test_stub("preview-poller-noovl");
		let rx = handle.preview_state_rx();

		let cancel_task = cancel.clone();
		let task = tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(100)).await;
			cancel_task.cancel();
		});

		preview_poller(
			"cam-preview-noovl".to_string(),
			Arc::clone(&handle),
			Arc::clone(&last_frame),
			mqtt,
			"bairelay".to_string(),
			10,
			rx,
			false, // disable overlay
			test_preview_limits(),
			cancel,
		)
		.await;
		let _ = task.await;
	}

	/// Motion Stop following a Start releases the wake guard via a
	/// spawned task. The publish-motion-off MQTT row must appear, and
	/// the channel close after the Stop must exit the listener cleanly.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn motion_listener_publishes_motion_off_on_stop_event() {
		type MotionItem = std::result::Result<
			neolink_core::bc_protocol::MotionStatus,
			neolink_core::bc_protocol::Error,
		>;
		let (motion_tx, motion_rx) = tokio::sync::mpsc::channel::<MotionItem>(8);
		let motion_data = neolink_core::bc_protocol::MotionData::test_new(motion_rx);
		let fake = FakeCameraBuilder::new()
			.with_motion_stream(motion_data)
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let cancel = CancellationToken::new();
		let wl = crate::wake_lock::WakeLockCounter::new();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		let task = tokio::spawn(motion_listener(
			"cam1".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			wl,
			cancel.clone(),
			Duration::from_secs(30),
			empty_cache(),
		));

		// Start then Stop in quick succession.
		motion_tx
			.send(Ok(neolink_core::bc_protocol::MotionStatus::Start(
				std::time::Instant::now(),
			)))
			.await
			.unwrap();
		motion_tx
			.send(Ok(neolink_core::bc_protocol::MotionStatus::Stop(
				std::time::Instant::now(),
			)))
			.await
			.unwrap();

		let saw_off = await_publish_matching(&mock, Duration::from_secs(2), |(t, p, _)| {
			t == "bairelay/cam1/status/motion" && p == b"off"
		})
		.await;

		cancel.cancel();
		let _ = task.await;
		assert!(saw_off, "Stop event must publish status/motion=off");
	}

	/// Wake-lock hold-down: Start acquires the lock; Stop schedules an
	/// inline release after `motion_wake_hold`; the lock remains held
	/// across the window and is dropped exactly once when the timer
	/// expires. Pins the post-Phase-3D refactor that replaced the
	/// detached `tokio::spawn` release with a `select!` arm.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn motion_listener_holds_wake_lock_through_hold_down() {
		type MotionItem = std::result::Result<
			neolink_core::bc_protocol::MotionStatus,
			neolink_core::bc_protocol::Error,
		>;
		let (motion_tx, motion_rx) = tokio::sync::mpsc::channel::<MotionItem>(8);
		let motion_data = neolink_core::bc_protocol::MotionData::test_new(motion_rx);
		let fake = FakeCameraBuilder::new()
			.with_motion_stream(motion_data)
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let cancel = CancellationToken::new();
		let wl = crate::wake_lock::WakeLockCounter::new();
		let (mqtt, _mock) = bairelay_mqtt::test_support::mock_client();
		// 500 ms hold-down absorbs tarpaulin instrumentation slop on
		// the timer; smaller windows have flaked under coverage.
		let hold = Duration::from_millis(500);

		let task = tokio::spawn(motion_listener(
			"cam-hold".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			wl.clone(),
			cancel.clone(),
			hold,
			empty_cache(),
		));

		// Start → wake lock acquired.
		motion_tx
			.send(Ok(neolink_core::bc_protocol::MotionStatus::Start(
				std::time::Instant::now(),
			)))
			.await
			.unwrap();
		tokio::time::sleep(Duration::from_millis(50)).await;
		assert_eq!(wl.count(), 1, "Start must acquire the wake lock");

		// Stop → lock still held during hold-down window.
		motion_tx
			.send(Ok(neolink_core::bc_protocol::MotionStatus::Stop(
				std::time::Instant::now(),
			)))
			.await
			.unwrap();
		tokio::time::sleep(Duration::from_millis(100)).await;
		assert_eq!(
			wl.count(),
			1,
			"wake lock must remain held during the post-stop hold-down",
		);

		// Past the deadline → lock dropped exactly once.
		tokio::time::sleep(Duration::from_millis(700)).await;
		assert_eq!(
			wl.count(),
			0,
			"wake lock must be released after the hold-down expires",
		);

		cancel.cancel();
		let _ = task.await;
	}

	/// A fresh Start during the hold-down window cancels the pending
	/// release and keeps holding the existing wake-lock guard. Critical
	/// invariant: each listener holds **at most one** guard, regardless
	/// of motion-event flap rate.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn motion_listener_start_during_hold_down_cancels_release() {
		type MotionItem = std::result::Result<
			neolink_core::bc_protocol::MotionStatus,
			neolink_core::bc_protocol::Error,
		>;
		let (motion_tx, motion_rx) = tokio::sync::mpsc::channel::<MotionItem>(8);
		let motion_data = neolink_core::bc_protocol::MotionData::test_new(motion_rx);
		let fake = FakeCameraBuilder::new()
			.with_motion_stream(motion_data)
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let cancel = CancellationToken::new();
		let wl = crate::wake_lock::WakeLockCounter::new();
		let (mqtt, _mock) = bairelay_mqtt::test_support::mock_client();
		// 500 ms hold-down absorbs tarpaulin instrumentation slop.
		let hold = Duration::from_millis(500);

		let task = tokio::spawn(motion_listener(
			"cam-flap".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			wl.clone(),
			cancel.clone(),
			hold,
			empty_cache(),
		));

		// Start → Stop → Start, all within the hold-down window.
		motion_tx
			.send(Ok(neolink_core::bc_protocol::MotionStatus::Start(
				std::time::Instant::now(),
			)))
			.await
			.unwrap();
		tokio::time::sleep(Duration::from_millis(50)).await;
		motion_tx
			.send(Ok(neolink_core::bc_protocol::MotionStatus::Stop(
				std::time::Instant::now(),
			)))
			.await
			.unwrap();
		tokio::time::sleep(Duration::from_millis(50)).await;
		motion_tx
			.send(Ok(neolink_core::bc_protocol::MotionStatus::Start(
				std::time::Instant::now(),
			)))
			.await
			.unwrap();

		// Past the original hold-down deadline (which the second Start
		// must have cancelled) — lock is still held because no Stop
		// has fired since the re-arm.
		tokio::time::sleep(Duration::from_millis(700)).await;
		assert_eq!(
			wl.count(),
			1,
			"a Start during the hold-down window must cancel the pending release",
		);

		cancel.cancel();
		let _ = task.await;
	}

	/// `MotionStatus::NoChange` events are not published but must not
	/// break the listener — a Start following a NoChange still emits on.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn motion_listener_ignores_nochange_and_continues() {
		type MotionItem = std::result::Result<
			neolink_core::bc_protocol::MotionStatus,
			neolink_core::bc_protocol::Error,
		>;
		let (motion_tx, motion_rx) = tokio::sync::mpsc::channel::<MotionItem>(8);
		let motion_data = neolink_core::bc_protocol::MotionData::test_new(motion_rx);
		let fake = FakeCameraBuilder::new()
			.with_motion_stream(motion_data)
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let cancel = CancellationToken::new();
		let wl = crate::wake_lock::WakeLockCounter::new();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		let task = tokio::spawn(motion_listener(
			"cam1".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			wl,
			cancel.clone(),
			Duration::from_secs(30),
			empty_cache(),
		));

		motion_tx
			.send(Ok(neolink_core::bc_protocol::MotionStatus::NoChange(
				std::time::Instant::now(),
			)))
			.await
			.unwrap();
		motion_tx
			.send(Ok(neolink_core::bc_protocol::MotionStatus::Start(
				std::time::Instant::now(),
			)))
			.await
			.unwrap();

		let saw_on = await_publish_matching(&mock, Duration::from_secs(2), |(t, p, _)| {
			t == "bairelay/cam1/status/motion" && p == b"on"
		})
		.await;

		cancel.cancel();
		let _ = task.await;
		assert!(saw_on, "NoChange must not block subsequent Start publish");
	}

	/// When `next_motion()` returns `Err`, the inner loop breaks; the
	/// outer loop sleeps 1 s then re-calls `listen_on_motion`, which
	/// panics (stream already consumed). Cancel the task before that
	/// retry lands so we observe only the "inner Err → break" path
	/// cleanly.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn motion_listener_breaks_inner_loop_on_next_motion_error() {
		type MotionItem = std::result::Result<
			neolink_core::bc_protocol::MotionStatus,
			neolink_core::bc_protocol::Error,
		>;
		let (motion_tx, motion_rx) = tokio::sync::mpsc::channel::<MotionItem>(8);
		let motion_data = neolink_core::bc_protocol::MotionData::test_new(motion_rx);
		let fake = FakeCameraBuilder::new()
			.with_motion_stream(motion_data)
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let cancel = CancellationToken::new();
		let wl = crate::wake_lock::WakeLockCounter::new();
		let (mqtt, _mock) = bairelay_mqtt::test_support::mock_client();

		let task = tokio::spawn(motion_listener(
			"cam1".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			wl,
			cancel.clone(),
			Duration::from_secs(30),
			empty_cache(),
		));

		// Push one Err into the motion channel. The listener will log
		// and break; we cancel before the 1s retry sleep finishes.
		motion_tx
			.send(Err(neolink_core::bc_protocol::Error::Other(
				"scripted next_motion failure",
			)))
			.await
			.unwrap();

		tokio::time::sleep(Duration::from_millis(100)).await;
		cancel.cancel();
		let _ = tokio::time::timeout(Duration::from_millis(500), task).await;
	}

	/// `floodlight_listener` returns quietly when `listen_on_floodlight`
	/// errors — e.g. the camera doesn't support floodlight. The task
	/// joins without panicking and no publishes are emitted.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn floodlight_listener_exits_cleanly_on_subscribe_error() {
		// Build without calling `.with_floodlight_stream(...)` → the
		// fake's `listen_on_floodlight` returns Err (via `unset()`
		// actually panics — but we need a real error. Provide a
		// drained channel instead so the `listen_on_floodlight` call
		// succeeds and `rx.recv()` returns None immediately — matches
		// the "None → break" path on line 300.)
		let (tx, rx) = tokio::sync::mpsc::channel::<neolink_core::bc::xml::FloodlightStatusList>(1);
		drop(tx); // close the sender so rx.recv() returns None.
		let fake = FakeCameraBuilder::new().with_floodlight_stream(rx).build();
		let driver: Arc<dyn CameraDriver> = fake;
		let cancel = CancellationToken::new();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		floodlight_listener(
			"cam1".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			cancel,
			empty_cache(),
		)
		.await;
		assert!(
			mock.published().is_empty(),
			"closed channel should publish nothing; got {:?}",
			mock.published_topics()
		);
	}

	/// `publish_pir_state` with a driver error — publishes nothing and
	/// returns cleanly.
	#[tokio::test]
	async fn publish_pir_state_handles_driver_error() {
		let fake = FakeCameraBuilder::new()
			.with_pirstate(|| Err(neolink_core::bc_protocol::Error::Other("pir refused")))
			.build();
		let driver: Arc<dyn CameraDriver> = fake;
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		publish_pir_state(
			"cam1".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			empty_cache(),
		)
		.await;
		assert!(
			!mock
				.published()
				.iter()
				.any(|(t, _, _)| t == "bairelay/cam1/status/pir"),
			"error path must not publish status/pir"
		);
	}

	/// `floodlight_poller` error branch: `is_floodlight_tasks_enabled`
	/// returns Err, and the poller swallows it at debug without
	/// publishing.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn floodlight_poller_tolerates_err_without_publishing() {
		let fake = FakeCameraBuilder::new()
			.with_is_floodlight_tasks_enabled(|| {
				Err(neolink_core::bc_protocol::Error::Other("tasks refused"))
			})
			.build();
		let driver: Arc<dyn CameraDriver> = fake;
		let cancel = CancellationToken::new();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let task = tokio::spawn(floodlight_poller(
			"cam1".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			20,
			cancel.clone(),
			empty_cache(),
		));
		// Let at least one tick fire + settle.
		tokio::time::sleep(Duration::from_millis(80)).await;
		cancel.cancel();
		let _ = task.await;
		assert!(
			!mock
				.published()
				.iter()
				.any(|(t, _, _)| t == "bairelay/cam1/status/floodlight_tasks"),
			"error branch must not publish floodlight_tasks"
		);
	}

	/// `preview_poller` while connected refreshes the cached JPEG via
	/// the driver's `get_snapshot()` and publishes the result. Covers
	/// the `state().is_connected() && bc_camera().is_some()` arm +
	/// the `Ok(bytes)` → `set_jpeg` branch in the poller body.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn preview_poller_refreshes_jpeg_while_connected() {
		use crate::camera::CameraHandle;
		use crate::config::test_helpers::minimal_camera_config;

		let fresh_jpeg = fake_jpeg();
		let fresh_clone = fresh_jpeg.clone();
		let fake = neolink_core::bc_protocol::FakeCameraBuilder::new()
			.with_snapshot(move || Ok(fresh_clone.to_vec()))
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(
			minimal_camera_config("cam-conn"),
			cancel.clone(),
			None,
		));
		handle.set_driver_for_test(driver);

		let last_frame = Arc::new(LastFrameBuffer::new());
		// Pre-seed with different bytes so we can observe the refresh.
		last_frame.set_jpeg(Bytes::from_static(&[0u8; 4]));
		let rx = handle.preview_state_rx();

		let mqtt = SharedMqttClient::for_test_stub("preview-poller-connected");

		let cancel_task = cancel.clone();
		let canceller = tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(80)).await;
			cancel_task.cancel();
		});

		preview_poller(
			"cam-conn".to_string(),
			Arc::clone(&handle),
			Arc::clone(&last_frame),
			mqtt,
			"bairelay".to_string(),
			10,
			rx,
			false,
			test_preview_limits(),
			cancel,
		)
		.await;
		let _ = canceller.await;

		// The cached buffer should now match the fresh JPEG from the driver.
		let got = last_frame.jpeg().expect("buffer populated");
		assert_eq!(
			got.as_ref(),
			fresh_jpeg.as_ref(),
			"connected path must refresh the cached JPEG from the driver"
		);
	}

	/// `preview_poller` connected path with a driver that errors on
	/// snapshot — the poller swallows the error and still publishes
	/// the pre-existing cached bytes.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn preview_poller_tolerates_snapshot_error_while_connected() {
		use crate::camera::CameraHandle;
		use crate::config::test_helpers::minimal_camera_config;

		let fake = neolink_core::bc_protocol::FakeCameraBuilder::new()
			.with_snapshot(|| Err(neolink_core::bc_protocol::Error::Other("snap denied")))
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(
			minimal_camera_config("cam-snap-err"),
			cancel.clone(),
			None,
		));
		handle.set_driver_for_test(driver);
		let last_frame = Arc::new(LastFrameBuffer::new());
		last_frame.set_jpeg(fake_jpeg());
		let rx = handle.preview_state_rx();

		let mqtt = SharedMqttClient::for_test_stub("preview-snap-err");
		let cancel_task = cancel.clone();
		let canceller = tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(80)).await;
			cancel_task.cancel();
		});

		preview_poller(
			"cam-snap-err".to_string(),
			Arc::clone(&handle),
			Arc::clone(&last_frame),
			mqtt,
			"bairelay".to_string(),
			10,
			rx,
			false,
			test_preview_limits(),
			cancel,
		)
		.await;
		let _ = canceller.await;
	}

	/// With no cached JPEG, the disconnected poller has nothing to
	/// publish — the `last_frame.jpeg() is None → continue` branch.
	#[tokio::test]
	async fn preview_poller_noops_when_buffer_is_empty() {
		use crate::camera::CameraHandle;
		use crate::config::test_helpers::minimal_camera_config;

		let last_frame = Arc::new(LastFrameBuffer::new());
		// Deliberately leave the buffer empty.
		let cancel = CancellationToken::new();
		let handle = Arc::new(CameraHandle::new(
			minimal_camera_config("cam-preview-empty"),
			cancel.clone(),
			None,
		));

		let mqtt = SharedMqttClient::for_test_stub("preview-poller-empty");
		let rx = handle.preview_state_rx();

		let cancel_task = cancel.clone();
		let task = tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(80)).await;
			cancel_task.cancel();
		});

		preview_poller(
			"cam-preview-empty".to_string(),
			Arc::clone(&handle),
			Arc::clone(&last_frame),
			mqtt,
			"bairelay".to_string(),
			10,
			rx,
			true,
			test_preview_limits(),
			cancel,
		)
		.await;
		let _ = task.await;
	}

	/// `motion_listener` with a driver that errors on every
	/// `listen_on_motion` subscribe: the outer loop must sleep for 1 s
	/// then retry. We cancel during the retry sleep so the task exits
	/// via the `cancel.cancelled()` arm of the retry-sleep select
	/// (lines 40, 87 of camera_tasks.rs).
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn motion_listener_retries_after_subscribe_error() {
		let fake = FakeCameraBuilder::new()
			.with_motion_stream_error(|| {
				neolink_core::bc_protocol::Error::Other("scripted subscribe failure")
			})
			.build();
		let driver: Arc<dyn CameraDriver> = fake;

		let cancel = CancellationToken::new();
		let wl = crate::wake_lock::WakeLockCounter::new();
		let (mqtt, _mock) = bairelay_mqtt::test_support::mock_client();

		let task = tokio::spawn(motion_listener(
			"cam-retry".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			wl,
			cancel.clone(),
			Duration::from_secs(30),
			empty_cache(),
		));

		// Give the listener enough wall time to invoke
		// `listen_on_motion` (fails) and enter the retry-sleep, but
		// cancel well before the 1 s sleep elapses.
		tokio::time::sleep(Duration::from_millis(50)).await;
		cancel.cancel();
		let _ = tokio::time::timeout(Duration::from_millis(500), task).await;
	}

	/// `floodlight_listener` with a driver that errors on
	/// `listen_on_floodlight`: the listener logs and returns
	/// immediately (early-return branch). Pins the "camera doesn't
	/// support floodlight" path where previously tests only covered
	/// the closed-channel case.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn floodlight_listener_returns_on_subscribe_error() {
		let fake = FakeCameraBuilder::new()
			.with_floodlight_stream_error(|| {
				neolink_core::bc_protocol::Error::Other("floodlight unsupported")
			})
			.build();
		let driver: Arc<dyn CameraDriver> = fake;
		let cancel = CancellationToken::new();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();

		// No spawn: the listener must return before we touch cancel.
		// If the early-return branch regresses, the test will hang on
		// `floodlight_listener` until the outer harness timeout fires.
		tokio::time::timeout(
			Duration::from_millis(500),
			floodlight_listener(
				"cam-err".to_string(),
				driver,
				mqtt,
				"bairelay".to_string(),
				cancel,
				empty_cache(),
			),
		)
		.await
		.expect("listener must return on subscribe error");

		assert!(
			mock.published().is_empty(),
			"subscribe error must not publish; got {:?}",
			mock.published_topics()
		);
	}

	/// `battery_poller` whose driver hangs past 10 s — the per-tick
	/// timeout fires and the poller logs a debug. Drives one tick under
	/// paused virtual clock so the test runs in microseconds; cancels
	/// before the next tick to avoid a publish race.
	#[tokio::test(flavor = "current_thread", start_paused = true)]
	async fn battery_poller_timeout_branch_logs_and_continues() {
		let fake = FakeCameraBuilder::new().with_battery_info_pending().build();
		let driver: Arc<dyn CameraDriver> = fake;
		let cancel = CancellationToken::new();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let task = tokio::spawn(battery_poller(
			"cam-bt".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			20,
			cancel.clone(),
			empty_cache(),
		));
		// Advance past the first tick (20 ms) plus the 10 s per-tick timeout.
		tokio::time::advance(Duration::from_secs(11)).await;
		tokio::task::yield_now().await;
		cancel.cancel();
		let _ = task.await;
		// Battery level publish must NOT have happened — the timeout
		// branch logs and continues without publishing.
		assert!(!mock
			.published()
			.iter()
			.any(|(t, _, _)| t == "bairelay/cam-bt/status/battery_level"));
	}

	/// `floodlight_poller` timeout branch — driver hangs past 10 s.
	#[tokio::test(flavor = "current_thread", start_paused = true)]
	async fn floodlight_poller_timeout_branch_logs_and_continues() {
		let fake = FakeCameraBuilder::new()
			.with_is_floodlight_tasks_enabled_pending()
			.build();
		let driver: Arc<dyn CameraDriver> = fake;
		let cancel = CancellationToken::new();
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let task = tokio::spawn(floodlight_poller(
			"cam-ft".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			20,
			cancel.clone(),
			empty_cache(),
		));
		// Advance past the first tick (20 ms) plus the 10 s per-tick timeout.
		tokio::time::advance(Duration::from_secs(11)).await;
		tokio::task::yield_now().await;
		cancel.cancel();
		let _ = task.await;
		// Timeout branch must NOT have published anything.
		assert!(!mock
			.published()
			.iter()
			.any(|(t, _, _)| t == "bairelay/cam-ft/status/floodlight_tasks"));
	}

	/// `publish_pir_state` timeout branch — driver hangs past 10 s.
	#[tokio::test(flavor = "current_thread", start_paused = true)]
	async fn publish_pir_state_timeout_branch() {
		let fake = FakeCameraBuilder::new().with_pirstate_pending().build();
		let driver: Arc<dyn CameraDriver> = fake;
		let (mqtt, mock) = bairelay_mqtt::test_support::mock_client();
		let task = tokio::spawn(publish_pir_state(
			"cam-pst".to_string(),
			driver,
			mqtt,
			"bairelay".to_string(),
			empty_cache(),
		));
		tokio::time::advance(Duration::from_secs(11)).await;
		let _ = task.await;
		assert!(!mock
			.published()
			.iter()
			.any(|(t, _, _)| t == "bairelay/cam-pst/status/pir"));
	}
}
