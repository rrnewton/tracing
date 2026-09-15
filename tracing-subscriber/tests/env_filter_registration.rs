#![cfg(all(feature = "env-filter", feature = "registry"))]

use std::{
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tracing::{subscriber::Interest, Dispatch, Event, Metadata, Subscriber};
use tracing_subscriber::{
    layer::{Context, SubscriberExt},
    EnvFilter, Layer,
};

struct PauseRegistration {
    armed: AtomicBool,
    reached: mpsc::SyncSender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl<S: Subscriber> Layer<S> for PauseRegistration {
    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        if metadata.name() == "registration_watched" && self.armed.swap(false, Ordering::SeqCst) {
            self.reached.send(()).unwrap();
            self.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(3))
                .unwrap();
        }
        Interest::always()
    }
}

struct Capture(Arc<Mutex<Vec<&'static str>>>);

impl<S: Subscriber> Layer<S> for Capture {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        self.0.lock().unwrap().push(event.metadata().target());
    }
}

fn emit() -> bool {
    let span = tracing::info_span!("registration_watched", owner = 0u64);
    let enabled = !span.is_disabled();
    let entered = span.enter();
    tracing::error!(target: "before_match", "not selected");
    drop(entered);
    span.record("owner", 7u64);
    let entered = span.enter();
    tracing::error!(target: "selected_record", "selected");
    drop(entered);
    tracing::error!(target: "after_exit", "not selected");
    enabled
}

fn run(test: &str, directive: &str) {
    const CHILD: &str = "TRACING_ENV_FILTER_REGISTRATION_CHILD";
    if std::env::var(CHILD).as_deref() != Ok(test) {
        // Callsite registration is process-global and happens only once. A
        // fresh, exact-name child keeps each case cold under ordinary discovery.
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test, "--nocapture", "--test-threads=1"])
            .env(CHILD, test)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while child.try_wait().unwrap().is_none() {
            if Instant::now() >= deadline {
                child.kill().unwrap();
                let output = child.wait_with_output().unwrap();
                panic!("callsite registration did not finish: {:?}", output);
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let output = child.wait_with_output().unwrap();
        assert!(output.status.success(), "{:?}", output);
        assert!(output.stderr.is_empty(), "{:?}", output);
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(
            stdout.contains(&format!("test {} ... ok", test)),
            "{}",
            stdout
        );
        assert!(
            stdout.contains("1 passed; 0 failed; 0 ignored;"),
            "{}",
            stdout
        );
        return;
    }

    let (reached_send, reached) = mpsc::sync_channel(1);
    let (release, release_recv) = mpsc::sync_channel(1);
    let records = Arc::new(Mutex::new(Vec::new()));
    let dispatch = Dispatch::new(
        tracing_subscriber::registry()
            .with(Capture(records.clone()))
            .with(EnvFilter::new(directive))
            // The outer callback pauses before this filter receives the new
            // callsite. No private registration state or matcher is modified.
            .with(PauseRegistration {
                armed: AtomicBool::new(true),
                reached: reached_send,
                release: Mutex::new(release_recv),
            }),
    );
    let first_dispatch = dispatch.clone();
    let first =
        std::thread::spawn(move || tracing::dispatcher::with_default(&first_dispatch, emit));
    reached.recv_timeout(Duration::from_secs(3)).unwrap();
    let second_dispatch = dispatch.clone();
    let (done_send, done) = mpsc::sync_channel(1);
    let second = std::thread::spawn(move || {
        let enabled = tracing::dispatcher::with_default(&second_dispatch, emit);
        done_send.send(enabled).unwrap();
    });
    let second_result = done.recv_timeout(Duration::from_secs(2));
    release.send(()).unwrap();
    let first_enabled = first.join().unwrap();
    second.join().unwrap();
    let second_enabled = second_result.unwrap();
    assert!(first_enabled);
    assert!(
        second_enabled,
        "a matching span was disabled while another thread registered it"
    );
    assert_eq!(
        *records.lock().unwrap(),
        vec!["selected_record", "selected_record"]
    );
}

#[test]
fn dynamic_fields_work_before_callsite_registration_finishes() {
    run(
        "dynamic_fields_work_before_callsite_registration_finishes",
        "off,[registration_watched{owner=7}]=info",
    );
}

#[test]
fn matching_spans_enable_less_verbose_events_during_registration() {
    run(
        "matching_spans_enable_less_verbose_events_during_registration",
        "off,[registration_watched{owner=7}]=error",
    );
}
