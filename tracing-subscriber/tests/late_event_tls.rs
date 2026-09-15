#![cfg(all(
    feature = "env-filter",
    feature = "fmt",
    target_os = "linux",
    target_pointer_width = "64"
))]

use std::{
    cell::RefCell,
    fmt::Write as _,
    fs::{File, OpenOptions},
    io::Write as _,
    os::unix::process::ExitStatusExt,
    path::PathBuf,
    process::{Command, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tracing::{
    span::{Attributes, Id, Record},
    Event, Subscriber,
};
use tracing_subscriber::{
    fmt::format::{DefaultFields, FormatFields, Writer},
    layer::{Context, SubscriberExt},
    registry::LookupSpan,
    EnvFilter, Layer,
};

const RECORD: &[u8] = b" INFO watched{owner=7}: host_record: retained value=11 answer=\"stable\"\n";

// A small test Layer uses stock field formatting and Registry context, without
// fmt::Layer's independent destructible output buffer. This isolates storage;
// it does not replace or claim to repair the published fmt Layer.
struct Capture(Arc<Mutex<File>>);
struct Fields(String);
impl<S> Layer<S> for Capture
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut fields = String::new();
        DefaultFields::new()
            .format_fields(Writer::new(&mut fields), attrs)
            .unwrap();
        ctx.span(id)
            .unwrap()
            .extensions_mut()
            .insert(Fields(fields));
    }
    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let mut fragment = String::new();
        DefaultFields::new()
            .format_fields(Writer::new(&mut fragment), values)
            .unwrap();
        let span = ctx.span(id).unwrap();
        let mut extensions = span.extensions_mut();
        let fields = &mut extensions.get_mut::<Fields>().unwrap().0;
        if !fields.is_empty() {
            fields.push(' ');
        }
        fields.push_str(&fragment);
    }
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        assert_eq!(*event.metadata().level(), tracing::Level::INFO);
        let mut record = String::from(" INFO ");
        if let Some(scope) = ctx.event_scope(event) {
            let mut seen = false;
            for span in scope.from_root() {
                record.push_str(span.name());
                let fields = span.extensions();
                let fields = &fields.get::<Fields>().unwrap().0;
                if !fields.is_empty() {
                    write!(record, "{{{}}}", fields).unwrap();
                }
                record.push(':');
                seen = true;
            }
            if seen {
                record.push(' ');
            }
        }
        write!(record, "{}: ", event.metadata().target()).unwrap();
        DefaultFields::new()
            .format_fields(Writer::new(&mut record), event)
            .unwrap();
        record.push('\n');
        self.0.lock().unwrap().write_all(record.as_bytes()).unwrap();
    }
}

fn event(parent: Option<&tracing::Span>) {
    match parent {
        Some(parent) => {
            tracing::info!(target: "host_record", parent: parent, value = 11, answer = "stable", "retained")
        }
        None => tracing::info!(target: "host_record", value = 11, answer = "stable", "retained"),
    }
}
struct Exit {
    case: String,
    parent: tracing::Span,
    entered: Option<tracing::span::EnteredSpan>,
}
impl Drop for Exit {
    fn drop(&mut self) {
        match self.case.strip_prefix("early-").unwrap_or(&self.case) {
            "entered-context" | "entered-dynamic" => event(None),
            "explicit-dynamic" | "source-dynamic-unentered" => event(Some(&self.parent)),
            "new-span-event" => {
                let span =
                    tracing::info_span!(target: "host_record", parent: None, "watched", owner = 7);
                let _entered = span.enter();
                event(None);
            }
            "source-new-span-only" => {
                let _span = tracing::info_span!(target: "host_record", parent: &self.parent, "late", value = 11);
            }
            case => panic!("unknown late case: {}", case),
        }
        self.entered.take();
    }
}
thread_local! {
    static EXIT: RefCell<Option<Exit>> = const { RefCell::new(None) };
}

#[test]
fn late_event_child() {
    let case = match std::env::var("TRACING_LATE_CASE") {
        Ok(case) => case,
        Err(_) => return,
    };
    // Linux x86_64 rlim_t is an unsigned 64-bit integer. No core output is
    // needed from the deliberately aborting stock controls.
    #[repr(C)]
    struct Limit {
        current: u64,
        maximum: u64,
    }
    extern "C" {
        fn setrlimit(resource: i32, value: *const Limit) -> i32;
    }
    assert_eq!(
        unsafe {
            setrlimit(
                4,
                &Limit {
                    current: 0,
                    maximum: 0,
                },
            )
        },
        0
    );
    let file = File::create(std::env::var_os("TRACING_LATE_OUTPUT").unwrap()).unwrap();
    let filter = EnvFilter::new(if case.contains("dynamic") {
        "off,[watched]=info"
    } else {
        "info"
    });
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry()
            .with(Capture(Arc::new(Mutex::new(file))))
            .with(filter),
    )
    .unwrap();
    std::thread::spawn(move || {
        if !case.starts_with("early-") {
            EXIT.with(|_| ());
        }
        let parent = tracing::info_span!(target: "host_record", "watched", owner = 7);
        let entered = parent.clone().entered();
        event(Some(&parent));
        let entered = if case == "source-dynamic-unentered" || case == "source-new-span-only" {
            drop(entered);
            None
        } else {
            Some(entered)
        };
        EXIT.with(|slot| {
            assert!(slot
                .borrow_mut()
                .replace(Exit {
                    case,
                    parent,
                    entered
                })
                .is_none())
        });
    })
    .join()
    .unwrap();
}

fn run(case: &str) -> (std::process::Output, Vec<u8>) {
    let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "late-record-{}-{}",
        std::process::id(),
        case
    ));
    let reserved = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .unwrap();
    drop(reserved);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "late_event_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("TRACING_LATE_CASE", case)
        .env("TRACING_LATE_OUTPUT", &path)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let until = Instant::now() + Duration::from_secs(10);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= until {
            child.kill().unwrap();
            let output = child.wait_with_output().unwrap();
            panic!("late storage deadline: {:?}", output);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let output = child.wait_with_output().unwrap();
    let bytes = std::fs::read(&path).unwrap();
    std::fs::remove_file(path).unwrap();
    (output, bytes)
}

#[test]
fn late_events_keep_all_142_bytes_or_reproduce_the_unmodified_stock_failures() {
    for case in [
        "entered-context",
        "entered-dynamic",
        "explicit-dynamic",
        "new-span-event",
    ] {
        let (early, bytes) = run(&format!("early-{}", case));
        assert!(early.status.success(), "{}: {:?}", case, early);
        assert!(early.stderr.is_empty(), "{}: {:?}", case, early);
        assert_eq!(bytes, RECORD.repeat(2), "{} early", case);
        let (late, bytes) = run(case);
        if cfg!(feature = "late-events") {
            assert!(late.status.success(), "{}: {:?}", case, late);
            assert!(late.stderr.is_empty(), "{}: {:?}", case, late);
            assert_eq!(bytes, RECORD.repeat(2), "{} late", case);
        } else if case == "entered-context" {
            assert!(late.status.success(), "{:?}", late);
            assert!(late.stderr.is_empty(), "{:?}", late);
            let mut expected = RECORD.to_vec();
            expected.extend_from_slice(b" INFO host_record: retained value=11 answer=\"stable\"\n");
            assert_eq!(bytes, expected);
            assert_eq!(bytes.len(), 124);
        } else {
            assert_eq!(late.status.signal(), Some(6), "{}: {:?}", case, late);
            let diagnostic = if case == "new-span-event" {
                "Thread count overflowed the configured max count."
            } else {
                "cannot access a Thread Local Storage value"
            };
            let stderr = String::from_utf8_lossy(&late.stderr);
            assert!(stderr.contains(diagnostic), "{}: {}", case, stderr);
            assert!(stderr.contains("thread local panicked on drop, aborting"));
            assert_eq!(bytes, RECORD, "{} warmup only", case);
        }
    }
}

#[test]
fn original_new_span_only_and_unentered_dynamic_probes_keep_their_meaning() {
    for case in ["source-new-span-only", "source-dynamic-unentered"] {
        let (output, bytes) = run(case);
        assert_eq!(bytes, RECORD, "{} has no selected second event", case);
        if cfg!(feature = "late-events") {
            assert!(output.status.success(), "{}: {:?}", case, output);
            assert!(output.stderr.is_empty(), "{}: {:?}", case, output);
        } else {
            assert_eq!(output.status.signal(), Some(6), "{}: {:?}", case, output);
            let diagnostic = if case == "source-new-span-only" {
                "Thread count overflowed the configured max count."
            } else {
                "cannot access a Thread Local Storage value"
            };
            assert!(String::from_utf8_lossy(&output.stderr).contains(diagnostic));
        }
    }
}
