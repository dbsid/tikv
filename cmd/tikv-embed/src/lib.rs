// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! TiKV as a static library.
//!
//! The host process (for example `tidb-one-server`, a TiDB build that links
//! this library through cgo) starts TiKV on a background thread with
//! [`tikv_embed_start`], waits for it with [`tikv_embed_wait_ready`], installs
//! a completion callback with [`tikv_embed_set_done`], and then sends requests
//! for the local store through [`tikv_embed_submit`]. A request is a
//! protobuf-encoded `tikvpb.BatchCommandsRequest.Request`; the response is a
//! protobuf-encoded `tikvpb.BatchCommandsResponse.Response`, delivered to the
//! callback from a TiKV thread. No host thread waits for TiKV, so the host can
//! give up on a request (cancellation, timeout) and many requests can be in
//! flight. TiKV still serves gRPC for remote clients, PD and Raft peers.
//!
//! The C declarations are in `include/tikv_embed.h`.

use std::{
    ffi::{CStr, OsString, c_char},
    panic::{AssertUnwindSafe, catch_unwind},
    path::Path,
    sync::{
        Mutex, OnceLock,
        atomic::{AtomicU8, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use clap::{App, Arg, ArgMatches};
use crypto::fips;
use futures::FutureExt;
use kvproto::tikvpb::BatchCommandsRequestRequest;
use protobuf::Message;
use tikv::{config::TikvConfig, server::local, storage::config::EngineType};
use service::service_event::ServiceEvent;
use tikv_util::mpsc as tikv_mpsc;

/// Result codes shared with the C header.
pub const TIKV_EMBED_OK: i32 = 0;
pub const TIKV_EMBED_UNAVAILABLE: i32 = 1;
pub const TIKV_EMBED_BAD_REQUEST: i32 = 2;
pub const TIKV_EMBED_ERROR: i32 = 3;
pub const TIKV_EMBED_TIMEOUT: i32 = 4;
pub const TIKV_EMBED_EXITED: i32 = 5;
pub const TIKV_EMBED_ALREADY_STARTED: i32 = 6;

const STATE_IDLE: u8 = 0;
const STATE_RUNNING: u8 = 1;
const STATE_EXITED: u8 = 2;

static STATE: AtomicU8 = AtomicU8::new(STATE_IDLE);

struct Runtime {
    service_event_tx: tikv_mpsc::Sender<ServiceEvent>,
    thread: Option<JoinHandle<()>>,
}

static RUNTIME: Mutex<Option<Runtime>> = Mutex::new(None);

fn app() -> App<'static, 'static> {
    App::new("TiKV (embedded)")
        .arg(Arg::with_name("config").long("config").short("C").takes_value(true))
        .arg(Arg::with_name("log-level").long("log-level").short("L").takes_value(true))
        .arg(Arg::with_name("log-file").long("log-file").short("f").takes_value(true))
        .arg(Arg::with_name("addr").long("addr").short("A").takes_value(true))
        .arg(Arg::with_name("advertise-addr").long("advertise-addr").takes_value(true))
        .arg(Arg::with_name("status-addr").long("status-addr").takes_value(true))
        .arg(
            Arg::with_name("advertise-status-addr")
                .long("advertise-status-addr")
                .takes_value(true),
        )
        .arg(Arg::with_name("data-dir").long("data-dir").short("s").takes_value(true))
        .arg(Arg::with_name("capacity").long("capacity").takes_value(true))
        .arg(
            Arg::with_name("pd-endpoints")
                .long("pd-endpoints")
                .aliases(&["pd", "pd-endpoint"])
                .takes_value(true)
                .multiple(true)
                .use_delimiter(true)
                .require_delimiter(true)
                .value_delimiter(","),
        )
        .arg(
            Arg::with_name("labels")
                .long("labels")
                .alias("label")
                .takes_value(true)
                .multiple(true)
                .use_delimiter(true)
                .require_delimiter(true)
                .value_delimiter(","),
        )
        .arg(Arg::with_name("metrics-addr").long("metrics-addr").takes_value(true))
}

fn load_config(matches: &ArgMatches<'_>) -> Result<TikvConfig, String> {
    let mut config = match matches.value_of_os("config") {
        Some(path) => TikvConfig::from_file(Path::new(path), None)
            .map_err(|e| format!("invalid TiKV configuration file: {e}"))?,
        None => TikvConfig::default(),
    };
    server::setup::overwrite_config_with_cmd_args(&mut config, matches);
    config.logger_compatible_adjust();
    config
        .storage
        .validate_engine_type()
        .map_err(|e| format!("invalid storage.engine configuration: {e}"))?;
    Ok(config)
}

/// Starts TiKV on a background thread. `argv` uses the `tikv-server`
/// command-line syntax without the program name.
///
/// # Safety
///
/// `argv` must point to `argc` valid NUL-terminated strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn tikv_embed_start(argv: *const *const c_char, argc: i32) -> i32 {
    if STATE
        .compare_exchange(STATE_IDLE, STATE_RUNNING, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return TIKV_EMBED_ALREADY_STARTED;
    }
    let mut args: Vec<OsString> = vec!["tikv-embed".into()];
    for i in 0..argc.max(0) as usize {
        // SAFETY: guaranteed by the caller.
        let arg = unsafe { CStr::from_ptr(*argv.add(i)) };
        args.push(arg.to_string_lossy().into_owned().into());
    }
    let matches = match app().get_matches_from_safe(args) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("tikv-embed: {e}");
            STATE.store(STATE_EXITED, Ordering::SeqCst);
            return TIKV_EMBED_BAD_REQUEST;
        }
    };
    let config = match load_config(&matches) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("tikv-embed: {e}");
            STATE.store(STATE_EXITED, Ordering::SeqCst);
            return TIKV_EMBED_BAD_REQUEST;
        }
    };

    fips::maybe_enable();
    local::enable_embedded();
    let (service_event_tx, service_event_rx) = tikv_mpsc::unbounded();
    let tx = service_event_tx.clone();
    let thread = std::thread::Builder::new()
        .name("tikv-embed-main".to_owned())
        .spawn(move || {
            server::setup::initial_logger(&config);
            tikv::log_tikv_info(option_env!("TIKV_BUILD_TIME"));
            fips::log_status();
            let mut config = config;
            config.memory.init();
            match config.storage.engine {
                EngineType::RaftKv => server::server::run_tikv(config, tx, service_event_rx),
                EngineType::RaftKv2 => server::server2::run_tikv(config, tx, service_event_rx),
            }
            STATE.store(STATE_EXITED, Ordering::SeqCst);
        });
    match thread {
        Ok(handle) => {
            *RUNTIME.lock().unwrap() = Some(Runtime {
                service_event_tx,
                thread: Some(handle),
            });
            TIKV_EMBED_OK
        }
        Err(e) => {
            eprintln!("tikv-embed: failed to spawn TiKV thread: {e}");
            STATE.store(STATE_EXITED, Ordering::SeqCst);
            TIKV_EMBED_ERROR
        }
    }
}

/// Waits until the local store serves requests. Returns `TIKV_EMBED_OK`,
/// `TIKV_EMBED_TIMEOUT`, or `TIKV_EMBED_EXITED` if TiKV stopped.
#[unsafe(no_mangle)]
pub extern "C" fn tikv_embed_wait_ready(timeout_ms: u64) -> i32 {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        if local::is_ready() {
            return TIKV_EMBED_OK;
        }
        if STATE.load(Ordering::SeqCst) == STATE_EXITED {
            return TIKV_EMBED_EXITED;
        }
        if Instant::now() >= deadline {
            return TIKV_EMBED_TIMEOUT;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Returns the ID of the local store, or 0 before it is known.
#[unsafe(no_mangle)]
pub extern "C" fn tikv_embed_store_id() -> u64 {
    local::store_id()
}

fn into_raw(buf: Vec<u8>, out: *mut *mut u8, out_len: *mut usize) {
    let boxed = buf.into_boxed_slice();
    let len = boxed.len();
    let ptr = Box::into_raw(boxed) as *mut u8;
    // SAFETY: the caller passes valid out pointers.
    unsafe {
        *out = ptr;
        *out_len = len;
    }
}

/// Completion callback of [`tikv_embed_submit`]: `token` identifies the
/// request, `code` is `TIKV_EMBED_OK` (`resp` holds an encoded
/// `BatchCommandsResponse.Response`) or `TIKV_EMBED_ERROR` (`resp` holds a
/// UTF-8 error message). The callee owns `resp` and releases it with
/// [`tikv_embed_free`]. It runs on a TiKV thread and must not block.
pub type DoneFn = extern "C" fn(token: u64, code: i32, resp: *mut u8, resp_len: usize);

static DONE: OnceLock<DoneFn> = OnceLock::new();

/// Threads that wait for local responses, encode them and run the callback.
/// The requests themselves run on TiKV's pools.
const COMPLETION_THREADS: usize = 4;

fn completion_runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(COMPLETION_THREADS)
            .thread_name("tikv-embed-done")
            .build()
            .expect("tikv-embed: build completion runtime")
    })
}

/// Installs the completion callback for [`tikv_embed_submit`]. Only the first
/// call has an effect.
#[unsafe(no_mangle)]
pub extern "C" fn tikv_embed_set_done(done: DoneFn) {
    let _ = DONE.set(done);
}

fn complete(done: DoneFn, token: u64, code: i32, buf: Vec<u8>) {
    let boxed = buf.into_boxed_slice();
    let len = boxed.len();
    let ptr = Box::into_raw(boxed) as *mut u8;
    done(token, code, ptr, len);
}

/// Starts one encoded `BatchCommandsRequest.Request` on the local store and
/// returns without waiting for it.
///
/// On `TIKV_EMBED_OK` the request is accepted and the callback installed by
/// [`tikv_embed_set_done`] is called exactly once with `token`, possibly
/// before this function returns. Any other code means the request was not
/// accepted and the callback is not called: `TIKV_EMBED_UNAVAILABLE` (use
/// gRPC; also returned before a callback is installed) or
/// `TIKV_EMBED_BAD_REQUEST` (the request does not decode).
///
/// # Safety
///
/// `req` must point to `req_len` readable bytes during the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn tikv_embed_submit(req: *const u8, req_len: usize, token: u64) -> i32 {
    let Some(&done) = DONE.get() else {
        return TIKV_EMBED_UNAVAILABLE;
    };
    if !local::is_ready() {
        return TIKV_EMBED_UNAVAILABLE;
    }
    let bytes = if req_len == 0 {
        &[][..]
    } else {
        // SAFETY: guaranteed by the caller.
        unsafe { std::slice::from_raw_parts(req, req_len) }
    };
    let mut request = BatchCommandsRequestRequest::new();
    if request.merge_from_bytes(bytes).is_err() {
        return TIKV_EMBED_BAD_REQUEST;
    }
    let fut = match catch_unwind(AssertUnwindSafe(|| local::call(request))) {
        Ok(Some(fut)) => fut,
        Ok(None) => return TIKV_EMBED_UNAVAILABLE,
        Err(_) => return TIKV_EMBED_ERROR,
    };
    completion_runtime().spawn(async move {
        let (code, buf) = match AssertUnwindSafe(fut).catch_unwind().await {
            Ok(Ok(response)) => match response.write_to_bytes() {
                Ok(buf) => (TIKV_EMBED_OK, buf),
                Err(e) => (TIKV_EMBED_ERROR, format!("encode response: {e}").into_bytes()),
            },
            Ok(Err(e)) => (TIKV_EMBED_ERROR, e.to_string().into_bytes()),
            Err(_) => (TIKV_EMBED_ERROR, b"local request panicked".to_vec()),
        };
        complete(done, token, code, buf);
    });
    TIKV_EMBED_OK
}

/// Writes the local store's health feedback, an encoded
/// `kvrpcpb.HealthFeedback`, to `*out` (release it with [`tikv_embed_free`])
/// and the interval in milliseconds after which to ask again to
/// `*interval_ms`. Returns `TIKV_EMBED_UNAVAILABLE` if the store is not ready
/// or health feedback is disabled (`server.health-feedback-interval = 0`).
///
/// # Safety
///
/// `out`, `out_len` and `interval_ms` must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn tikv_embed_health_feedback(
    out: *mut *mut u8,
    out_len: *mut usize,
    interval_ms: *mut u64,
) -> i32 {
    // SAFETY: guaranteed by the caller.
    unsafe {
        *out = std::ptr::null_mut();
        *out_len = 0;
        *interval_ms = 0;
    }
    let Ok(Some((feedback, interval))) = catch_unwind(local::health_feedback) else {
        return TIKV_EMBED_UNAVAILABLE;
    };
    match feedback.write_to_bytes() {
        Ok(buf) => {
            into_raw(buf, out, out_len);
            // SAFETY: guaranteed by the caller.
            unsafe { *interval_ms = interval.as_millis() as u64 };
            TIKV_EMBED_OK
        }
        Err(_) => TIKV_EMBED_ERROR,
    }
}

/// Releases a buffer passed to the completion callback or returned by
/// [`tikv_embed_health_feedback`].
///
/// # Safety
///
/// `ptr` and `len` must come from one of those buffers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn tikv_embed_free(ptr: *mut u8, len: usize) {
    if ptr.is_null() {
        return;
    }
    // SAFETY: the buffer was created by `into_raw` with this length.
    unsafe {
        drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, len)));
    }
}

/// Stops TiKV and waits up to `timeout_ms` for it to exit. With `graceful`
/// set, leaders are transferred away first (`server.graceful-shutdown-timeout`).
#[unsafe(no_mangle)]
pub extern "C" fn tikv_embed_stop(graceful: bool, timeout_ms: u64) -> i32 {
    local::set_ready(false);
    let mut guard = RUNTIME.lock().unwrap();
    let Some(runtime) = guard.as_mut() else {
        return TIKV_EMBED_OK;
    };
    let event = if graceful {
        ServiceEvent::GracefulShutdown
    } else {
        ServiceEvent::Exit
    };
    let _ = runtime.service_event_tx.send(event);
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    while STATE.load(Ordering::SeqCst) != STATE_EXITED {
        if Instant::now() >= deadline {
            return TIKV_EMBED_TIMEOUT;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    if let Some(handle) = runtime.thread.take() {
        let _ = handle.join();
    }
    TIKV_EMBED_OK
}
