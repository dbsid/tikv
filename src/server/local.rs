// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! In-process access to the KV service.
//!
//! When TiKV is embedded in another process (for example `tidb-one-server`,
//! which links TiDB and TiKV into one binary), the embedding client can send
//! batch commands to the local store without going through gRPC. The request
//! is still a protobuf `BatchCommandsRequest.Request`; only the transport
//! (HTTP/2 framing, the batch-commands stream envelope and the socket) is
//! removed. Request handling, scheduling, metrics and resource control are the
//! same as for the batch-commands gRPC stream.
//!
//! The registry is disabled unless [`enable_embedded`] is called before the
//! server starts, so a normal `tikv-server` is not affected.

use std::{
    cell::RefCell,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use std::time::Duration;

use futures::future::BoxFuture;
use kvproto::{
    kvrpcpb::HealthFeedback,
    tikvpb::{BatchCommandsRequestRequest, BatchCommandsResponseResponse},
};

use super::Error;

/// A KV service that can execute batch commands in process.
///
/// Implementations are `Send` but not necessarily `Sync`; every calling thread
/// works on its own clone obtained through [`LocalKvService::clone_box`].
pub trait LocalKvService: Send {
    /// Executes one batch command request and resolves to its response.
    fn call(
        &self,
        req: BatchCommandsRequestRequest,
    ) -> BoxFuture<'static, Result<BatchCommandsResponseResponse, Error>>;

    /// Clones the service for use on another thread.
    fn clone_box(&self) -> Box<dyn LocalKvService>;

    /// The store's health feedback (as the batch-commands stream attaches it
    /// to responses) and the interval at which the client should ask again,
    /// or `None` if health feedback is disabled.
    fn health_feedback(&self) -> Option<(HealthFeedback, Duration)> {
        None
    }
}

static EMBEDDED: AtomicBool = AtomicBool::new(false);
static READY: AtomicBool = AtomicBool::new(false);
static STORE_ID: AtomicU64 = AtomicU64::new(0);
/// Bumped on every register/unregister so thread-local clones are refreshed.
static GENERATION: AtomicU64 = AtomicU64::new(0);
static SERVICE: Mutex<Option<Box<dyn LocalKvService>>> = Mutex::new(None);

thread_local! {
    static CACHED: RefCell<Option<(u64, Box<dyn LocalKvService>)>> = const { RefCell::new(None) };
}

/// Marks the process as an embedded deployment. Must be called before the
/// server is created. In embedded mode TiKV does not install its own signal
/// handlers; the host process owns shutdown.
pub fn enable_embedded() {
    EMBEDDED.store(true, Ordering::SeqCst);
}

/// Returns whether TiKV runs embedded in another process.
pub fn is_embedded() -> bool {
    EMBEDDED.load(Ordering::Relaxed)
}

/// Registers the KV service of this process. No-op unless embedded.
pub fn register(store_id: u64, service: Box<dyn LocalKvService>) {
    if !is_embedded() {
        return;
    }
    *SERVICE.lock().unwrap() = Some(service);
    STORE_ID.store(store_id, Ordering::SeqCst);
    GENERATION.fetch_add(1, Ordering::SeqCst);
}

/// Marks the local service as able to serve requests. Called once the gRPC
/// server is running, so the store is registered in PD and reachable by peers.
pub fn set_ready(ready: bool) {
    READY.store(ready, Ordering::SeqCst);
}

/// Returns whether a registered service is ready to serve requests.
pub fn is_ready() -> bool {
    READY.load(Ordering::Acquire) && STORE_ID.load(Ordering::Acquire) != 0
}

/// Returns the store ID of the registered service, or 0.
pub fn store_id() -> u64 {
    STORE_ID.load(Ordering::Acquire)
}

/// Starts `req` on the registered service. Returns `None` if no service is
/// ready, in which case the caller should use gRPC.
pub fn call(
    req: BatchCommandsRequestRequest,
) -> Option<BoxFuture<'static, Result<BatchCommandsResponseResponse, Error>>> {
    with_service(|s| s.call(req))
}

/// Returns the local store's health feedback and the interval to ask again,
/// or `None` if no service is ready or health feedback is disabled.
pub fn health_feedback() -> Option<(HealthFeedback, Duration)> {
    with_service(|s| s.health_feedback()).flatten()
}

fn with_service<T>(f: impl FnOnce(&dyn LocalKvService) -> T) -> Option<T> {
    if !READY.load(Ordering::Acquire) {
        return None;
    }
    let generation = GENERATION.load(Ordering::Acquire);
    CACHED.with(|cached| {
        let mut cached = cached.borrow_mut();
        let stale = !matches!(&*cached, Some((g, _)) if *g == generation);
        if stale {
            let service = SERVICE.lock().unwrap().as_ref().map(|s| s.clone_box());
            *cached = service.map(|s| (generation, s));
        }
        cached.as_ref().map(|(_, s)| f(s.as_ref()))
    })
}

/// Drops the registered service. Called on shutdown.
pub fn unregister() {
    set_ready(false);
    *SERVICE.lock().unwrap() = None;
    GENERATION.fetch_add(1, Ordering::SeqCst);
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;

    use super::*;

    #[derive(Clone)]
    struct Echo;

    impl LocalKvService for Echo {
        fn call(
            &self,
            _req: BatchCommandsRequestRequest,
        ) -> BoxFuture<'static, Result<BatchCommandsResponseResponse, Error>> {
            async { Ok(BatchCommandsResponseResponse::default()) }.boxed()
        }

        fn clone_box(&self) -> Box<dyn LocalKvService> {
            Box::new(self.clone())
        }
    }

    #[test]
    fn test_registry_lifecycle() {
        // Not embedded: registration is ignored.
        register(1, Box::new(Echo));
        set_ready(true);
        assert!(call(Default::default()).is_none());

        enable_embedded();
        set_ready(false);
        register(7, Box::new(Echo));
        assert_eq!(store_id(), 7);
        assert!(call(Default::default()).is_none(), "not ready yet");
        set_ready(true);
        let resp = futures::executor::block_on(call(Default::default()).unwrap()).unwrap();
        assert!(resp.cmd.is_none());
        // Another thread gets its own clone.
        std::thread::spawn(|| assert!(call(Default::default()).is_some()))
            .join()
            .unwrap();

        unregister();
        assert!(call(Default::default()).is_none());
        EMBEDDED.store(false, Ordering::SeqCst);
    }
}
