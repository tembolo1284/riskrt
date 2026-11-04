//! riskrt-grpc: the engine's network edge.
//!
//! The engine publishes immutable versioned values (`RiskSnapshot`,
//! `CurveSet`, `QuoteSnapshot`) through single-writer, many-reader handles.
//! This crate puts a gRPC server over those handles so clients outside the
//! process (a desk UI, Excel, Python) can read them. Nothing here feeds back
//! into the pipeline; the server is one more reader.
//!
//! Each streaming RPC gets a small std thread that blocks on `wait_newer`,
//! exactly like an in-process stage would, and forwards into a tokio
//! channel of capacity one. If the client is slower than the engine, the
//! thread overwrites the pending update rather than queueing, so the client
//! always receives the latest version and never a backlog. That is the same
//! conflation rule the engine applies between its own stages.
//!
//! The tokio runtime lives on its own thread inside `spawn_server`, so the
//! CLI and the engine stay synchronous.

#![forbid(unsafe_code)]

mod bridge;
mod service;

pub use service::{Handles, RiskService};

use riskrt_proto::RiskServer;
use std::io;
use std::net::SocketAddr;
use std::thread::JoinHandle;
use tokio::sync::oneshot;

/// Anything that can go wrong while serving (as opposed to binding).
pub type ServeError = Box<dyn std::error::Error + Send + Sync>;

/// A running server. `stop` asks it to drain; `join` waits for it.
pub struct ServerHandle {
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<Result<(), ServeError>>>,
    addr: SocketAddr,
}

impl ServerHandle {
    /// The address the server actually bound (useful with port 0).
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Begins a graceful shutdown: open streams end, new connections are refused.
    pub fn stop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }

    /// `stop` then wait for the runtime thread to exit.
    pub fn join(mut self) -> Result<(), ServeError> {
        self.stop();
        match self.thread.take() {
            Some(t) => t.join().expect("grpc runtime thread panicked"),
            None => Ok(()),
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Binds `addr` and serves `handles` on a dedicated runtime thread.
///
/// Returns once the listener is bound, so a caller can read
/// `local_addr()` immediately. Errors binding the socket are reported here;
/// errors while serving come back from `join`.
pub fn spawn_server(handles: Handles, addr: SocketAddr) -> io::Result<ServerHandle> {
    let listener = std::net::TcpListener::bind(addr)?;
    listener.set_nonblocking(true)?;
    let bound = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let thread = std::thread::Builder::new()
        .name("grpc".to_owned())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name("grpc-worker")
                .enable_all()
                .build()
                .expect("tokio runtime");
            rt.block_on(async move {
                let listener = tokio::net::TcpListener::from_std(listener)?;
                let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
                tonic::transport::Server::builder()
                    .add_service(RiskServer::new(RiskService::new(handles)))
                    .serve_with_incoming_shutdown(incoming, async {
                        let _ = shutdown_rx.await;
                    })
                    .await?;
                Ok(())
            })
        })?;

    Ok(ServerHandle {
        shutdown: Some(shutdown_tx),
        thread: Some(thread),
        addr: bound,
    })
}
