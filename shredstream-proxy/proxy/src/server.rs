use std::{
    net::SocketAddr,
    path::Path,
    sync::{atomic::AtomicBool, Arc},
    thread::JoinHandle,
    time::Duration,
};

use crossbeam_channel::Receiver;
use jito_protos::{
    filtered::{
        filtered_tx_stream_server::{FilteredTxStream, FilteredTxStreamServer},
        StreamRequest, TxData,
    },
    shredstream::{
        shredstream_proxy_server::{ShredstreamProxy, ShredstreamProxyServer},
        Entry as PbEntry, SubscribeEntriesRequest,
    },
};
use log::{debug, warn};
use tokio::net::UnixListener;
use tokio::sync::broadcast::{error::RecvError, Receiver as BroadcastReceiver, Sender};
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};

// ======================================================
// Server Endpoint Configuration
// ======================================================
#[derive(Debug, Clone)]
pub enum ServerEndpoint {
    Tcp(SocketAddr),
    Unix(String),
}

impl ServerEndpoint {
    /// Parse endpoint from string.
    /// - "http://0.0.0.0:9999" or "9999" -> TCP
    /// - "http://unix:/path/to.sock" or "unix:/path/to.sock" -> Unix socket
    pub fn parse(s: &str) -> Option<Self> {
        // Strip http:// prefix if present
        let s = s.strip_prefix("http://").unwrap_or(s);

        if let Some(path) = s.strip_prefix("unix:") {
            Some(ServerEndpoint::Unix(path.to_string()))
        } else {
            // Try to parse as SocketAddr (e.g., "0.0.0.0:9999" or "[::]:9999")
            // or as a port number only (e.g., "9999")
            s.parse::<SocketAddr>()
                .ok()
                .or_else(|| {
                    // If just a port number, use default address
                    s.parse::<u16>()
                        .ok()
                        .map(|port| SocketAddr::new(std::net::Ipv4Addr::UNSPECIFIED.into(), port))
                })
                .map(ServerEndpoint::Tcp)
        }
    }

    pub fn display(&self) -> String {
        match self {
            ServerEndpoint::Tcp(addr) => addr.to_string(),
            ServerEndpoint::Unix(path) => format!("unix:{}", path),
        }
    }
}

// ======================================================
// Shredstream Proxy Service
// ======================================================
#[derive(Debug)]
pub struct ShredstreamProxyService {
    entry_sender: Arc<Sender<PbEntry>>,
}

pub fn start_server_thread(
    endpoint: ServerEndpoint,
    entry_sender: Arc<Sender<PbEntry>>,
    filtered_tx_sender: Arc<Sender<TxData>>,
    exit: Arc<AtomicBool>,
    shutdown_receiver: Receiver<()>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().unwrap();

        let server_handle = runtime.spawn(async move {
            log::info!("starting gRPC server on {}", endpoint.display());
            let result = match &endpoint {
                ServerEndpoint::Tcp(addr) => {
                    tonic::transport::Server::builder()
                        .add_service(ShredstreamProxyServer::new(ShredstreamProxyService {
                            entry_sender: entry_sender.clone(),
                        }))
                        .add_service(FilteredTxStreamServer::new(FilteredTxServiceImpl {
                            filtered_tx_sender,
                        }))
                        .serve(*addr)
                        .await
                }
                ServerEndpoint::Unix(path) => {
                    // Ensure parent directory exists
                    if let Some(parent) = Path::new(path).parent() {
                        if let Err(e) = std::fs::create_dir_all(parent) {
                            log::warn!("Failed to create socket directory {}: {}", parent.display(), e);
                        }
                    }
                    // Remove existing socket file if present
                    if let Err(e) = std::fs::remove_file(path) {
                        if e.kind() != std::io::ErrorKind::NotFound {
                            log::debug!("Failed to remove existing socket file {}: {}", path, e);
                        }
                    }

                    let listener = match UnixListener::bind(path) {
                        Ok(l) => l,
                        Err(e) => {
                            log::error!("Failed to bind Unix socket {}: {}", path, e);
                            return Ok(());
                        }
                    };
                    tonic::transport::Server::builder()
                        .add_service(ShredstreamProxyServer::new(ShredstreamProxyService {
                            entry_sender: entry_sender.clone(),
                        }))
                        .add_service(FilteredTxStreamServer::new(FilteredTxServiceImpl {
                            filtered_tx_sender,
                        }))
                        .serve_with_incoming(UnixListenerStream::new(listener))
                        .await
                }
            };

            if let Err(e) = result {
                log::error!("gRPC server error: {}", e);
            }

            // Clean up socket file on shutdown for Unix socket
            if let ServerEndpoint::Unix(path) = &endpoint {
                if let Err(e) = std::fs::remove_file(path) {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        log::debug!("Failed to clean up socket file {}: {}", path, e);
                    }
                }
            }

            Ok::<(), tonic::transport::Error>(())
        });

        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            if shutdown_receiver
                .recv_timeout(Duration::from_secs(1))
                .is_ok()
            {
                server_handle.abort();
                log::info!("shutting down gRPC server");
                break;
            }
        }
    })
}

#[tonic::async_trait]
impl ShredstreamProxy for ShredstreamProxyService {
    type SubscribeEntriesStream = ReceiverStream<Result<PbEntry, tonic::Status>>;

    async fn subscribe_entries(
        &self,
        _request: tonic::Request<SubscribeEntriesRequest>,
    ) -> Result<tonic::Response<Self::SubscribeEntriesStream>, tonic::Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(1024);
        let mut entry_receiver: BroadcastReceiver<PbEntry> = self.entry_sender.subscribe();

        tokio::spawn(async move {
            loop {
                match entry_receiver.recv().await {
                    Ok(entry) => match tx.send(Ok(entry)).await {
                        Ok(_) => (),
                        Err(_e) => {
                            debug!("client disconnected");
                            break;
                        }
                    },
                    Err(RecvError::Lagged(skipped)) => {
                        warn!("entry stream lagged, skipped {skipped} messages");
                    }
                    Err(RecvError::Closed) => break,
                }
            }
        });

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}

// ======================================================
// Filtered Transaction Stream Service
// ======================================================
struct FilteredTxServiceImpl {
    filtered_tx_sender: Arc<Sender<TxData>>,
}

#[tonic::async_trait]
impl FilteredTxStream for FilteredTxServiceImpl {
    type StreamTxsStream = ReceiverStream<Result<TxData, tonic::Status>>;

    async fn stream_txs(
        &self,
        _request: tonic::Request<StreamRequest>,
    ) -> Result<tonic::Response<Self::StreamTxsStream>, tonic::Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(1024);
        let mut tx_receiver = self.filtered_tx_sender.subscribe();

        tokio::spawn(async move {
            loop {
                match tx_receiver.recv().await {
                    Ok(tx_data) => match tx.send(Ok(tx_data)).await {
                        Ok(_) => (),
                        Err(_e) => {
                            debug!("filtered stream client disconnected");
                            break;
                        }
                    },
                    Err(RecvError::Lagged(skipped)) => {
                        warn!("filtered tx stream lagged, skipped {skipped} messages");
                    }
                    Err(RecvError::Closed) => break,
                }
            }
        });

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}
