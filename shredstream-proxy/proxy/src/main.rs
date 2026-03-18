use std::{
    io,
    net::IpAddr,
    panic,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    thread,
    thread::{sleep, Builder},
    time::{Duration, Instant},
};

use clap::Parser;
use crossbeam_channel::{Receiver, Sender};
use log::*;
use signal_hook::consts::{SIGINT, SIGTERM};
use solana_metrics::set_host_id;
use solana_perf::{
    deduper::Deduper,
    packet::PacketBatchRecycler,
    recycler::Recycler,
};
use solana_streamer::streamer::StreamerReceiveStats;
use tokio::sync::broadcast::Sender as BroadcastSender;

use crate::deshred::ShredsStateTracker;
use crate::forwarder::ShredMetrics;
use crate::multicast_config::create_multicast_socket_on_device;
use crate::server::ServerEndpoint;

mod deshred;
mod forwarder;
mod multicast_config;
mod server;

#[derive(Clone, Debug, Parser)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Address where Shredstream proxy listens.
    #[arg(long, env, default_value_t = IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)))]
    src_bind_addr: IpAddr,

    /// Port where Shredstream proxy listens. Use `0` for random ephemeral port.
    #[arg(long, env, default_value_t = 20_000)]
    src_bind_port: u16,

    /// Multicast IP to listen for shreds. If none provided, attempts to
    /// parse multicast routes for the device specified by `--multicast-device`
    /// via `ip --json route show dev <device>`.
    #[arg(long, env)]
    multicast_bind_ip: Option<IpAddr>,

    /// Network device to use for multicast route discovery and interface selection.
    /// Example: `eth0`, `en0`, or `doublezero1`.
    #[arg(long, env, default_value = "doublezero1")]
    multicast_device: String,

    /// Port to receive multicast shreds
    #[arg(long, env, default_value_t = 20001)]
    multicast_subscribe_port: u16,

    /// Interval between logging stats to stdout and influx
    #[arg(long, env, default_value_t = 5_000)]
    metrics_report_interval_ms: u64,

    /// GRPC endpoint for serving decoded shreds as Solana entries.
    /// Format: PORT (e.g., 9999) for TCP, or "unix:PATH" (e.g., "unix:/tmp/shredstream.sock") for Unix socket.
    #[arg(long, env)]
    grpc_service_endpoint: String,

    /// Number of threads to use. Defaults to use up to 4.
    #[arg(long, env)]
    num_threads: Option<usize>,
}

// Creates a channel that gets a message every time `SIGINT` is signalled.
fn shutdown_notifier(exit: Arc<AtomicBool>) -> io::Result<(Sender<()>, Receiver<()>)> {
    let (s, r) = crossbeam_channel::bounded(256);
    let mut signals = signal_hook::iterator::Signals::new([SIGINT, SIGTERM])?;

    let s_thread = s.clone();
    thread::spawn(move || {
        for _ in signals.forever() {
            exit.store(true, Ordering::SeqCst);
            // send shutdown signal multiple times since crossbeam doesn't have broadcast channels
            // each thread will consume a shutdown signal
            for _ in 0..256 {
                if s_thread.send(()).is_err() {
                    break;
                }
            }
        }
    });

    Ok((s, r))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::builder().init();

    let args: Args = Args::parse();

    set_host_id(hostname::get()?.into_string().unwrap());

    let exit = Arc::new(AtomicBool::new(false));
    let (shutdown_sender, shutdown_receiver) =
        shutdown_notifier(exit.clone()).expect("Failed to set up signal handler");
    let panic_hook = panic::take_hook();
    {
        let exit = exit.clone();
        panic::set_hook(Box::new(move |panic_info| {
            exit.store(true, Ordering::SeqCst);
            let _ = shutdown_sender.send(());
            error!("exiting process");
            sleep(Duration::from_secs(1));
            // invoke the default handler and exit the process
            panic_hook(panic_info);
        }));
    }

    let metrics = Arc::new(ShredMetrics::new(true));

    let num_threads = args
        .num_threads
        .unwrap_or_else(|| usize::from(std::thread::available_parallelism().unwrap()).min(4));

    let recycler: PacketBatchRecycler = Recycler::warmed(100, 1024);

    let deduper = Arc::new(RwLock::new(Deduper::<2, [u8]>::new(
        &mut rand::thread_rng(),
        forwarder::DEDUPER_NUM_BITS,
    )));

    let entry_sender = Arc::new(BroadcastSender::new(4096));
    let filtered_tx_sender = Arc::new(BroadcastSender::new(4096));

    // Parse gRPC endpoint
    let grpc_endpoint = ServerEndpoint::parse(&args.grpc_service_endpoint)
        .ok_or_else(|| format!("Invalid gRPC endpoint format: '{}'. Expected format: PORT (e.g., 9999), HOST:PORT (e.g., 0.0.0.0:9999), or unix:PATH (e.g., unix:/tmp/shredstream.sock)", args.grpc_service_endpoint))?;

    // Start gRPC server
    let server_hdl = server::start_server_thread(
        grpc_endpoint,
        entry_sender.clone(),
        filtered_tx_sender.clone(),
        exit.clone(),
        shutdown_receiver.clone(),
    );

    // Create multicast socket (optional)
    let multicast_sockets = create_multicast_socket_on_device(
        &args.multicast_device,
        args.multicast_subscribe_port,
        args.multicast_bind_ip,
    ).unwrap_or_default();
    if !multicast_sockets.is_empty() {
        info!("Multicast listeners found: {:?}", multicast_sockets);
    } else {
        info!("No multicast listeners found, running in unicast-only mode");
    }

    // Bind listening socket for unicast shreds
    let (port, sockets) = solana_net_utils::multi_bind_in_range_with_config(
        args.src_bind_addr,
        (args.src_bind_port, args.src_bind_port + 1),
        solana_net_utils::SocketConfig::default().reuseport(true),
        num_threads,
    )?;
    info!("Bound to port {}", port);

    let mut thread_handles = vec![server_hdl];

    // Start shred receiver and reconstruct threads
    for (thread_id, incoming_shred_socket) in sockets.into_iter().chain(multicast_sockets).enumerate() {
        let (packet_sender, packet_receiver) = crossbeam_channel::unbounded();
        let listen_thread = solana_streamer::streamer::receiver(
            format!("ssListen{thread_id}"),
            Arc::new(incoming_shred_socket),
            exit.clone(),
            packet_sender,
            recycler.clone(),
            Arc::new(StreamerReceiveStats::new("shredstream_proxy-listen_thread")),
            Duration::default(),
            false,
            None,
            false,
        );
        thread_handles.push(listen_thread);

        let deduper = deduper.clone();
        let metrics = metrics.clone();
        let entry_sender = entry_sender.clone();
        let filtered_tx_sender = filtered_tx_sender.clone();
        let exit = exit.clone();
        let shutdown_receiver = shutdown_receiver.clone();

        let process_thread = Builder::new()
            .name(format!("ssProcess_{thread_id}"))
            .spawn(move || {
                let mut all_shreds = ahash::HashMap::<
                    u64, // Slot
                    (
                        ahash::HashMap<u32, crate::deshred::FecSetShreds>,
                        ShredsStateTracker,
                    ),
                >::default();
                let mut slot_fec_indexes_to_iterate = Vec::<(u64, u32)>::new();
                let mut deshredded_entries =
                    Vec::<(u64, Vec<solana_entry::entry::Entry>, Vec<u8>)>::new();
                let mut packet_batch_vec = Vec::with_capacity(1);
                let mut highest_slot_seen: u64 = 0;
                let rs_cache = solana_ledger::shred::ReedSolomonCache::default();

                while !exit.load(std::sync::atomic::Ordering::Relaxed) {
                    crossbeam_channel::select! {
                        recv(packet_receiver) -> packet_batch => {
                            let Ok(packet_batch) = packet_batch else {
                                break;
                            };

                            metrics
                                .received
                                .fetch_add(packet_batch.len() as u64, Ordering::Relaxed);
                            metrics
                                .packet_batch_count
                                .fetch_add(1, Ordering::Relaxed);

                            packet_batch_vec.clear();
                            packet_batch_vec.push(packet_batch);
                            let dedup_start = Instant::now();
                            solana_perf::deduper::dedup_packets_and_count_discards(
                                &deduper.read().unwrap(),
                                &mut packet_batch_vec,
                            );
                            metrics.dedup_elapsed_us.fetch_add(
                                dedup_start.elapsed().as_micros() as u64,
                                Ordering::Relaxed,
                            );

                            let filtered_sender = (filtered_tx_sender.receiver_count() > 0)
                                .then_some(filtered_tx_sender.as_ref());
                            let emit_entries_to_grpc = entry_sender.receiver_count() > 0;
                            let reconstruct_start = Instant::now();
                            deshred::reconstruct_shreds(
                                packet_batch_vec
                                    .pop()
                                    .expect("packet batch vec should contain the current batch"),
                                &mut all_shreds,
                                &mut slot_fec_indexes_to_iterate,
                                &mut deshredded_entries,
                                &mut highest_slot_seen,
                                &rs_cache,
                                &metrics,
                                emit_entries_to_grpc,
                                &filtered_sender,
                            );
                            metrics.reconstruct_elapsed_us.fetch_add(
                                reconstruct_start.elapsed().as_micros() as u64,
                                Ordering::Relaxed,
                            );

                            if emit_entries_to_grpc {
                                let grpc_send_start = Instant::now();
                                let mut grpc_entry_send_count = 0u64;
                                deshredded_entries.drain(..).for_each(
                                    |(slot, _entries, entries_bytes)| {
                                        grpc_entry_send_count += 1;
                                        let _ = entry_sender.send(jito_protos::shredstream::Entry {
                                            slot,
                                            entries: entries_bytes,
                                        });
                                    },
                                );
                                metrics.grpc_entry_send_count.fetch_add(
                                    grpc_entry_send_count,
                                    Ordering::Relaxed,
                                );
                                metrics.grpc_entry_send_elapsed_us.fetch_add(
                                    grpc_send_start.elapsed().as_micros() as u64,
                                    Ordering::Relaxed,
                                );
                            } else {
                                deshredded_entries.clear();
                            }
                        }
                        recv(shutdown_receiver) -> _ => {
                            break;
                        }
                    }
                }
                info!("Exiting shred processing thread {thread_id}.");
            })
            .unwrap();
        thread_handles.push(process_thread);
    }

    // Start metrics reporting thread
    let metrics_hdl = Builder::new()
        .name("ssPxyMetrics".to_string())
        .spawn({
            let metrics = metrics.clone();
            move || {
                let metrics_tick = crossbeam_channel::tick(Duration::from_millis(args.metrics_report_interval_ms));
                let deduper_reset_tick = crossbeam_channel::tick(Duration::from_secs(2));
                let mut rng = rand::thread_rng();
                while !exit.load(Ordering::Relaxed) {
                    crossbeam_channel::select! {
                        recv(deduper_reset_tick) -> _ => {
                            deduper
                                .write()
                                .unwrap()
                                .maybe_reset(&mut rng, forwarder::DEDUPER_FALSE_POSITIVE_RATE, forwarder::DEDUPER_RESET_CYCLE);
                        }
                        recv(metrics_tick) -> _ => {
                            metrics.report();
                            metrics.reset();
                        }
                        recv(shutdown_receiver) -> _ => {
                            break;
                        }
                    }
                }
            }
        })
        .unwrap();
    thread_handles.push(metrics_hdl);

    info!(
        "Shredstream proxy started, listening on {}:{}/udp. gRPC server on {}",
        args.src_bind_addr, args.src_bind_port, args.grpc_service_endpoint
    );

    for thread in thread_handles {
        thread.join().expect("thread panicked");
    }

    info!(
        "Exiting Shredstream proxy, {} shreds received, {} entries decoded, {} transactions filtered.",
        metrics.agg_received_cumulative.load(Ordering::Relaxed),
        metrics.entry_count.load(Ordering::Relaxed),
        metrics.txn_count.load(Ordering::Relaxed),
    );
    Ok(())
}
