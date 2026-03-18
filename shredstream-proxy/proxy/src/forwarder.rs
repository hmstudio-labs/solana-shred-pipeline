use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, RwLock,
    },
    thread::Builder,
    time::Duration,
};

use crossbeam_channel::Receiver;
use solana_metrics::datapoint_info;
use solana_perf::deduper::Deduper;

// values copied from https://github.com/solana-labs/solana/blob/33bde55bbdde13003acf45bb6afe6db4ab599ae4/core/src/sigverify_shreds.rs#L20
pub const DEDUPER_FALSE_POSITIVE_RATE: f64 = 0.001;
pub const DEDUPER_NUM_BITS: u64 = 637_534_199; // 76MB
pub const DEDUPER_RESET_CYCLE: Duration = Duration::from_secs(5 * 60);

/// Reset dedup + send metrics to influx
pub fn start_forwarder_accessory_thread(
    deduper: Arc<RwLock<Deduper<2, [u8]>>>,
    metrics: Arc<ShredMetrics>,
    metrics_update_interval_ms: u64,
    shutdown_receiver: Receiver<()>,
    exit: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    Builder::new()
        .name("ssPxyAccessory".to_string())
        .spawn(move || {
            let metrics_tick =
                crossbeam_channel::tick(Duration::from_millis(metrics_update_interval_ms));
            let deduper_reset_tick = crossbeam_channel::tick(Duration::from_secs(2));
            let mut rng = rand::thread_rng();
            while !exit.load(Ordering::Relaxed) {
                crossbeam_channel::select! {
                    // reset deduper to avoid false positives
                    recv(deduper_reset_tick) -> _ => {
                        deduper
                            .write()
                            .unwrap()
                            .maybe_reset(&mut rng, DEDUPER_FALSE_POSITIVE_RATE, DEDUPER_RESET_CYCLE);
                    }

                    // send metrics to influx
                    recv(metrics_tick) -> _ => {
                        metrics.report();
                        metrics.reset();
                    }

                    // handle SIGINT shutdown
                    recv(shutdown_receiver) -> _ => {
                        break;
                    }
                }
            }
        })
        .unwrap()
}

pub struct ShredMetrics {
    // receive stats
    /// Total number of shreds received.
    pub received: AtomicU64,
    /// Total number of packet batches processed.
    pub packet_batch_count: AtomicU64,
    /// Number of shreds skipped before copying because they are too old.
    pub precopy_old_slot_skip_count: AtomicU64,
    /// Number of shreds skipped before copying because slot/fec/index was already completed.
    pub precopy_completed_skip_count: AtomicU64,
    /// Total time spent parsing shred payloads from packets.
    pub parse_shred_elapsed_us: AtomicU64,
    /// Total time spent in packet deduplication.
    pub dedup_elapsed_us: AtomicU64,
    /// Total time spent in reconstruct_shreds.
    pub reconstruct_elapsed_us: AtomicU64,
    /// Number of FEC recovery attempts.
    pub fec_recovery_attempt_count: AtomicU64,
    /// Number of FEC sets recovered successfully.
    pub fec_recovery_success_count: AtomicU64,
    /// Total time spent in FEC recovery attempts.
    pub fec_recovery_elapsed_us: AtomicU64,
    /// Number of deshred segments attempted.
    pub deshred_segment_count: AtomicU64,
    /// Total time spent in Shredder::deshred.
    pub deshred_elapsed_us: AtomicU64,
    /// Number of bincode deserialize attempts.
    pub bincode_deserialize_attempt_count: AtomicU64,
    /// Total time spent in bincode entry deserialization.
    pub bincode_deserialize_elapsed_us: AtomicU64,
    /// Number of filtered transactions emitted to gRPC.
    pub filtered_tx_match_count: AtomicU64,
    /// Number of filter_entries invocations.
    pub filter_invocation_count: AtomicU64,
    /// Total time spent filtering transactions from entries.
    pub filter_elapsed_us: AtomicU64,
    /// Number of entry messages sent to gRPC broadcast.
    pub grpc_entry_send_count: AtomicU64,
    /// Total time spent sending entry payloads to gRPC broadcast.
    pub grpc_entry_send_elapsed_us: AtomicU64,

    // service metrics
    pub enabled_grpc_service: bool,
    /// Number of data shreds recovered using coding shreds
    pub recovered_count: AtomicU64,
    /// Number of Solana entries decoded from shreds
    pub entry_count: AtomicU64,
    /// Number of transactions decoded from shreds
    pub txn_count: AtomicU64,
    /// Number of times we couldn't find the previous DATA_COMPLETE_SHRED flag
    pub unknown_start_position_count: AtomicU64,
    /// Number of FEC recovery errors
    pub fec_recovery_error_count: AtomicU64,
    /// Number of bincode Entry deserialization errors
    pub bincode_deserialize_error_count: AtomicU64,
    /// Number of times we couldn't find the previous DATA_COMPLETE_SHRED flag but tried to deshred+deserialize, and failed
    pub unknown_start_position_error_count: AtomicU64,

    // cumulative metrics (persist after reset)
    pub agg_received_cumulative: AtomicU64,
}

impl Default for ShredMetrics {
    fn default() -> Self {
        Self::new(false)
    }
}

impl ShredMetrics {
    pub fn new(enabled_grpc_service: bool) -> Self {
        Self {
            enabled_grpc_service,
            received: Default::default(),
            packet_batch_count: Default::default(),
            precopy_old_slot_skip_count: Default::default(),
            precopy_completed_skip_count: Default::default(),
            parse_shred_elapsed_us: Default::default(),
            dedup_elapsed_us: Default::default(),
            reconstruct_elapsed_us: Default::default(),
            fec_recovery_attempt_count: Default::default(),
            fec_recovery_success_count: Default::default(),
            fec_recovery_elapsed_us: Default::default(),
            deshred_segment_count: Default::default(),
            deshred_elapsed_us: Default::default(),
            bincode_deserialize_attempt_count: Default::default(),
            bincode_deserialize_elapsed_us: Default::default(),
            filtered_tx_match_count: Default::default(),
            filter_invocation_count: Default::default(),
            filter_elapsed_us: Default::default(),
            grpc_entry_send_count: Default::default(),
            grpc_entry_send_elapsed_us: Default::default(),
            recovered_count: Default::default(),
            entry_count: Default::default(),
            txn_count: Default::default(),
            unknown_start_position_count: Default::default(),
            fec_recovery_error_count: Default::default(),
            bincode_deserialize_error_count: Default::default(),
            unknown_start_position_error_count: Default::default(),
            agg_received_cumulative: Default::default(),
        }
    }

    pub fn report(&self) {
        let received = self.received.load(Ordering::Relaxed);
        let packet_batches = self.packet_batch_count.swap(0, Ordering::Relaxed);
        let precopy_old_slot_skip_count = self.precopy_old_slot_skip_count.swap(0, Ordering::Relaxed);
        let precopy_completed_skip_count =
            self.precopy_completed_skip_count.swap(0, Ordering::Relaxed);
        let parse_shred_elapsed_us = self.parse_shred_elapsed_us.swap(0, Ordering::Relaxed);
        let dedup_elapsed_us = self.dedup_elapsed_us.swap(0, Ordering::Relaxed);
        let reconstruct_elapsed_us = self.reconstruct_elapsed_us.swap(0, Ordering::Relaxed);
        let fec_recovery_attempt_count =
            self.fec_recovery_attempt_count.swap(0, Ordering::Relaxed);
        let fec_recovery_success_count =
            self.fec_recovery_success_count.swap(0, Ordering::Relaxed);
        let fec_recovery_elapsed_us = self.fec_recovery_elapsed_us.swap(0, Ordering::Relaxed);
        let deshred_segment_count = self.deshred_segment_count.swap(0, Ordering::Relaxed);
        let deshred_elapsed_us = self.deshred_elapsed_us.swap(0, Ordering::Relaxed);
        let bincode_deserialize_attempt_count =
            self.bincode_deserialize_attempt_count.swap(0, Ordering::Relaxed);
        let bincode_deserialize_elapsed_us =
            self.bincode_deserialize_elapsed_us.swap(0, Ordering::Relaxed);
        let filtered_tx_match_count = self.filtered_tx_match_count.swap(0, Ordering::Relaxed);
        let filter_invocation_count = self.filter_invocation_count.swap(0, Ordering::Relaxed);
        let filter_elapsed_us = self.filter_elapsed_us.swap(0, Ordering::Relaxed);
        let grpc_entry_send_count = self.grpc_entry_send_count.swap(0, Ordering::Relaxed);
        let grpc_entry_send_elapsed_us = self.grpc_entry_send_elapsed_us.swap(0, Ordering::Relaxed);
        let recovered_count = self.recovered_count.swap(0, Ordering::Relaxed);
        let unknown_start_position_count =
            self.unknown_start_position_count.swap(0, Ordering::Relaxed);
        let fec_recovery_error_count = self.fec_recovery_error_count.swap(0, Ordering::Relaxed);
        let bincode_deserialize_error_count =
            self.bincode_deserialize_error_count.swap(0, Ordering::Relaxed);
        let unknown_start_position_error_count =
            self.unknown_start_position_error_count.swap(0, Ordering::Relaxed);

        datapoint_info!(
            "shredstream_proxy-metrics",
            ("received", received, i64),
            ("packet_batches", packet_batches, i64),
            ("precopy_old_slot_skip_count", precopy_old_slot_skip_count, i64),
            (
                "precopy_completed_skip_count",
                precopy_completed_skip_count,
                i64
            ),
            ("parse_shred_elapsed_us", parse_shred_elapsed_us, i64),
            ("dedup_elapsed_us", dedup_elapsed_us, i64),
            ("reconstruct_elapsed_us", reconstruct_elapsed_us, i64),
        );

        if self.enabled_grpc_service {
            datapoint_info!(
                "shredstream_proxy-service_metrics",
                ("recovered_count", recovered_count, i64),
                ("entry_count", self.entry_count.load(Ordering::Relaxed), i64),
                ("txn_count", self.txn_count.load(Ordering::Relaxed), i64),
                ("unknown_start_position_count", unknown_start_position_count, i64),
                ("fec_recovery_error_count", fec_recovery_error_count, i64),
                (
                    "bincode_deserialize_error_count",
                    bincode_deserialize_error_count,
                    i64
                ),
                (
                    "unknown_start_position_error_count",
                    unknown_start_position_error_count,
                    i64
                ),
                ("fec_recovery_attempt_count", fec_recovery_attempt_count, i64),
                ("fec_recovery_success_count", fec_recovery_success_count, i64),
                ("fec_recovery_elapsed_us", fec_recovery_elapsed_us, i64),
                ("deshred_segment_count", deshred_segment_count, i64),
                ("deshred_elapsed_us", deshred_elapsed_us, i64),
                (
                    "bincode_deserialize_attempt_count",
                    bincode_deserialize_attempt_count,
                    i64
                ),
                (
                    "bincode_deserialize_elapsed_us",
                    bincode_deserialize_elapsed_us,
                    i64
                ),
                ("filtered_tx_match_count", filtered_tx_match_count, i64),
                ("filter_invocation_count", filter_invocation_count, i64),
                ("filter_elapsed_us", filter_elapsed_us, i64),
                ("grpc_entry_send_count", grpc_entry_send_count, i64),
                ("grpc_entry_send_elapsed_us", grpc_entry_send_elapsed_us, i64),
            );

            let avg = |total: u64, count: u64| -> u64 {
                if count == 0 { 0 } else { total / count }
            };
            log::info!(
                "5s metrics: recv_shreds={} batches={} parse_us={} dedup_us={} reconstruct_us={} fec_attempts={} fec_success_sets={} fec_recovered_shreds={} fec_us={} deshred_segments={} deshred_avg_us={} bincode_attempts={} bincode_avg_us={} filtered_matches={} filter_calls={} filter_avg_us={} grpc_entry_sends={} grpc_entry_avg_us={} precopy_old_skips={} precopy_done_skips={}",
                received,
                packet_batches,
                parse_shred_elapsed_us,
                dedup_elapsed_us,
                reconstruct_elapsed_us,
                fec_recovery_attempt_count,
                fec_recovery_success_count,
                recovered_count,
                fec_recovery_elapsed_us,
                deshred_segment_count,
                avg(deshred_elapsed_us, deshred_segment_count),
                bincode_deserialize_attempt_count,
                avg(bincode_deserialize_elapsed_us, bincode_deserialize_attempt_count),
                filtered_tx_match_count,
                filter_invocation_count,
                avg(filter_elapsed_us, filter_invocation_count),
                grpc_entry_send_count,
                avg(grpc_entry_send_elapsed_us, grpc_entry_send_count),
                precopy_old_slot_skip_count,
                precopy_completed_skip_count,
            );
        }
    }

    /// resets current values, increments cumulative values
    pub fn reset(&self) {
        self.agg_received_cumulative
            .fetch_add(self.received.swap(0, Ordering::Relaxed), Ordering::Relaxed);
    }
}
