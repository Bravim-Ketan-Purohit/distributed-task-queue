#pragma once

#include <atomic>
#include <chrono>
#include <mutex>
#include <vector>
#include <string>

namespace dtq {

struct LatencyRecord {
    double enqueue_ms;
    double e2e_ms; // end-to-end: enqueue to ack
    std::chrono::steady_clock::time_point timestamp;
};

class Stats {
public:
    Stats();

    // Thread-safe counters
    void record_enqueue();
    void record_complete(double latency_ms);
    void record_failure();
    void record_latency(double enqueue_ms, double e2e_ms);

    // Snapshot for periodic reporting
    uint64_t enqueued() const { return enqueued_.load(std::memory_order_relaxed); }
    uint64_t completed() const { return completed_.load(std::memory_order_relaxed); }
    uint64_t failed() const { return failed_.load(std::memory_order_relaxed); }

    // Compute percentiles from recorded latencies
    struct Percentiles {
        double p50;
        double p95;
        double p99;
        double max;
        double mean;
    };

    Percentiles compute_latency_percentiles() const;

    // Compute throughput
    double enqueue_rate() const;
    double complete_rate() const;

    // Mark start/end
    void start();
    void stop();
    double elapsed_seconds() const;

    // Export to JSON string
    std::string to_json(const std::string& host_info, int worker_count,
                        int payload_bytes, double failure_rate) const;

private:
    std::atomic<uint64_t> enqueued_{0};
    std::atomic<uint64_t> completed_{0};
    std::atomic<uint64_t> failed_{0};

    mutable std::mutex latency_mutex_;
    std::vector<double> latencies_; // e2e latencies in ms

    std::chrono::steady_clock::time_point start_time_;
    std::chrono::steady_clock::time_point end_time_;
    bool running_{false};
};

} // namespace dtq
