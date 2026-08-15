#include "stats.hpp"
#include <algorithm>
#include <numeric>
#include <nlohmann/json.hpp>
#include <sstream>
#include <ctime>
#include <iomanip>

namespace dtq {

Stats::Stats() = default;

void Stats::record_enqueue() {
    enqueued_.fetch_add(1, std::memory_order_relaxed);
}

void Stats::record_complete(double latency_ms) {
    completed_.fetch_add(1, std::memory_order_relaxed);
    std::lock_guard<std::mutex> lock(latency_mutex_);
    latencies_.push_back(latency_ms);
}

void Stats::record_failure() {
    failed_.fetch_add(1, std::memory_order_relaxed);
}

void Stats::record_latency(double enqueue_ms, double e2e_ms) {
    std::lock_guard<std::mutex> lock(latency_mutex_);
    latencies_.push_back(e2e_ms);
}

Stats::Percentiles Stats::compute_latency_percentiles() const {
    std::lock_guard<std::mutex> lock(latency_mutex_);
    Percentiles p{0, 0, 0, 0, 0};
    if (latencies_.empty()) return p;

    std::vector<double> sorted = latencies_;
    std::sort(sorted.begin(), sorted.end());

    auto percentile = [&](double pct) -> double {
        size_t idx = static_cast<size_t>(pct / 100.0 * (sorted.size() - 1));
        return sorted[idx];
    };

    p.p50 = percentile(50);
    p.p95 = percentile(95);
    p.p99 = percentile(99);
    p.max = sorted.back();
    p.mean = std::accumulate(sorted.begin(), sorted.end(), 0.0) / sorted.size();
    return p;
}

double Stats::enqueue_rate() const {
    double elapsed = elapsed_seconds();
    if (elapsed <= 0) return 0;
    return static_cast<double>(enqueued_.load()) / elapsed;
}

double Stats::complete_rate() const {
    double elapsed = elapsed_seconds();
    if (elapsed <= 0) return 0;
    return static_cast<double>(completed_.load()) / elapsed;
}

void Stats::start() {
    start_time_ = std::chrono::steady_clock::now();
    running_ = true;
}

void Stats::stop() {
    end_time_ = std::chrono::steady_clock::now();
    running_ = false;
}

double Stats::elapsed_seconds() const {
    auto end = running_ ? std::chrono::steady_clock::now() : end_time_;
    return std::chrono::duration<double>(end - start_time_).count();
}

std::string Stats::to_json(const std::string& host_info, int worker_count,
                           int payload_bytes, double failure_rate) const {
    auto pcts = compute_latency_percentiles();

    // Get current time as ISO string
    auto now = std::chrono::system_clock::now();
    auto t = std::chrono::system_clock::to_time_t(now);
    std::ostringstream oss;
    oss << std::put_time(std::gmtime(&t), "%Y-%m-%dT%H:%M:%SZ");

    nlohmann::json j = {
        {"timestamp", oss.str()},
        {"duration_seconds", elapsed_seconds()},
        {"host", host_info},
        {"configuration", {
            {"worker_count", worker_count},
            {"payload_bytes", payload_bytes},
            {"failure_rate", failure_rate},
            {"redis_persistence", "appendonly"},
            {"otel_exporters", "off"},
        }},
        {"results", {
            {"enqueued", enqueued_.load()},
            {"completed", completed_.load()},
            {"failed", failed_.load()},
            {"enqueue_rate_per_sec", enqueue_rate()},
            {"complete_rate_per_sec", complete_rate()},
            {"events_per_day", static_cast<uint64_t>(complete_rate() * 86400)},
            {"reconciliation", {
                {"enqueued", enqueued_.load()},
                {"completed_plus_failed", completed_.load() + failed_.load()},
                {"difference", static_cast<int64_t>(enqueued_.load()) -
                               static_cast<int64_t>(completed_.load() + failed_.load())},
            }},
        }},
        {"latency_ms", {
            {"p50", pcts.p50},
            {"p95", pcts.p95},
            {"p99", pcts.p99},
            {"max", pcts.max},
            {"mean", pcts.mean},
        }},
    };

    return j.dump(2);
}

} // namespace dtq
