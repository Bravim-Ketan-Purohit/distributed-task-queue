/**
 * DTQ Load Generator (C++20)
 *
 * Drives the Redis Streams broker at rates Python can't achieve,
 * removing the harness as a confound from throughput measurements.
 *
 * Uses pipelined hiredis for maximum throughput with minimal latency.
 * Reports enqueue rate, completion rate, end-to-end latency p50/p95/p99,
 * and a reconciliation count.
 *
 * Build:
 *   cmake -S bench -B bench/build -DCMAKE_BUILD_TYPE=Release
 *   cmake --build bench/build --parallel 10
 *
 * Run:
 *   ./bench/build/loadgen --rate 5000 --duration 1800 --producers 4 --consumers 4
 */

#include "options.hpp"
#include "producer.hpp"
#include "consumer.hpp"
#include "stats.hpp"

#include <atomic>
#include <chrono>
#include <fstream>
#include <iostream>
#include <thread>
#include <vector>
#include <csignal>
#include <sys/utsname.h>
#include <sstream>

namespace {
    std::atomic<bool> g_running{true};
}

void signal_handler(int) {
    g_running.store(false, std::memory_order_relaxed);
}

std::string get_host_info() {
    struct utsname u{};
    uname(&u);
    std::ostringstream oss;
    oss << u.sysname << " " << u.machine << " " << u.nodename;
    return oss.str();
}

int main(int argc, char* argv[]) {
    auto opts = dtq::parse_options(argc, argv);

    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    std::cout << "=== DTQ Load Generator ===" << std::endl;
    std::cout << "Rate:       " << opts.rate << " events/sec" << std::endl;
    std::cout << "Duration:   " << opts.duration_s << " seconds" << std::endl;
    std::cout << "Producers:  " << opts.producers << std::endl;
    std::cout << "Consumers:  " << opts.consumers << std::endl;
    std::cout << "Payload:    " << opts.payload_bytes << " bytes" << std::endl;
    std::cout << "Pipeline:   " << opts.pipeline_size << std::endl;
    std::cout << "Redis:      " << opts.redis_host << ":" << opts.redis_port << std::endl;
    std::cout << "Output:     " << opts.output_file << std::endl;
    std::cout << "=========================" << std::endl;

    dtq::Stats stats;
    stats.start();

    // Launch producer threads
    std::vector<std::thread> threads;
    std::vector<std::unique_ptr<dtq::Producer>> producers;
    for (int i = 0; i < opts.producers; ++i) {
        producers.push_back(std::make_unique<dtq::Producer>(opts, stats, i));
        threads.emplace_back([&, i]() { producers[i]->run(g_running); });
    }

    // Launch consumer threads
    std::vector<std::unique_ptr<dtq::Consumer>> consumers;
    for (int i = 0; i < opts.consumers; ++i) {
        consumers.push_back(std::make_unique<dtq::Consumer>(opts, stats, i));
        threads.emplace_back([&, i]() { consumers[i]->run(g_running); });
    }

    // Progress reporting thread
    std::thread reporter([&]() {
        while (g_running.load()) {
            std::this_thread::sleep_for(std::chrono::seconds(10));
            if (!g_running.load()) break;

            double elapsed = stats.elapsed_seconds();
            std::cout << "[" << static_cast<int>(elapsed) << "s] "
                      << "enqueued=" << stats.enqueued()
                      << " completed=" << stats.completed()
                      << " rate=" << static_cast<int>(stats.complete_rate()) << "/s"
                      << " failed=" << stats.failed()
                      << std::endl;
        }
    });

    // Run for specified duration
    auto start = std::chrono::steady_clock::now();
    while (g_running.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        auto elapsed = std::chrono::steady_clock::now() - start;
        if (std::chrono::duration_cast<std::chrono::seconds>(elapsed).count()
            >= static_cast<int64_t>(opts.duration_s)) {
            g_running.store(false);
        }
    }

    // Shutdown
    std::cout << "\nShutting down..." << std::endl;
    stats.stop();

    for (auto& t : threads) {
        if (t.joinable()) t.join();
    }
    reporter.join();

    // Final report
    auto pcts = stats.compute_latency_percentiles();
    std::cout << "\n=== Final Results ===" << std::endl;
    std::cout << "Duration:     " << stats.elapsed_seconds() << " seconds" << std::endl;
    std::cout << "Enqueued:     " << stats.enqueued() << std::endl;
    std::cout << "Completed:    " << stats.completed() << std::endl;
    std::cout << "Failed:       " << stats.failed() << std::endl;
    std::cout << "Enqueue rate: " << static_cast<int>(stats.enqueue_rate()) << " /s" << std::endl;
    std::cout << "Complete rate:" << static_cast<int>(stats.complete_rate()) << " /s" << std::endl;
    std::cout << "Events/day:   " << static_cast<uint64_t>(stats.complete_rate() * 86400) << std::endl;
    std::cout << "Latency p50:  " << pcts.p50 << " ms" << std::endl;
    std::cout << "Latency p95:  " << pcts.p95 << " ms" << std::endl;
    std::cout << "Latency p99:  " << pcts.p99 << " ms" << std::endl;
    std::cout << "Latency max:  " << pcts.max << " ms" << std::endl;
    std::cout << "=====================" << std::endl;

    // Write results JSON
    std::string json_output = stats.to_json(
        get_host_info(),
        opts.producers + opts.consumers,
        opts.payload_bytes,
        opts.failure_rate
    );

    std::ofstream out(opts.output_file);
    if (out.is_open()) {
        out << json_output;
        out.close();
        std::cout << "Results written to: " << opts.output_file << std::endl;
    } else {
        std::cerr << "WARNING: Could not write to " << opts.output_file << std::endl;
        std::cout << json_output << std::endl;
    }

    return 0;
}
