#pragma once

#include <string>
#include <cstdint>

namespace dtq {

struct Options {
    // Target rate (events/sec). 0 = unlimited.
    uint64_t rate = 5000;

    // Run duration in seconds.
    uint64_t duration_s = 1800; // 30 minutes

    // Number of producer threads.
    int producers = 2;

    // Number of consumer threads.
    int consumers = 2;

    // Payload size in bytes.
    int payload_bytes = 256;

    // Queue names (comma-separated).
    std::string queues = "bench-default";

    // Redis host.
    std::string redis_host = "localhost";

    // Redis port.
    int redis_port = 7202;

    // Pipeline batch size for hiredis.
    int pipeline_size = 100;

    // Simulated failure rate (0.0 - 1.0).
    double failure_rate = 0.0;

    // Output file path for results JSON.
    std::string output_file = "bench/results/run.json";

    // Whether to also produce Parquet output.
    bool parquet_output = false;
};

Options parse_options(int argc, char* argv[]);
void print_usage();

} // namespace dtq
