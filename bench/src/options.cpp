#include "options.hpp"
#include <iostream>
#include <cstring>
#include <cstdlib>

namespace dtq {

void print_usage() {
    std::cerr << R"(
Usage: loadgen [OPTIONS]

Options:
  --rate N            Target events/sec (0 = unlimited)        [default: 5000]
  --duration N        Run duration in seconds                  [default: 1800]
  --producers N       Number of producer threads               [default: 2]
  --consumers N       Number of consumer threads               [default: 2]
  --payload-bytes N   Payload size per task                    [default: 256]
  --queues STR        Comma-separated queue names              [default: bench-default]
  --redis-host STR    Redis hostname                           [default: localhost]
  --redis-port N      Redis port                               [default: 7202]
  --pipeline N        hiredis pipeline batch size              [default: 100]
  --failure-rate F    Simulated failure rate (0.0-1.0)         [default: 0.0]
  --output STR        Output JSON file path                    [default: bench/results/run.json]
  --parquet           Enable Parquet output alongside JSON
  --help              Show this help message

Example:
  ./loadgen --rate 5000 --duration 1800 --producers 4 --consumers 4
)" << std::endl;
}

Options parse_options(int argc, char* argv[]) {
    Options opts;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "--help" || arg == "-h") {
            print_usage();
            std::exit(0);
        } else if (arg == "--rate" && i + 1 < argc) {
            opts.rate = std::stoull(argv[++i]);
        } else if (arg == "--duration" && i + 1 < argc) {
            opts.duration_s = std::stoull(argv[++i]);
        } else if (arg == "--producers" && i + 1 < argc) {
            opts.producers = std::stoi(argv[++i]);
        } else if (arg == "--consumers" && i + 1 < argc) {
            opts.consumers = std::stoi(argv[++i]);
        } else if (arg == "--payload-bytes" && i + 1 < argc) {
            opts.payload_bytes = std::stoi(argv[++i]);
        } else if (arg == "--queues" && i + 1 < argc) {
            opts.queues = argv[++i];
        } else if (arg == "--redis-host" && i + 1 < argc) {
            opts.redis_host = argv[++i];
        } else if (arg == "--redis-port" && i + 1 < argc) {
            opts.redis_port = std::stoi(argv[++i]);
        } else if (arg == "--pipeline" && i + 1 < argc) {
            opts.pipeline_size = std::stoi(argv[++i]);
        } else if (arg == "--failure-rate" && i + 1 < argc) {
            opts.failure_rate = std::stod(argv[++i]);
        } else if (arg == "--output" && i + 1 < argc) {
            opts.output_file = argv[++i];
        } else if (arg == "--parquet") {
            opts.parquet_output = true;
        } else {
            std::cerr << "Unknown option: " << arg << std::endl;
            print_usage();
            std::exit(1);
        }
    }

    return opts;
}

} // namespace dtq
