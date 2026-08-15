#pragma once

#include "options.hpp"
#include "stats.hpp"
#include <atomic>
#include <string>

namespace dtq {

class Producer {
public:
    Producer(const Options& opts, Stats& stats, int thread_id);
    ~Producer();

    // Run the producer loop until stopped
    void run(std::atomic<bool>& running);

private:
    // Generate a task envelope JSON payload
    std::string make_envelope(uint64_t seq);

    // Connect to Redis
    void connect();

    const Options& opts_;
    Stats& stats_;
    int thread_id_;
    void* ctx_; // hiredis context (redisContext*)
    std::string queue_stream_; // e.g., "q:bench-default"
    uint64_t seq_{0};
};

} // namespace dtq
