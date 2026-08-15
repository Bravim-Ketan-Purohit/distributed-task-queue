#pragma once

#include "options.hpp"
#include "stats.hpp"
#include <atomic>
#include <string>

namespace dtq {

class Consumer {
public:
    Consumer(const Options& opts, Stats& stats, int thread_id);
    ~Consumer();

    // Run the consumer loop until stopped
    void run(std::atomic<bool>& running);

private:
    void connect();
    void ensure_group();

    const Options& opts_;
    Stats& stats_;
    int thread_id_;
    void* ctx_; // hiredis context
    std::string queue_stream_;
    std::string group_;
    std::string consumer_name_;
};

} // namespace dtq
