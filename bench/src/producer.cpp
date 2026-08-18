#include "producer.hpp"
#include <hiredis/hiredis.h>
#include <chrono>
#include <thread>
#include <iostream>
#include <sstream>
#include <cstring>
#include <nlohmann/json.hpp>

namespace dtq {

Producer::Producer(const Options& opts, Stats& stats, int thread_id)
    : opts_(opts), stats_(stats), thread_id_(thread_id), ctx_(nullptr) {
    // Parse first queue from comma-separated list
    auto pos = opts_.queues.find(',');
    std::string queue = (pos != std::string::npos)
                            ? opts_.queues.substr(0, pos)
                            : opts_.queues;
    queue_stream_ = "q:" + queue;
}

Producer::~Producer() {
    if (ctx_) {
        redisFree(static_cast<redisContext*>(ctx_));
    }
}

void Producer::connect() {
    auto* c = redisConnect(opts_.redis_host.c_str(), opts_.redis_port);
    if (c == nullptr || c->err) {
        std::cerr << "Producer " << thread_id_ << " Redis connect error: "
                  << (c ? c->errstr : "null context") << std::endl;
        if (c) redisFree(c);
        ctx_ = nullptr;
        return;
    }
    ctx_ = c;
}

std::string Producer::make_envelope(uint64_t seq) {
    // Generate a random-looking task ID
    char task_id[64];
    std::snprintf(task_id, sizeof(task_id),
                  "%08x-%04x-%04x-%04x-%012llx",
                  static_cast<unsigned>(seq * 2654435761u),
                  static_cast<unsigned>(seq) & 0xFFFF,
                  0x4000 | (static_cast<unsigned>(seq >> 16) & 0x0FFF),
                  0x8000 | (static_cast<unsigned>(seq >> 28) & 0x3FFF),
                  static_cast<unsigned long long>(seq));

    // Build payload of requested size
    std::string padding(std::max(0, opts_.payload_bytes - 50), 'x');

    nlohmann::json envelope = {
        {"task_id", task_id},
        {"queue", opts_.queues},
        {"task_name", "bench_task"},
        {"payload", {{"seq", seq}, {"thread", thread_id_}, {"data", padding}}},
        {"attempt", 0},
        {"max_attempts", 3},
        {"priority", 0},
        {"created_at", std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch()).count()},
    };

    return envelope.dump();
}

void Producer::run(std::atomic<bool>& running) {
    connect();
    if (!ctx_) return;

    auto* c = static_cast<redisContext*>(ctx_);

    // Calculate per-thread rate limit
    uint64_t per_thread_rate = opts_.rate > 0
        ? opts_.rate / static_cast<uint64_t>(opts_.producers)
        : 0;

    auto interval = per_thread_rate > 0
        ? std::chrono::nanoseconds(1'000'000'000 / per_thread_rate)
        : std::chrono::nanoseconds(0);

    auto next_send = std::chrono::steady_clock::now();
    int pipeline_count = 0;

    while (running.load(std::memory_order_relaxed)) {
        // Rate limiting
        if (per_thread_rate > 0) {
            auto now = std::chrono::steady_clock::now();
            if (now < next_send) {
                std::this_thread::sleep_until(next_send);
            }
            next_send += interval;
        }

        // Build and send via pipeline
        std::string data = make_envelope(seq_++);
        // MAXLEN ~ bounds stream memory. Without it the stream grows until Redis
        // hits maxmemory and (under noeviction) silently rejects every XADD,
        // which stalls consumers and produces phantom enqueue counts.
        redisAppendCommand(c, "XADD %s MAXLEN ~ 200000 * data %s",
                          queue_stream_.c_str(), data.c_str());
        pipeline_count++;

        if (pipeline_count >= opts_.pipeline_size) {
            // Flush pipeline
            for (int i = 0; i < pipeline_count; ++i) {
                redisReply* reply = nullptr;
                if (redisGetReply(c, reinterpret_cast<void**>(&reply)) == REDIS_OK) {
                    if (reply) {
                        if (reply->type == REDIS_REPLY_ERROR) {
                            stats_.record_failure();
                        } else {
                            stats_.record_enqueue();
                        }
                        freeReplyObject(reply);
                    }
                } else {
                    stats_.record_failure();
                    // Reconnect on error
                    redisFree(c);
                    connect();
                    c = static_cast<redisContext*>(ctx_);
                    if (!c) return;
                    break;
                }
            }
            pipeline_count = 0;
        }
    }

    // Flush remaining
    for (int i = 0; i < pipeline_count; ++i) {
        redisReply* reply = nullptr;
        if (redisGetReply(c, reinterpret_cast<void**>(&reply)) == REDIS_OK && reply) {
            stats_.record_enqueue();
            freeReplyObject(reply);
        }
    }
}

} // namespace dtq
