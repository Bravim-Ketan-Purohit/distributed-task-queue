#include "consumer.hpp"
#include <hiredis/hiredis.h>
#include <chrono>
#include <thread>
#include <iostream>
#include <cstring>
#include <nlohmann/json.hpp>

namespace dtq {

Consumer::Consumer(const Options& opts, Stats& stats, int thread_id)
    : opts_(opts), stats_(stats), thread_id_(thread_id), ctx_(nullptr) {
    auto pos = opts_.queues.find(',');
    std::string queue = (pos != std::string::npos)
                            ? opts_.queues.substr(0, pos)
                            : opts_.queues;
    queue_stream_ = "q:" + queue;
    group_ = queue; // consumer group named after queue
    consumer_name_ = "loadgen-consumer-" + std::to_string(thread_id);
}

Consumer::~Consumer() {
    if (ctx_) {
        redisFree(static_cast<redisContext*>(ctx_));
    }
}

void Consumer::connect() {
    auto* c = redisConnect(opts_.redis_host.c_str(), opts_.redis_port);
    if (c == nullptr || c->err) {
        std::cerr << "Consumer " << thread_id_ << " Redis connect error: "
                  << (c ? c->errstr : "null context") << std::endl;
        if (c) redisFree(c);
        ctx_ = nullptr;
        return;
    }
    ctx_ = c;
}

void Consumer::ensure_group() {
    if (!ctx_) return;
    auto* c = static_cast<redisContext*>(ctx_);

    // Create consumer group (ignore BUSYGROUP error if already exists)
    auto* reply = static_cast<redisReply*>(
        redisCommand(c, "XGROUP CREATE %s %s 0 MKSTREAM",
                     queue_stream_.c_str(), group_.c_str()));
    if (reply) {
        freeReplyObject(reply);
    }
}

void Consumer::run(std::atomic<bool>& running) {
    connect();
    if (!ctx_) return;
    ensure_group();

    auto* c = static_cast<redisContext*>(ctx_);

    while (running.load(std::memory_order_relaxed)) {
        // XREADGROUP GROUP <group> <consumer> COUNT 10 BLOCK 1000 STREAMS <stream> >
        auto* reply = static_cast<redisReply*>(
            redisCommand(c,
                "XREADGROUP GROUP %s %s COUNT %d BLOCK 1000 STREAMS %s >",
                group_.c_str(), consumer_name_.c_str(),
                opts_.pipeline_size, queue_stream_.c_str()));

        if (!reply) {
            // Connection error, reconnect
            redisFree(c);
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            connect();
            c = static_cast<redisContext*>(ctx_);
            if (!c) return;
            ensure_group();
            continue;
        }

        if (reply->type == REDIS_REPLY_NIL || reply->type == REDIS_REPLY_ERROR) {
            freeReplyObject(reply);
            continue;
        }

        if (reply->type != REDIS_REPLY_ARRAY || reply->elements == 0) {
            freeReplyObject(reply);
            continue;
        }

        // Process messages and ACK in pipeline
        int msg_count = 0;
        for (size_t s = 0; s < reply->elements; ++s) {
            auto* stream_reply = reply->element[s];
            if (stream_reply->type != REDIS_REPLY_ARRAY || stream_reply->elements < 2)
                continue;

            auto* messages = stream_reply->element[1];
            for (size_t m = 0; m < messages->elements; ++m) {
                auto* msg = messages->element[m];
                if (msg->type != REDIS_REPLY_ARRAY || msg->elements < 2)
                    continue;

                // msg->element[0] is the message ID
                const char* msg_id = msg->element[0]->str;

                // Parse envelope to compute latency
                auto* fields = msg->element[1];
                double latency_ms = 0;
                for (size_t f = 0; f + 1 < fields->elements; f += 2) {
                    if (std::strcmp(fields->element[f]->str, "data") == 0) {
                        try {
                            auto j = nlohmann::json::parse(fields->element[f + 1]->str);
                            if (j.contains("created_at")) {
                                auto created = j["created_at"].get<int64_t>();
                                auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::system_clock::now().time_since_epoch()).count();
                                latency_ms = static_cast<double>(now_ms - created);
                            }
                        } catch (...) {}
                    }
                }

                // Simulate task execution (tiny delay for realism)
                // In real benchmarks, Python workers handle execution

                // ACK the message
                redisAppendCommand(c, "XACK %s %s %s",
                                  queue_stream_.c_str(), group_.c_str(), msg_id);
                msg_count++;
                stats_.record_complete(latency_ms);
            }
        }
        freeReplyObject(reply);

        // Flush ACKs
        for (int i = 0; i < msg_count; ++i) {
            redisReply* ack_reply = nullptr;
            if (redisGetReply(c, reinterpret_cast<void**>(&ack_reply)) == REDIS_OK && ack_reply) {
                freeReplyObject(ack_reply);
            }
        }
    }
}

} // namespace dtq
