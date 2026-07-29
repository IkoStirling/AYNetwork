#pragma once
// AckPipeline.h - R4.1-B explicit CHANNEL_ACK pipeline
//
// RequiresAck frames carry a 4-byte seq prefix in the body. Receivers
// reply with kMsgTypeAppAck on CHANNEL_ACK. GNS transport remains
// responsible for wire reliability; this layer gives callers an optional
// application-level delivery confirmation hook.

#include <IAYNetwork.h>
#include <PacketCodec.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <unordered_map>

namespace ayt::net
{

constexpr size_t kAckSeqPrefixBytes = 4;

class AckPipeline {
public:
    static std::vector<uint8_t> sealAckable(const uint8_t* body, size_t bodyLen,
                                            uint16_t msgType, uint8_t channel,
                                            uint32_t seq, uint32_t timestampMs,
                                            bool compress = false);

    static std::vector<uint8_t> sealAck(uint32_t seq, uint32_t timestampMs);

    static bool parseAckBody(const uint8_t* body, size_t bodyLen, uint32_t& seqOut);

    // Strips the seq prefix from `body`; returns false on truncation.
    static bool unwrapAckableBody(std::vector<uint8_t>& body, uint32_t& seqOut);
};

class AckTracker {
public:
    using Callback = std::function<void(bool confirmed)>;

    uint32_t allocateSeq() {
        return _nextSeq.fetch_add(1, std::memory_order_relaxed);
    }

    void registerPending(uint32_t seq, Callback cb, uint32_t timeoutMs = 5000);
    void onAck(uint32_t seq);
    void expire();
    size_t pendingCount() const;

private:
    struct Entry {
        Callback cb;
        std::chrono::steady_clock::time_point deadline;
    };

    mutable std::mutex _mutex;
    std::atomic<uint32_t> _nextSeq{1};
    std::unordered_map<uint32_t, Entry> _pending;
};

} // namespace ayt::net
