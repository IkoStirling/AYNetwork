#pragma once
// AYNetwork/Protocol/AYNetwork/Protocol/AYNetwork/Protocol/AckPipeline.h - R4.1-B explicit CHANNEL_ACK pipeline
//
// RequiresAck frames carry a 4-byte seq prefix in the body. Receivers
// reply with kMsgTypeAppAck on CHANNEL_ACK. GNS transport remains
// responsible for wire reliability; this layer gives callers an optional
// application-level delivery confirmation hook.

#include <AYNetwork/INetwork.h>
#include <AYNetwork/Protocol/PacketCodec.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>

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
        uint64_t deadlineUs = 0;
    };

    // R6 C3 (2026-08-25): std::map (was std::unordered_map) so expire() walks
    // pending acks in ascending seq order. The single-thread model drops
    // the over-defensive _mutex — callers must drive registerPending /
    // onAck / expire from the main network thread only.
    std::atomic<uint32_t> _nextSeq{1};
    std::map<uint32_t, Entry> _pending;
};

} // namespace ayt::net
