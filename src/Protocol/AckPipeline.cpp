#include <AYNetwork/Protocol/AckPipeline.h>

#include <AYNetwork/Protocol/PacketHeader.h>

#include <cstring>

namespace ayt::net
{

namespace {

void writeU32LE(uint8_t* dst, uint32_t v) {
    dst[0] = static_cast<uint8_t>(v);
    dst[1] = static_cast<uint8_t>(v >> 8);
    dst[2] = static_cast<uint8_t>(v >> 16);
    dst[3] = static_cast<uint8_t>(v >> 24);
}

bool readU32LE(const uint8_t* src, size_t len, uint32_t& out) {
    if (len < 4) return false;
    out = static_cast<uint32_t>(src[0])
        | (static_cast<uint32_t>(src[1]) << 8)
        | (static_cast<uint32_t>(src[2]) << 16)
        | (static_cast<uint32_t>(src[3]) << 24);
    return true;
}

} // anonymous namespace

std::vector<uint8_t> AckPipeline::sealAckable(const uint8_t* body, size_t bodyLen,
                                                uint16_t msgType, uint8_t channel,
                                                uint32_t seq, uint32_t timestampMs,
                                                bool compress) {
    std::vector<uint8_t> wrapped(kAckSeqPrefixBytes + bodyLen);
    writeU32LE(wrapped.data(), seq);
    if (bodyLen > 0 && body != nullptr) {
        std::memcpy(wrapped.data() + kAckSeqPrefixBytes, body, bodyLen);
    }
    const uint8_t flags = static_cast<uint8_t>(PacketFlag::RequiresAck);
    return PacketCodec::encode(
        wrapped.data(), wrapped.size(),
        msgType, kSchemaVersion,
        channel, flags, timestampMs, compress);
}

std::vector<uint8_t> AckPipeline::sealAck(uint32_t seq, uint32_t timestampMs) {
    uint8_t body[kAckSeqPrefixBytes];
    writeU32LE(body, seq);
    return PacketCodec::encode(
        body, sizeof(body),
        kMsgTypeAppAck, kSchemaVersion,
        CHANNEL_ACK, /*flags=*/ 0, timestampMs, /*compress=*/ false);
}

bool AckPipeline::parseAckBody(const uint8_t* body, size_t bodyLen, uint32_t& seqOut) {
    seqOut = 0;
    return readU32LE(body, bodyLen, seqOut);
}

bool AckPipeline::unwrapAckableBody(std::vector<uint8_t>& body, uint32_t& seqOut) {
    seqOut = 0;
    if (body.size() < kAckSeqPrefixBytes) return false;
    if (!readU32LE(body.data(), body.size(), seqOut)) return false;
    if (body.size() == kAckSeqPrefixBytes) {
        body.clear();
        return true;
    }
    std::vector<uint8_t> stripped(body.begin() + kAckSeqPrefixBytes, body.end());
    body = std::move(stripped);
    return true;
}

void AckTracker::registerPending(uint32_t seq, Callback cb, uint32_t timeoutMs) {
    if (!cb) return;
    Entry entry;
    entry.cb = std::move(cb);
    entry.deadlineUs = ayt::performanceNowUs()
        + static_cast<uint64_t>(timeoutMs) * 1000u;
    std::lock_guard<std::mutex> lk(_mutex);
    _pending[seq] = std::move(entry);
}

void AckTracker::onAck(uint32_t seq) {
    Callback cb;
    {
        std::lock_guard<std::mutex> lk(_mutex);
        auto it = _pending.find(seq);
        if (it == _pending.end()) return;
        cb = std::move(it->second.cb);
        _pending.erase(it);
    }
    if (cb) cb(true);
}

void AckTracker::expire() {
    const uint64_t nowUs = ayt::performanceNowUs();
    std::vector<Callback> expired;
    {
        std::lock_guard<std::mutex> lk(_mutex);
        for (auto it = _pending.begin(); it != _pending.end();) {
            if (nowUs >= it->second.deadlineUs) {
                expired.push_back(std::move(it->second.cb));
                it = _pending.erase(it);
            } else {
                ++it;
            }
        }
    }
    for (Callback& cb : expired) {
        if (cb) cb(false);
    }
}

size_t AckTracker::pendingCount() const {
    std::lock_guard<std::mutex> lk(_mutex);
    return _pending.size();
}

} // namespace ayt::net
