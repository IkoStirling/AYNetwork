// PacketAssembler.cpp - Fragment / reassemble for application payloads.

#include <AYNetwork/Protocol/PacketAssembler.h>

#include <algorithm>
#include <cstring>

namespace ayt::net
{

// =============================================================================
// fragment() — split payload into N sealed frames (last variable length).
// =============================================================================
std::vector<std::vector<uint8_t>> PacketAssembler::fragment(
    const uint8_t* payload, size_t payloadLen, uint32_t mtu,
    uint16_t msgType, uint16_t schemaVersion,
    uint8_t  channel, uint32_t timestampMs, uint32_t fragmentId)
{
    std::vector<std::vector<uint8_t>> frames;

    if ((payload == nullptr && payloadLen != 0) ||
        payloadLen > kDefaultMaxReassembledBytes) {
        return frames;
    }

    // Per-fragment body = [FragmentHeader 8B][chunk]. Each frame wire =
    // [PacketHeader 12B][body][CRC32C 4B].
    // chunkSize = mtu - 12 - 8 - 4 = mtu - 24.
    if (mtu <= PacketCodec::kHeaderSize +
                PacketCodec::kFragmentHeaderSize +
                PacketCodec::kCrcSize) {
        // MTU too small to carry even one byte of payload + framing.
        // Caller bug; return empty (caller should validate mtu).
        return frames;
    }
    const size_t chunkSize = mtu - PacketCodec::kHeaderSize
                                - PacketCodec::kFragmentHeaderSize
                                - PacketCodec::kCrcSize;
    if (chunkSize == 0) return frames;

    const size_t fragmentCountWide = (payloadLen == 0)
        ? 1u
        : ((payloadLen + chunkSize - 1) / chunkSize);
    if (fragmentCountWide > kDefaultMaxFragments) return frames;
    const uint16_t fragmentCount = static_cast<uint16_t>(fragmentCountWide);

    // Special case: payload fits in one frame, no Fragmented flag.
    if (fragmentCount == 1) {
        // Don't apply Fragmented flag for a single chunk — encode straight
        // through. Caller (GnsConnection::send) usually wants the Fragmented
        // path to be opt-in. To keep the contract simple, the Assembler
        // always sets Fragmented; callers who don't want fragmentation
        // should call PacketCodec::encode directly.
        frames.reserve(1);
        // [FragmentHeader 8B][payload]
        std::vector<uint8_t> body(PacketCodec::kFragmentHeaderSize + payloadLen);
        uint8_t* fh = body.data();
        fh[0] = static_cast<uint8_t>(fragmentId        & 0xFF);
        fh[1] = static_cast<uint8_t>((fragmentId >> 8)  & 0xFF);
        fh[2] = static_cast<uint8_t>((fragmentId >> 16) & 0xFF);
        fh[3] = static_cast<uint8_t>((fragmentId >> 24) & 0xFF);
        fh[4] = 0; fh[5] = 0;                         // fragmentIndex = 0
        fh[6] = static_cast<uint8_t>(fragmentCount & 0xFF);
        fh[7] = static_cast<uint8_t>((fragmentCount >> 8) & 0xFF);
        if (payloadLen > 0) {
            std::memcpy(body.data() + PacketCodec::kFragmentHeaderSize,
                        payload, payloadLen);
        }
        auto frame = PacketCodec::encode(
            body.data(), body.size(),
            msgType, schemaVersion,
            channel,
            static_cast<uint8_t>(PacketFlag::Fragmented),
            timestampMs,
            /*compress=*/false);
        if (frame.empty()) return {};
        frames.push_back(std::move(frame));
        return frames;
    }

    frames.reserve(fragmentCount);
    for (uint16_t i = 0; i < fragmentCount; ++i) {
        const size_t offset = static_cast<size_t>(i) * chunkSize;
        const size_t thisChunkLen =
            (i + 1 == fragmentCount)
                ? (payloadLen - offset)              // last: variable
                : chunkSize;                          // others: full chunk

        // Build body = [FragmentHeader 8B][chunk]
        std::vector<uint8_t> body(PacketCodec::kFragmentHeaderSize + thisChunkLen);
        uint8_t* fh = body.data();
        fh[0] = static_cast<uint8_t>(fragmentId        & 0xFF);
        fh[1] = static_cast<uint8_t>((fragmentId >> 8)  & 0xFF);
        fh[2] = static_cast<uint8_t>((fragmentId >> 16) & 0xFF);
        fh[3] = static_cast<uint8_t>((fragmentId >> 24) & 0xFF);
        fh[4] = static_cast<uint8_t>(i & 0xFF);
        fh[5] = static_cast<uint8_t>((i >> 8) & 0xFF);
        fh[6] = static_cast<uint8_t>(fragmentCount & 0xFF);
        fh[7] = static_cast<uint8_t>((fragmentCount >> 8) & 0xFF);
        if (thisChunkLen > 0) {
            std::memcpy(body.data() + PacketCodec::kFragmentHeaderSize,
                        payload + offset, thisChunkLen);
        }
        auto frame = PacketCodec::encode(
            body.data(), body.size(),
            msgType, schemaVersion,
            channel,
            static_cast<uint8_t>(PacketFlag::Fragmented),
            timestampMs,
            /*compress=*/false);
        if (frame.empty()) return {};
        frames.push_back(std::move(frame));
    }
    return frames;
}

// =============================================================================
// consume() — receive a fragment and optionally yield the reassembled payload
// =============================================================================
std::optional<std::vector<uint8_t>> PacketAssembler::consume(
    const uint8_t* fragBody, size_t fragBodyLen)
{
    const uint32_t nowMs = static_cast<uint32_t>(
        (ayt::performanceNowUs() / 1000u) & 0xFFFFFFFFu);
    return consume(fragBody, fragBodyLen, nowMs);
}

std::optional<std::vector<uint8_t>> PacketAssembler::consume(
    const uint8_t* fragBody, size_t fragBodyLen, uint32_t monotonicNowMs)
{
    reapExpired(monotonicNowMs);

    if (fragBody == nullptr ||
        fragBodyLen < PacketCodec::kFragmentHeaderSize) {
        return std::nullopt;
    }

    const uint8_t* fh = fragBody;
    const uint32_t fragmentId =
          static_cast<uint32_t>(fh[0])
        | (static_cast<uint32_t>(fh[1]) << 8)
        | (static_cast<uint32_t>(fh[2]) << 16)
        | (static_cast<uint32_t>(fh[3]) << 24);
    const uint16_t fragmentIndex =
          static_cast<uint16_t>(fh[4])
        | (static_cast<uint16_t>(fh[5]) << 8);
    const uint16_t fragmentCount =
          static_cast<uint16_t>(fh[6])
        | (static_cast<uint16_t>(fh[7]) << 8);

    if (fragmentCount == 0 || fragmentIndex >= fragmentCount ||
        fragmentCount > _maxFragments) {
        return std::nullopt; // malformed
    }

    const uint8_t* chunk = fragBody + PacketCodec::kFragmentHeaderSize;
    const size_t   chunkLen = fragBodyLen - PacketCodec::kFragmentHeaderSize;

    auto pendingIt = _pending.find(fragmentId);
    if (pendingIt == _pending.end()) {
        if (_pending.size() >= _maxInFlight) return std::nullopt;
        pendingIt = _pending.emplace(fragmentId, FragmentBuffer{}).first;
    }
    auto& buf = pendingIt->second;
    if (buf.fragmentCount == 0) {
        // First time we see this id — initialize.
        buf.fragmentCount = fragmentCount;
        buf.firstSeenMs = monotonicNowMs;
        buf.receivedMask.assign(fragmentCount, false);
        // We don't know maxChunk up front; instead, we'll copy each
        // chunk at a tentative offset based on observed index. The
        // recorded offset = fragmentIndex * maxChunk once we know maxChunk.
        // To handle that, we keep `chunks` as a flat buffer and resize it
        // lazily on the LAST fragment (we need lastChunkLen). For now, we
        // record chunks into a temporary map keyed by index.
        buf._indexToChunk.clear();
        buf._indexToChunk.resize(fragmentCount);
    } else if (buf.fragmentCount != fragmentCount) {
        // Mismatched count for the same id — drop.
        return std::nullopt;
    }

    if (buf.receivedMask[fragmentIndex]) {
        // Duplicate; ignore.
        return std::nullopt;
    }


    if (chunkLen > _maxReassembledBytes - buf.totalBytes ||
        chunkLen > _maxPendingBytes - _pendingBytes) {
        _pendingBytes -= buf.totalBytes;
        _pending.erase(pendingIt);
        return std::nullopt;
    }

    // Record the chunk at its tentative position. We'll flatten on completion.
    buf._indexToChunk[fragmentIndex].assign(chunk, chunk + chunkLen);
    buf.receivedMask[fragmentIndex] = true;
    buf.receivedCount++;
    buf.totalBytes += chunkLen;
    _pendingBytes += chunkLen;

    if (buf.receivedCount != buf.fragmentCount) {
        return std::nullopt;
    }

    // All fragments received — flatten.
    // Compute total = sum of all chunks. The LAST chunk is whatever length
    // it had on the wire (variable), so summing is the only correct way
    // (the old code used `count * chunkSize` which over-allocates when
    // the last fragment is shorter).
    std::vector<uint8_t> out;
    size_t total = 0;
    for (const auto& c : buf._indexToChunk) total += c.size();
    out.resize(total);
    size_t pos = 0;
    for (const auto& c : buf._indexToChunk) {
        if (!c.empty()) std::memcpy(out.data() + pos, c.data(), c.size());
        pos += c.size();
    }

    // Reap.
    _pendingBytes -= buf.totalBytes;
    _pending.erase(fragmentId);
    return out;
}

void PacketAssembler::clear() {
    _pending.clear();
    _pendingBytes = 0;
}

void PacketAssembler::reapExpired(uint32_t monotonicNowMs) {
    for (auto it = _pending.begin(); it != _pending.end();) {
        // Unsigned subtraction intentionally handles the 32-bit millisecond
        // clock wrapping roughly every 49 days.
        if (static_cast<uint32_t>(monotonicNowMs - it->second.firstSeenMs) >=
            _timeoutMs) {
            _pendingBytes -= it->second.totalBytes;
            it = _pending.erase(it);
        } else {
            ++it;
        }
    }
}

} // namespace ayt::net
