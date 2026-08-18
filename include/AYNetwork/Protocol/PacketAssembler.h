#pragma once
// AYNetwork/Protocol/AYNetwork/Protocol/AYNetwork/Protocol/PacketAssembler.h - Fragment a payload and reassemble fragments.
//
// R2 (2026-07-27): rewrote the R1 stub. Two halves:
//   - static fragment(payload, mtu, ...) -> vector<vector<uint8_t>>
//     Each entry is a complete sealed frame (already PacketHeader + CRC),
//     ready to hand to GnsConnection::sendRaw. Last fragment is variable
//     length (Ct <= chunkSize for any non-last fragment).
//   - instance consume(fragBody, fragLen) -> optional<vector<uint8_t>>
//     Caller passes the decoded body (after PacketCodec::decode strips the
//     outer PacketHeader). Robust to reorder, duplicates, and loss:
//       * Reorder: keyed by (fragmentId, fragmentIndex)
//       * Duplicate: receivedMask tracks bitset; dup fragments are ignored
//       * Loss: returns payload only when ALL bits are set; the optional
//         stays empty until then.
//     Returns the reassembled payload exactly once (the LAST arriving
//     fragment completes the set).

#include <AYCore.h>
#include <AYNetwork/Protocol/PacketHeader.h>
#include <AYNetwork/Protocol/PacketCodec.h>

#include <cstdint>
#include <cstddef>
#include <optional>
#include <vector>
#include <unordered_map>

namespace ayt::net
{

class PacketAssembler {
public:
    static constexpr uint16_t kDefaultMaxFragments = 4096;
    static constexpr size_t kDefaultMaxReassembledBytes = 4u * 1024u * 1024u;
    static constexpr size_t kDefaultMaxPendingBytes = 8u * 1024u * 1024u;
    // ========================================================================
    // Fragment a payload into N sealed frames. If payloadLen fits in one
    // MTU, returns a single-element vector. Last fragment may be shorter
    // than the others (Ct <= chunkSize for non-last).
    //
    // Args:
    //   payload           - application bytes to ship
    //   payloadLen        - length of payload
    //   mtu               - max wire size per frame (header + body + CRC)
    //   msgType, schema   - stamped into each frame's PacketHeader
    //   channel, tsMs     - same
    //   fragmentId        - per-call 32-bit id; caller generates unique values
    //                       to disambiguate concurrent sends. NOT a transport
    //                       seq (GNS owns that).
    // ========================================================================
    static std::vector<std::vector<uint8_t>> fragment(
        const uint8_t* payload, size_t payloadLen, uint32_t mtu,
        uint16_t msgType, uint16_t schemaVersion,
        uint8_t  channel, uint32_t timestampMs, uint32_t fragmentId);

    // ========================================================================
    // Consume a fragment's BODY (i.e. [FragmentHeader 8B][chunk]) and
    // optionally return the reassembled payload.
    //
    // Caller is responsible for checking that the outer PacketHeader had
    // PacketFlag::Fragmented set BEFORE calling this. Returns nullopt when
    // more fragments are pending; returns a vector when the last unique
    // fragment completes the set.
    //
    // Malformed fragments (bad fragmentIndex/count, wrong frame size) are
    // dropped silently — they will never complete and the in-flight
    // FragmentBuffer will eventually be reaped by clear() or by timeout.
    // ========================================================================
    std::optional<std::vector<uint8_t>> consume(
        const uint8_t* fragBody, size_t fragBodyLen);
    std::optional<std::vector<uint8_t>> consume(
        const uint8_t* fragBody, size_t fragBodyLen, uint32_t monotonicNowMs);

    // Drop all in-flight reassembly state.
    void clear();
    void reapExpired(uint32_t monotonicNowMs);

    // Configuration
    void setMaxInFlight(uint32_t n) { _maxInFlight = n == 0 ? 1 : n; }
    void setTimeoutMs(uint32_t ms) { _timeoutMs = ms == 0 ? 1 : ms; }
    void setMaxFragments(uint16_t n) { _maxFragments = n == 0 ? 1 : n; }
    void setMaxReassembledBytes(size_t n) { _maxReassembledBytes = n == 0 ? 1 : n; }
    void setMaxPendingBytes(size_t n) { _maxPendingBytes = n == 0 ? 1 : n; }

    size_t pendingCount() const { return _pending.size(); }
    size_t pendingBytes() const { return _pendingBytes; }

private:
    struct FragmentBuffer {
        uint16_t                 fragmentCount = 0;
        std::vector<bool>        receivedMask;       // size == fragmentCount
        // Per-index chunk storage; flattened on completion. This avoids the
        // R1 over-alloc bug (which sized chunks as count*chunkSize and
        // ignored last-fragment-is-shorter).
        std::vector<std::vector<uint8_t>> _indexToChunk;
        uint32_t                 firstSeenMs = 0;    // monotonic arrival time
        uint32_t                 receivedCount = 0;  // unique received
        size_t                   totalBytes = 0;
    };

    std::unordered_map<uint32_t, FragmentBuffer> _pending;
    uint32_t _maxInFlight = 32;
    uint32_t _timeoutMs   = 5000;
    uint16_t _maxFragments = kDefaultMaxFragments;
    size_t _maxReassembledBytes = kDefaultMaxReassembledBytes;
    size_t _maxPendingBytes = kDefaultMaxPendingBytes;
    size_t _pendingBytes = 0;
};

} // namespace ayt::net
