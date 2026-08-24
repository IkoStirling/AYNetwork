#pragma once
// AYNetwork/Prediction/ClientInputCodec.h - R5.2 wire codec for kMsgTypeClientInput.
//
// Wire format (R5.2, 2026-08-24)
// =============================
//   Body bytes (host endian, contiguous):
//     [u32 inputSeq][u32 serverTickAtSend][u8 payload...]
//
//   - inputSeq is the per-connection monotonic sequence.
//   - serverTickAtSend is what the client stamped at send-time (telemetry).
//   - payload is opaque bytes decided by the application; the library
//     does NOT introspect it. Zero-length payloads are allowed (keepalive).
//   - One frame may stack multiple input records back-to-back; the
//     reader returns the first and advances `cur` past it. Callers
//     loop until cur == end.
//
// No msgType byte on the wire — the channel is identified by the
// surrounding PacketHeader (msgType = kMsgTypeClientInput).

#include <cstdint>
#include <cstddef>

namespace ayt::net::ClientInputCodec
{

// Encode one record. Writes 8 bytes of header plus payloadSize bytes of
// payload into `dst`. Returns total bytes written (8 + payloadSize).
// Returns 0 if dst is null or payloadSize > 0 with a null payload.
inline size_t write(uint8_t* dst,
                    uint32_t inputSeq,
                    uint32_t serverTickAtSend,
                    const uint8_t* payload, size_t payloadSize)
{
    if (!dst) return 0;
    if (payloadSize > 0 && !payload) return 0;
    // Header: 2x u32 little-endian (matches AYNetwork host byte order
    // convention used by PacketCodec / ReflectSerializer).
    dst[0] = static_cast<uint8_t>(inputSeq & 0xFFu);
    dst[1] = static_cast<uint8_t>((inputSeq >> 8) & 0xFFu);
    dst[2] = static_cast<uint8_t>((inputSeq >> 16) & 0xFFu);
    dst[3] = static_cast<uint8_t>((inputSeq >> 24) & 0xFFu);
    dst[4] = static_cast<uint8_t>(serverTickAtSend & 0xFFu);
    dst[5] = static_cast<uint8_t>((serverTickAtSend >> 8) & 0xFFu);
    dst[6] = static_cast<uint8_t>((serverTickAtSend >> 16) & 0xFFu);
    dst[7] = static_cast<uint8_t>((serverTickAtSend >> 24) & 0xFFu);
    for (size_t i = 0; i < payloadSize; ++i) {
        dst[8 + i] = payload[i];
    }
    return 8u + payloadSize;
}

// Decode one record. Reads inputSeq / serverTickAtSend / payload bytes
// from `body[0..bodySize)`. payloadOut points into `body` (no copy).
// Returns true on success; false on truncated body.
inline bool read(const uint8_t* body, size_t bodySize,
                 uint32_t& inputSeqOut,
                 uint32_t& serverTickAtSendOut,
                 const uint8_t*& payloadOut, size_t& payloadSizeOut)
{
    inputSeqOut = 0;
    serverTickAtSendOut = 0;
    payloadOut = nullptr;
    payloadSizeOut = 0;
    if (!body || bodySize < 8u) return false;
    inputSeqOut =
        static_cast<uint32_t>(body[0]) |
        (static_cast<uint32_t>(body[1]) << 8) |
        (static_cast<uint32_t>(body[2]) << 16) |
        (static_cast<uint32_t>(body[3]) << 24);
    serverTickAtSendOut =
        static_cast<uint32_t>(body[4]) |
        (static_cast<uint32_t>(body[5]) << 8) |
        (static_cast<uint32_t>(body[6]) << 16) |
        (static_cast<uint32_t>(body[7]) << 24);
    payloadOut = body + 8;
    payloadSizeOut = bodySize - 8u;
    return true;
}

} // namespace ayt::net::ClientInputCodec
