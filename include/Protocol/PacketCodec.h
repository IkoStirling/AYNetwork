#pragma once
// PacketCodec.h - Pure encode/decode of a PacketHeader v2 frame.
//
// R2 (2026-07-27): the new framing layer in front of GNS reliable send.
//
// All methods are static and free of I/O — no clocks, no GNS handles, no
// sockets. This is intentional: conformance tests (乱序/丢包/重复/分片)
// drive the codec directly via raw byte vectors without instantiating
// GnsConnection. GnsConnection wraps these calls at the send/recv funnel.

#include <AYCore.h>
#include <PacketHeader.h>

#include <cstdint>
#include <cstddef>
#include <optional>
#include <vector>

namespace ayt::net
{

// Result of PacketCodec::decode. ok=false means the frame should be
// dropped (CRC mismatch, truncated, or implausible length). On ok=true,
// body is the APPLICATION bytes (post-lz4 decompression if the
// Compressed flag was set; raw fragment chunk bytes otherwise — caller
// inspects header.flags to know which).
struct DecodedPacket {
    PacketHeader      header;
    std::vector<uint8_t> body;
    bool              ok = false;
};

class PacketCodec {
public:
    // Sizes exposed so Assembler can size MTU chunks precisely.
    static constexpr size_t kHeaderSize       = sizeof(PacketHeader); // 12
    static constexpr size_t kCrcSize          = 4;
    static constexpr size_t kUncompressedSizePrefix = 4; // u32 before lz4 block
    static constexpr size_t kFragmentHeaderSize = sizeof(FragmentHeader); // 8

    // ========================================================================
    // Encode: returns wire bytes ready to hand to SendMessageToConnection.
    //
    // If `compress` is true, body is lz4-compressed (raw block API) and the
    // resulting frame carries PacketFlag::Compressed plus a 4B uncompressed
    // size prefix so the decoder knows the destination buffer size (matches
    // AYStorage::Lz4Decompressor's non-streaming interface).
    //
    // `length` in the header is filled in automatically.
    // ========================================================================
    static std::vector<uint8_t> encode(
        const uint8_t* body, size_t bodyLen,
        uint16_t msgType, uint16_t schemaVersion,
        uint8_t  channel, uint8_t  flags, uint32_t timestampMs,
        bool compress);

    // ========================================================================
    // Decode: verifies the trailing CRC32C over [header][optional uncSize]
    // [body]. On mismatch returns ok=false. On success body is the
    // application payload post-decompression (or raw fragment bytes if
    // the Fragmented flag is set — the Assembler handles reassembly).
    // ========================================================================
    static DecodedPacket decode(const uint8_t* wire, size_t wireLen);

    // ========================================================================
    // CRC32C (Castagnoli, poly 0x1EDC6F41). Public for tests; inlined
    // implementation lives in PacketCodec.cpp.
    // ========================================================================
    static uint32_t computeCrc32c(const uint8_t* data, size_t len);

    // Initial value used to start a CRC32C computation (Castagnoli uses
    // 0xFFFFFFFF and final XOR 0xFFFFFFFF, like IEEE CRC32 but with a
    // different polynomial).
    static constexpr uint32_t kCrcInit = 0xFFFFFFFFu;
};

} // namespace ayt::net