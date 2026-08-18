// PacketCodec.cpp - Pure encode/decode of a PacketHeader v2 frame.
//
// R2 (2026-07-27): CRC32C + lz4 (decode side reuses AYStorage::Lz4Decompressor;
// encode side is a thin wrapper over <lz4.h> raw block API).

#include <AYNetwork/Protocol/PacketCodec.h>

#include <AYStorage/Lz4Decompressor.h>   // R2: decode-side reuse from AYStorage

#include <lz4.h>                         // R2: encode side, raw block API

#include <algorithm>
#include <cstring>
#include <cstdio>

namespace ayt::net
{

namespace
{

// =============================================================================
// CRC32C (Castagnoli) — inlined stdlib-only implementation.
//
// R2 design decision: the andytech-tiny-crc32c vcpkg port is available in
// the registry but not installed on this machine. Inlining a small,
// header-free table-driven CRC32C avoids dragging a new vcpkg dependency
// into the project for a single use site. ~40 lines, MIT/BSD-compatible
// (algorithm is public). Castagnoli polynomial 0x1EDC6F41 (reversed:
// 0x82F63B78) matches GNS / Intel CRC32C instruction.
//
// Reference vector: CRC32C("123456789") = 0xE3069283.
// =============================================================================
struct Crc32cTable {
    uint32_t v[256];
    Crc32cTable() {
        constexpr uint32_t kPoly = 0x82F63B78u;  // reversed Castagnoli
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int j = 0; j < 8; ++j) {
                c = (c >> 1) ^ ((c & 1) ? kPoly : 0);
            }
            v[i] = c;
        }
    }
};

const Crc32cTable& crcTable() {
    static const Crc32cTable t;
    return t;
}

inline uint32_t crc32cUpdate(uint32_t crc, const uint8_t* data, size_t len) {
    const auto& t = crcTable();
    for (size_t i = 0; i < len; ++i) {
        crc = t.v[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc;
}

// Little-endian helpers (host is x64 little-endian; written defensively
// in case of future big-endian port).
inline void writeU16LE(uint8_t* dst, uint16_t v) {
    dst[0] = static_cast<uint8_t>(v & 0xFF);
    dst[1] = static_cast<uint8_t>((v >> 8) & 0xFF);
}
inline void writeU32LE(uint8_t* dst, uint32_t v) {
    dst[0] = static_cast<uint8_t>(v & 0xFF);
    dst[1] = static_cast<uint8_t>((v >> 8)  & 0xFF);
    dst[2] = static_cast<uint8_t>((v >> 16) & 0xFF);
    dst[3] = static_cast<uint8_t>((v >> 24) & 0xFF);
}
inline uint16_t readU16LE(const uint8_t* src) {
    return static_cast<uint16_t>(
        static_cast<uint16_t>(src[0]) |
        (static_cast<uint16_t>(src[1]) << 8));
}
inline uint32_t readU32LE(const uint8_t* src) {
    return  static_cast<uint32_t>(src[0])
        | (static_cast<uint32_t>(src[1]) << 8)
        | (static_cast<uint32_t>(src[2]) << 16)
        | (static_cast<uint32_t>(src[3]) << 24);
}

} // anonymous namespace

// =============================================================================
// Public CRC32C entry point — matches the API test vectors expect.
// =============================================================================
uint32_t PacketCodec::computeCrc32c(const uint8_t* data, size_t len) {
    uint32_t crc = kCrcInit;
    crc = crc32cUpdate(crc, data, len);
    return crc ^ 0xFFFFFFFFu;
}

// =============================================================================
// Encode
// =============================================================================
std::vector<uint8_t> PacketCodec::encode(
    const uint8_t* body, size_t bodyLen,
    uint16_t msgType, uint16_t schemaVersion,
    uint8_t  channel, uint8_t  flags, uint32_t timestampMs,
    bool compress)
{
    if ((body == nullptr && bodyLen != 0) ||
        bodyLen > PacketCodec::kMaxDecodedBodySize) {
        return {};
    }

    // R2 pitfall: Compressed + Fragmented never combine. Caller (Assembler)
    // compresses the entire payload first, then fragments the compressed
    // bytes if needed. We honor whatever flag the caller passed; if both
    // bits are set the encoder still works (length covers uncSize + lz4
    // block) but the decoder will surface flag confusion.
    uint8_t outFlags = flags;
    if (compress) {
        outFlags = static_cast<uint8_t>(outFlags | static_cast<uint8_t>(PacketFlag::Compressed));
    }

    // Compress the body (or pass through).
    std::vector<uint8_t> compressed;
    uint32_t uncSize = 0;
    const uint8_t* payload = nullptr;
    size_t         payloadLen = 0;
    if (compress) {
        if (body == nullptr || bodyLen == 0) {
            // Empty body, compression is a no-op — fall through to plain.
            payload = body;
            payloadLen = bodyLen;
            outFlags = static_cast<uint8_t>(outFlags & ~static_cast<uint8_t>(PacketFlag::Compressed));
        } else {
            int maxCompressed = LZ4_compressBound(static_cast<int>(bodyLen));
            compressed.resize(static_cast<size_t>(maxCompressed));
            int n = LZ4_compress_default(
                reinterpret_cast<const char*>(body),
                reinterpret_cast<char*>(compressed.data()),
                static_cast<int>(bodyLen),
                maxCompressed);
            if (n <= 0) {
                // Compression failed — caller doesn't see this as fatal; we
                // fall back to sending the original bytes (matches AYStorage
                // write-failure semantics: don't drop the packet).
                ::fprintf(stderr, "[PacketCodec] LZ4_compress_default failed (%d), sending plain\n", n);
                payload = body;
                payloadLen = bodyLen;
                outFlags = static_cast<uint8_t>(outFlags & ~static_cast<uint8_t>(PacketFlag::Compressed));
            } else {
                compressed.resize(static_cast<size_t>(n));
                uncSize = static_cast<uint32_t>(bodyLen);
                payload = compressed.data();
                payloadLen = compressed.size();
            }
        }
    } else {
        payload = body;
        payloadLen = bodyLen;
    }

    // Total body length (between PacketHeader and trailing CRC):
    //   plain    : payloadLen
    //   compressed : kUncompressedSizePrefix (4) + payloadLen
    const size_t framedPayloadLen =
        (outFlags & static_cast<uint8_t>(PacketFlag::Compressed))
            ? PacketCodec::kUncompressedSizePrefix + payloadLen
            : payloadLen;
    if (framedPayloadLen > PacketCodec::kMaxWireBodySize) {
        return {};
    }

    uint16_t length;
    if (outFlags & static_cast<uint8_t>(PacketFlag::Compressed)) {
        length = static_cast<uint16_t>(PacketCodec::kUncompressedSizePrefix + payloadLen);
    } else {
        length = static_cast<uint16_t>(payloadLen);
    }

    // Allocate [header 12B] [body length bytes] [crc 4B]
    std::vector<uint8_t> wire;
    wire.resize(PacketCodec::kHeaderSize + length + PacketCodec::kCrcSize);

    // Fill header (little-endian).
    uint8_t* hp = wire.data();
    writeU16LE(hp + 0, msgType);
    writeU16LE(hp + 2, schemaVersion);
    writeU16LE(hp + 4, length);
    hp[6] = channel;
    hp[7] = outFlags;
    writeU32LE(hp + 8, timestampMs);

    // Fill body.
    uint8_t* bp = wire.data() + PacketCodec::kHeaderSize;
    if (outFlags & static_cast<uint8_t>(PacketFlag::Compressed)) {
        writeU32LE(bp, uncSize);
        if (payloadLen > 0) {
            std::memcpy(bp + PacketCodec::kUncompressedSizePrefix, payload, payloadLen);
        }
    } else {
        if (payloadLen > 0) {
            std::memcpy(bp, payload, payloadLen);
        }
    }

    // CRC over [header][body] — NOT over the trailing 4B itself.
    uint32_t crc = PacketCodec::computeCrc32c(
        wire.data(), wire.size() - PacketCodec::kCrcSize);
    uint8_t* cp = wire.data() + wire.size() - PacketCodec::kCrcSize;
    writeU32LE(cp, crc);

    return wire;
}

// =============================================================================
// Decode
// =============================================================================
DecodedPacket PacketCodec::decode(const uint8_t* wire, size_t wireLen) {
    DecodedPacket out;
    if (wire == nullptr || wireLen < PacketCodec::kHeaderSize + PacketCodec::kCrcSize) {
        return out; // ok=false
    }

    // Parse header.
    const uint8_t* hp = wire;
    out.header.msgType       = readU16LE(hp + 0);
    out.header.schemaVersion = readU16LE(hp + 2);
    out.header.length        = readU16LE(hp + 4);
    out.header.channel       = hp[6];
    out.header.flags         = hp[7];
    out.header.timestampMs   = readU32LE(hp + 8);

    constexpr uint8_t kKnownFlags =
        static_cast<uint8_t>(PacketFlag::Fragmented) |
        static_cast<uint8_t>(PacketFlag::Compressed) |
        static_cast<uint8_t>(PacketFlag::RequiresAck);
    if (out.header.channel > 3 || (out.header.flags & ~kKnownFlags) != 0) {
        return out;
    }
    if (hasFlag(out.header.flags, PacketFlag::Compressed) &&
        hasFlag(out.header.flags, PacketFlag::Fragmented)) {
        return out;
    }

    // Length sanity: must equal (wireLen - header - crc).
    size_t expected = static_cast<size_t>(out.header.length)
                    + PacketCodec::kHeaderSize + PacketCodec::kCrcSize;
    if (expected != wireLen) {
        ::fprintf(stderr, "[PacketCodec] decode length mismatch: header.length=%u, wireLen=%zu\n",
                  out.header.length, wireLen);
        return out; // ok=false
    }

    // CRC check: CRC over [header][body], compared against trailing u32.
    uint32_t expectedCrc = readU32LE(wire + wireLen - PacketCodec::kCrcSize);
    uint32_t actualCrc = PacketCodec::computeCrc32c(wire, wireLen - PacketCodec::kCrcSize);
    if (actualCrc != expectedCrc) {
        ::fprintf(stderr, "[PacketCodec] CRC mismatch: got %08x, expected %08x\n",
                  actualCrc, expectedCrc);
        return out; // ok=false
    }

    // Slice the body.
    const uint8_t* bodyStart = wire + PacketCodec::kHeaderSize;
    size_t bodyLen = out.header.length;

    if (out.header.flags & static_cast<uint8_t>(PacketFlag::Compressed)) {
        // Compressed body layout: [u32 uncSize][lz4 block]. AYStorage's
        // Lz4Decompressor needs both the compressed slice and the original
        // size (it's non-streaming).
        if (bodyLen < PacketCodec::kUncompressedSizePrefix) {
            return out; // malformed
        }
        uint32_t uncSize = readU32LE(bodyStart);
        const uint8_t* compStart = bodyStart + PacketCodec::kUncompressedSizePrefix;
        size_t compLen = bodyLen - PacketCodec::kUncompressedSizePrefix;
        if (uncSize == 0 || uncSize > PacketCodec::kMaxDecodedBodySize ||
            compLen == 0 ||
            static_cast<uint64_t>(uncSize) >
                static_cast<uint64_t>(compLen) * PacketCodec::kMaxCompressionRatio) {
            return out;
        }

        // Reuse AYStorage's Lz4Decompressor. It decompresses everything at
        // once into its internal buffer; we then read into out.body.
        ayt::storage::Lz4Decompressor decomp;
        if (!decomp.setSource(compStart, compLen, uncSize)) {
            ::fprintf(stderr, "[PacketCodec] Lz4Decompressor::setSource failed (uncSize=%u compLen=%zu)\n",
                      uncSize, compLen);
            return out;
        }
        out.body.resize(uncSize);
        size_t got = decomp.read(out.body.data(), uncSize);
        if (got != uncSize) {
            ::fprintf(stderr, "[PacketCodec] Lz4Decompressor::read returned %zu, expected %u\n",
                      got, uncSize);
            return out;
        }
    } else {
        out.body.assign(bodyStart, bodyStart + bodyLen);
    }

    out.ok = true;
    return out;
}

} // namespace ayt::net
