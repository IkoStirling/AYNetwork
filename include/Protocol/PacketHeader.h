#pragma once
// PacketHeader.h - Network packet header

#include <AYCore.h>
#include <cstdint>

#if defined(_MSC_VER)
    #pragma pack(push, 1)
#endif

namespace ayt::net
{

// =============================================================================
// PacketHeader - 12 bytes
// =============================================================================
struct PacketHeader {
    uint32_t packetId;
    uint16_t length;
    uint8_t channel;
    uint8_t flags;
    uint32_t timestamp;
    uint32_t checksum;

#if defined(__GNUC__) || defined(__clang__)
    __attribute__((packed))
#endif
};

static_assert(sizeof(PacketHeader) == 12, "PacketHeader must be 12 bytes");

#if defined(_MSC_VER)
    #pragma pack(pop)
#endif

// =============================================================================
// FragmentHeader - for large packets
// =============================================================================
struct FragmentHeader {
    uint32_t fragmentId;
    uint16_t fragmentIndex;
    uint16_t fragmentCount;

#if defined(__GNUC__) || defined(__clang__)
    __attribute__((packed))
#endif
};

static_assert(sizeof(FragmentHeader) == 8, "FragmentHeader must be 8 bytes");

// =============================================================================
// PacketFlags
// =============================================================================
enum class PacketFlag : uint8_t {
    None = 0,
    Fragmented = 1 << 0,
    Compressed = 1 << 1,
    Encrypted = 1 << 2,
    Reliable = 1 << 3,
};

} // namespace ayt::net