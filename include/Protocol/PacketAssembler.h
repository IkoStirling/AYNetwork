#pragma once
// PacketAssembler.h - Packet assembly and fragmentation

#include <AYCore.h>
#include <PacketHeader.h>
#include <vector>
#include <unordered_map>
#include <cstdint>

namespace ayt::net
{

// =============================================================================
// PacketAssembler - Assembles and fragments packets
// =============================================================================
class PacketAssembler {
public:
    // Assemble a complete packet from fragments
    // Returns true if packet is complete
    bool assemble(uint32_t packetId, const uint8_t* data, size_t size);

    // Check if we have all fragments for a packet
    bool isComplete(uint32_t packetId) const;

    // Get assembled packet data
    const uint8_t* getPacketData(uint32_t packetId) const;
    size_t getPacketSize(uint32_t packetId) const;

    // Remove completed packet
    void removePacket(uint32_t packetId);

    // Clear all pending packets
    void clear();

    // Fragment a packet if it's too large
    static std::vector<const uint8_t*> fragment(const uint8_t* data, size_t size,
                                                 uint32_t mtu, uint32_t& outFragmentCount);

    // Configuration
    void setMaxFragments(uint32_t max) { _maxFragments = max; }
    void setFragmentTimeout(uint32_t timeoutMs) { _fragmentTimeoutMs = timeoutMs; }

private:
    struct FragmentBuffer {
        std::vector<uint8_t> data;
        uint16_t fragmentCount = 0;
        uint16_t receivedCount = 0;
        uint32_t firstTimestamp = 0;
    };

    std::unordered_map<uint32_t, FragmentBuffer> _pendingPackets;
    uint32_t _maxFragments = 64;
    uint32_t _fragmentTimeoutMs = 5000;
};

} // namespace ayt::net