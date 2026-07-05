// PacketAssembler.cpp - Packet assembly and fragmentation

#include <PacketAssembler.h>

namespace ayt::net
{

bool PacketAssembler::assemble(uint32_t packetId, const uint8_t* data, size_t size) {
    if (size < sizeof(FragmentHeader)) {
        return false;
    }

    const FragmentHeader* fragHeader = reinterpret_cast<const FragmentHeader*>(data);
    const uint8_t* payload = data + sizeof(FragmentHeader);
    size_t payloadSize = size - sizeof(FragmentHeader);

    auto& buffer = _pendingPackets[packetId];

    if (buffer.receivedCount == 0) {
        buffer.fragmentCount = fragHeader->fragmentCount;
        buffer.data.resize(buffer.fragmentCount * payloadSize);
        buffer.firstTimestamp = 0;
    }

    if (fragHeader->fragmentIndex >= buffer.fragmentCount) {
        return false;
    }

    size_t offset = fragHeader->fragmentIndex * payloadSize;
    std::memcpy(buffer.data.data() + offset, payload, payloadSize);
    buffer.receivedCount++;

    return buffer.receivedCount == buffer.fragmentCount;
}

bool PacketAssembler::isComplete(uint32_t packetId) const {
    auto it = _pendingPackets.find(packetId);
    if (it == _pendingPackets.end()) {
        return false;
    }
    return it->second.receivedCount == it->second.fragmentCount;
}

const uint8_t* PacketAssembler::getPacketData(uint32_t packetId) const {
    auto it = _pendingPackets.find(packetId);
    if (it == _pendingPackets.end()) {
        return nullptr;
    }
    return it->second.data.data();
}

size_t PacketAssembler::getPacketSize(uint32_t packetId) const {
    auto it = _pendingPackets.find(packetId);
    if (it == _pendingPackets.end()) {
        return 0;
    }
    return it->second.data.size();
}

void PacketAssembler::removePacket(uint32_t packetId) {
    _pendingPackets.erase(packetId);
}

void PacketAssembler::clear() {
    _pendingPackets.clear();
}

std::vector<const uint8_t*> PacketAssembler::fragment(const uint8_t* data, size_t size,
                                                     uint32_t mtu, uint32_t& outFragmentCount) {
    std::vector<const uint8_t*> fragments;

    size_t payloadSize = mtu - sizeof(PacketHeader) - sizeof(FragmentHeader);
    uint32_t fragmentCount = (uint32_t)((size + payloadSize - 1) / payloadSize);

    outFragmentCount = fragmentCount;

    return fragments;
}

} // namespace ayt::net