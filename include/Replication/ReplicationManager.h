#pragma once
// ReplicationManager.h - Replication manager for network sync

#include <IAYNetwork.h>
#include <unordered_map>
#include <vector>

namespace ayt::net
{

// =============================================================================
// ReplicationManager - Manages object replication
// =============================================================================
class ReplicationManager {
public:
    ReplicationManager(INetworkSubSystem* network);
    ~ReplicationManager();

    // Register/unregister replicable objects
    void registerObject(IReplicable* obj, uint32_t netId);
    void unregisterObject(uint32_t netId);
    IReplicable* findObject(uint32_t netId) const;

    // Replication tick (called every frame on server)
    void replicate();

    // Receive replication data (called on client)
    void onReceive(BitStream& stream);

    // Force sync specific object
    void forceReplicate(uint32_t netId);

    // Extension point
    void setExtension(INetworkExtension* ext);

    // Channel configuration
    void setChannel(uint8_t channel) { _channel = channel; }
    uint8_t getChannel() const { return _channel; }

private:
    // Serialize object based on AYReflect metadata
    template<typename T>
    void serializeObject(T* obj, BitStream& stream);

    INetworkSubSystem* _network = nullptr;
    INetworkExtension* _extension = nullptr;
    std::unordered_map<uint32_t, IReplicable*> _objects;
    std::vector<IReplicable*> _replicationQueue;
    uint8_t _channel = CHANNEL_RELIABLE;
};

} // namespace ayt::net