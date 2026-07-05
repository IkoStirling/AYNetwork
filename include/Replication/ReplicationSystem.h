#pragma once
// ReplicationSystem.h - ECS system for replication

#include <IAYNetwork.h>

namespace ayt::net
{

// =============================================================================
// ReplicationSystem - ECS system that replicates entities
// =============================================================================
class ReplicationSystem : public ISystem {
public:
    const char* getName() const override { return "Replication"; }

    void onStart() override;
    void onUpdate(float dt) override;

    // Set network subsystem
    void setNetwork(INetworkSubSystem* network);

    // Register entity for replication
    void registerEntity(uint32_t netId, uint32_t entityId);
    void unregisterEntity(uint32_t netId);
    uint32_t findEntity(uint32_t netId) const;

private:
    INetworkSubSystem* _network = nullptr;
    std::unordered_map<uint32_t, uint32_t> _netIdToEntity;
    std::unordered_map<uint32_t, uint32_t> _entityToNetId;
};

} // namespace ayt::net