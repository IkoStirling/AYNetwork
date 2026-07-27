// ReplicationSystem.cpp - ECS system for replication

#include <ReplicationSystem.h>

namespace ayt::net
{

void ReplicationSystem::onStart() {
    // R1 stub. R3 will subscribe to AYEntity's World and prime per-entity
    // snapshot state.
}

void ReplicationSystem::onUpdate(float /*dt*/) {
    // R1 stub. R3 will route per-frame work through AYReflect metadata walks.
}

void ReplicationSystem::setNetwork(INetworkSubSystem* network) {
    _network = network;
}

void ReplicationSystem::registerEntity(uint32_t netId, uint32_t entityId) {
    _netIdToEntity[netId] = entityId;
    _entityToNetId[entityId] = netId;
}

void ReplicationSystem::unregisterEntity(uint32_t netId) {
    auto it = _netIdToEntity.find(netId);
    if (it != _netIdToEntity.end()) {
        _entityToNetId.erase(it->second);
        _netIdToEntity.erase(it);
    }
}

uint32_t ReplicationSystem::findEntity(uint32_t netId) const {
    auto it = _netIdToEntity.find(netId);
    return (it != _netIdToEntity.end()) ? it->second : UINT32_MAX;
}

} // namespace ayt::net