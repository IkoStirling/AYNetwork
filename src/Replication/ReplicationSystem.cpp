// ReplicationSystem.cpp - R3.0 ECS bridge adapter
//
// R3.0 (2026-07-27): ReplicationSystem is no longer a peer of ReplicationManager
// holding its own netId↔entityId maps. Design §13 R3 explicitly forbids the
// double-map ("ReplicationSystem ↔ ReplicationManager 互通 / 禁止双 map").
// Instead, ReplicationSystem is a thin adapter that:
//   - Forwards onUpdate to ReplicationManager::tick so the GameLoop tick path
//     keeps working.
//   - Provides optional ECS wiring (registerSystem on AYEntity::World) via a
//     caller-supplied function pointer. The full ECS bridge is in
//     EntityReplicationAdapter.h.
//
// The old _netIdToEntity / _entityToNetId maps are GONE. Entity↔netId
// metadata is held by ReplicationManager._objects (and by NetworkComponent
// stubs in AYEntity for the user-facing API).

#include <Replication/ReplicationSystem.h>

namespace ayt::net
{

void ReplicationSystem::onStart() {
    // R3.0: no per-system priming needed. The ECS bridge in
    // EntityReplicationAdapter.h takes care of subscribing to AYEntity's
    // World::update path if the user wires it in.
}

void ReplicationSystem::onUpdate(float dt) {
    // R3.0: this class no longer holds its own data — forward to the manager
    // if one has been bound via setNetwork + getReplicationManager().
    if (!_network) return;
    auto* mgr = _network->getReplicationManager();
    if (mgr) mgr->tick(dt);
}

void ReplicationSystem::setNetwork(INetworkSubSystem* network) {
    _network = network;
}

} // namespace ayt::net