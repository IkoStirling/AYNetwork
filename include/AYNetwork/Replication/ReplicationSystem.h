#pragma once
// AYNetwork/Replication/AYNetwork/Replication/AYNetwork/Replication/ReplicationSystem.h - R3.0 thin adapter for ECS-style integration
//
// R3.0 (2026-07-27): previously held its own netId↔entityId maps and
// registerEntity/findEntity API. Design §13 R3 explicitly forbids the double
// map ("ReplicationSystem ↔ ReplicationManager 互通 / 禁止双 map"). This class
// is now a thin adapter that forwards onUpdate to ReplicationManager::tick so
// the GameLoop tick path keeps working with AYEntity's ISystem-style
// interface.
//
// ECS integration strategy:
//   - The user wires AYEntity::World::registerSystem with an instance of
//     this class (the ISystem adapter provides getName/onStart/onUpdate).
//   - ReplicationManager (held by INetworkSubSystem) owns the actual data;
//     this class never holds netId/object state.
//   - For per-entity registration, the user calls
//     EntityReplicationAdapter::registerEntityComponent<T>(mgr, entity, netId).
//     This class is NOT a peer of ReplicationManager in the data sense.

#include <AYNetwork/INetwork.h>
#include <cstdint>

namespace ayt::net
{

class ReplicationSystem {
public:
    ReplicationSystem() = default;

    const char* getName() const { return "Replication"; }

    void onStart();
    void onUpdate(float dt);

    // Set network subsystem. Required for onUpdate to forward ticks.
    void setNetwork(INetworkSubSystem* network);

private:
    INetworkSubSystem* _network = nullptr;
};

} // namespace ayt::net