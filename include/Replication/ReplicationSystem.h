#pragma once
// ReplicationSystem.h - ECS system for replication
//
// R1 (2026-07-26): previously extended `ayt::entity::ISystem` which forced
// AYNetwork to link against AYEntity. AYEntity transitively pulls AYAnimation
// (which has its own MSVC env issues), causing unrelated build failures when
// we try to compile AYNetwork in isolation.
//
// This class now exposes a minimal, self-contained interface that R3 will
// integrate with AYEntity's World::registerSystem by passing the resulting
// instance through an adapter — see design.md §13 R3.

#include <IAYNetwork.h>
#include <unordered_map>
#include <cstdint>

namespace ayt::net
{

// =============================================================================
// ReplicationSystem - replicates entity state to subscribed clients
// =============================================================================
//
// Currently R1 provides a tiny helper that maps netId <-> entityId and
// exposes onUpdate for the GameLoop tick path. R3 will:
//   - Make this implement the AYEntity ISystem interface via a thin adapter
//   - Hook into AYEntity's Query<> to iterate replicated components
//   - Use AYReflect to walk data components and serialise only NetReplicate
//     fields
class ReplicationSystem {
public:
    ReplicationSystem() = default;

    const char* getName() const { return "Replication"; }

    void onStart();
    void onUpdate(float dt);

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