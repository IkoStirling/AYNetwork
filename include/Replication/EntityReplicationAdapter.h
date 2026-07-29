#pragma once
// EntityReplicationAdapter.h - R3.0 ECS bridge (header-only)
//
// R3.0 (2026-07-27): thin ECS bridge between AYEntity and ReplicationManager.
// The user wires AYEntity's World update to drive replication by calling
// ReplicationManager::tick directly, OR uses this helper to bind per-entity
// components automatically.
//
// Minimal R3.0 scope: header-only helper that registers/unregisters an
// IComponent-subclassed data component on a given Entity. The user calls
// `registerEntityComponent(entity, netId)` once; the adapter extracts the
// component pointer + its reflected type info and forwards to
// ReplicationManager::registerObject.
//
// R3.1+ may grow into a full World-level subscription that auto-discovers
// entities holding NetworkComponent and registers/unregisters them as they
// enter/leave the world.

#include <Replication/ReplicationManager.h>

#include <cstdint>

// AYEntity forward-declared to avoid pulling AYEntity into AYNetwork's
// transitive link surface (the design §11 audit flagged this dependency).
namespace ayt::entity { class Entity; }

namespace ayt::net
{

class EntityReplicationAdapter {
public:
    // Register a single component instance on `entity` for replication.
    // `T` must satisfy:
    //   - derived from ayt::entity::IComponent
    //   - reflected via AYTYPE_REGISTER + AYTYPE_FIELDS with NetReplicate
    //     on at least one field
    // `netId` must be unique within this manager.
    //
    // Returns true on success, false if the component is not present on the
    // entity or the type metadata is not registered.
    template <typename T>
    static bool registerEntityComponent(ReplicationManager& mgr, ayt::entity::Entity* entity, uint32_t netId);

    static bool unregisterEntityComponent(ReplicationManager& mgr, uint32_t netId);
};

} // namespace ayt::net

// =============================================================================
// Inline template implementation. Kept in the header because AYEntity's
// Entity class is forward-declared above; the implementation needs the full
// definition (T::getComponent<T>, TypeRegistryImpl::findType<T>).
// =============================================================================
#include <ayreflect/ReflectRegistry.h>

#include <type_traits>

namespace ayt::net
{

template <typename T>
bool EntityReplicationAdapter::registerEntityComponent(ReplicationManager& mgr, ayt::entity::Entity* entity, uint32_t netId) {
    if (!entity) return false;
    static_assert(std::is_base_of_v<::ayt::entity::IComponent, T>,
                  "T must derive from ayt::entity::IComponent");
    T* comp = entity->template getComponent<T>();
    if (!comp) return false;

    const auto* type = ayt::reflect::TypeRegistryImpl::instance().findType<T>();
    if (!type) return false;

    mgr.registerObject(static_cast<void*>(comp), type, netId);
    return true;
}

inline bool EntityReplicationAdapter::unregisterEntityComponent(ReplicationManager& mgr, uint32_t netId) {
    if (netId == 0) return false;
    mgr.unregisterObject(netId);
    return true;
}

} // namespace ayt::net