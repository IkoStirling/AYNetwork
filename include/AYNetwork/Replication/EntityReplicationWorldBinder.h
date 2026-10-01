#pragma once

// Optional AYEntity integration. This header is intentionally separate from
// AYNetwork.h so the transport/replication core does not acquire a mandatory
// AYEntity link dependency.

#include <AYNetwork/Replication/ReplicationManager.h>
#include <AYNetwork/Replication/ReflectSerializer.h>

#include <AYEntity.h>
#include <AYEntity/ComponentRegistry.h>
#include <AYEntity/components/NetworkComponent.h>
#include <AYReflect/ReflectRegistry.h>

#include <cstdint>
#include <map>
#include <set>
#include <vector>

namespace ayt::net
{

class EntityReplicationWorldBinder {
public:
    EntityReplicationWorldBinder(ReplicationManager& manager, ayt::entity::World& world)
        : _manager(manager), _world(world) {}

    ~EntityReplicationWorldBinder() { clear(); }

    EntityReplicationWorldBinder(const EntityReplicationWorldBinder&) = delete;
    EntityReplicationWorldBinder& operator=(const EntityReplicationWorldBinder&) = delete;

    // Reconcile the World with ReplicationManager. Call once per simulation
    // frame before ReplicationManager::tick (or from an AYEntity system).
    // Entities and components added/removed since the previous call are
    // registered/unregistered automatically.
    size_t synchronize() {
        struct DesiredBinding {
            ayt::entity::IComponent* component = nullptr;
            const ayt::reflect::ITypeInfo* type = nullptr;
            uint32_t entityNetId = 0;
        };

        // R6 C2 (2026-08-25): std::map / std::set for deterministic iteration
        // (was unordered_map / unordered_set).
        std::map<uint32_t, DesiredBinding> desired;
        std::set<uint32_t> collisions;

        for (ayt::entity::Entity* entity : _world.getAllEntities()) {
            if (!entity) continue;
            auto* network = entity->getComponent<ayt::entity::NetworkComponent>();
            if (!network || !network->isValid()) continue;

            for (ayt::entity::IComponent* component : entity->getComponents()) {
                if (!component || component == network) continue;
                auto* type = ayt::reflect::TypeRegistryImpl::instance().findType(
                    typeid(*component).hash_code());
                if (!hasReplicatedFields(type)) continue;

                const uint64_t schemaHash = ReflectSerializer::hashTypeSchema(type);
                const auto* descriptor =
                    ayt::entity::ComponentRegistry::instance().find(*component);
                const bool multiple = descriptor && descriptor->multiplicity
                    == ayt::entity::ComponentMultiplicity::Multiple;
                const auto* instance = multiple
                    ? entity->componentInstance(component) : nullptr;
                if (multiple && (!instance
                    || !ayt::entity::isValidComponentInstanceId(instance->id)))
                    continue;
                const uint32_t objectNetId = multiple
                    ? makeComponentNetId(network->getNetId(), schemaHash,
                                         instance->id)
                    : makeComponentNetId(network->getNetId(), schemaHash);
                if (objectNetId == INVALID_NET_ID || !desired.emplace(
                        objectNetId, DesiredBinding{component, type, network->getNetId()}).second) {
                    collisions.insert(objectNetId);
                }
            }
        }

        for (uint32_t collision : collisions) desired.erase(collision);
        _collisionCount += collisions.size();

        for (auto it = _bindings.begin(); it != _bindings.end();) {
            const auto wanted = desired.find(it->first);
            if (wanted == desired.end() || wanted->second.component != it->second.component ||
                wanted->second.type != it->second.type) {
                _manager.unregisterObject(it->first);
                it = _bindings.erase(it);
            } else {
                desired.erase(wanted);
                ++it;
            }
        }

        for (const auto& [objectNetId, binding] : desired) {
            // Do not overwrite registrations owned outside this binder.
            if (_manager.findObject(objectNetId) != nullptr) {
                ++_collisionCount;
                continue;
            }
            _manager.registerObject(binding.component, binding.type, objectNetId);
            _bindings.emplace(objectNetId, Binding{
                binding.component, binding.type, binding.entityNetId});
        }
        return _bindings.size();
    }

    void clear() {
        std::vector<uint32_t> ids;
        ids.reserve(_bindings.size());
        for (const auto& [objectNetId, binding] : _bindings) {
            (void)binding;
            ids.push_back(objectNetId);
        }
        _bindings.clear();
        for (uint32_t objectNetId : ids) _manager.unregisterObject(objectNetId);
    }

    size_t boundComponentCount() const { return _bindings.size(); }
    size_t collisionCount() const { return _collisionCount; }

    static uint32_t makeComponentNetId(uint32_t entityNetId, uint64_t schemaHash) {
        uint32_t hash = 0x811C9DC5u;
        const auto append = [&hash](uint8_t byte) {
            hash ^= byte;
            hash *= 0x01000193u;
        };
        for (int i = 0; i < 4; ++i) append(static_cast<uint8_t>(entityNetId >> (i * 8)));
        for (int i = 0; i < 8; ++i) append(static_cast<uint8_t>(schemaHash >> (i * 8)));
        return hash == INVALID_NET_ID ? 1u : hash;
    }

    // Multi-instance bindings include the authored component slot. Peers must
    // create the same component IDs before synchronizing the binder.
    static uint32_t makeComponentNetId(uint32_t entityNetId,
                                       uint64_t schemaHash,
                                       const std::string& componentId) {
        uint32_t hash = makeComponentNetId(entityNetId, schemaHash);
        for (unsigned char byte : componentId) {
            hash ^= byte;
            hash *= 0x01000193u;
        }
        return hash == INVALID_NET_ID ? 1u : hash;
    }

private:
    struct Binding {
        ayt::entity::IComponent* component = nullptr;
        const ayt::reflect::ITypeInfo* type = nullptr;
        uint32_t entityNetId = 0;
    };

    static bool hasReplicatedFields(const ayt::reflect::ITypeInfo* type) {
        if (!type || ReflectSerializer::hashTypeSchema(type) == 0) return false;
        for (uint32_t i = 0; i < type->getFieldCount(); ++i) {
            const auto* field = type->getField(i);
            if (!field || !field->hasAttribute(ayt::reflect::FieldAttribute::NetReplicate)) continue;
            WireTypeId wid;
            if (ReflectSerializer::resolveWireTypeId(field->getType(), wid)) return true;
        }
        return false;
    }

    ReplicationManager& _manager;
    ayt::entity::World& _world;
    std::map<uint32_t, Binding> _bindings;  // R6 C2: sorted by netId
    size_t _collisionCount = 0;
};

} // namespace ayt::net
