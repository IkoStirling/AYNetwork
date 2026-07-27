// ReplicationManager.cpp - Replication manager implementation

#include <ReplicationManager.h>

namespace ayt::net
{

ReplicationManager::ReplicationManager(INetworkSubSystem* network)
    : _network(network)
{
}

ReplicationManager::~ReplicationManager() {
}

void ReplicationManager::registerObject(IReplicable* obj, uint32_t netId) {
    obj->setNetId(netId);
    _objects[netId] = obj;
}

void ReplicationManager::unregisterObject(uint32_t netId) {
    _objects.erase(netId);
}

IReplicable* ReplicationManager::findObject(uint32_t netId) const {
    auto it = _objects.find(netId);
    return (it != _objects.end()) ? it->second : nullptr;
}

// P0 audit fix (2026-07-26): minimal tick entry point. R1 will route real
// per-frame work (GNS RunCallbacks dispatch, snapshot diffing, dirty-mark
// flushing) through here. For now: assert the wiring is alive and forward
// to replicate() so existing call paths continue to no-op gracefully.
void ReplicationManager::tick(float deltaTime) {
    (void)deltaTime;   // unused until R1
    replicate();
}

void ReplicationManager::replicate() {
}

void ReplicationManager::onReceive(BitStream& stream) {
}

void ReplicationManager::forceReplicate(uint32_t netId) {
    auto* obj = findObject(netId);
    if (obj) {
    }
}

void ReplicationManager::setExtension(INetworkExtension* ext) {
    _extension = ext;
}

// P0 audit fix (2026-07-26): removed the `template<typename T> serializeObject`
// stub. The audit (design.md §3.4) flagged this as a misdesigned API —
// it forced per-type specialization instead of walking AYReflect metadata.
// R3 will reintroduce the method as a non-template:
//   void serializeObject(const ayt::reflect::ITypeInfo* type, void* obj, BitStream& s);

} // namespace ayt::net