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

template<typename T>
void ReplicationManager::serializeObject(T* obj, BitStream& stream) {
}

} // namespace ayt::net