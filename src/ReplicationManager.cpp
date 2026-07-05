// ReplicationManager.cpp - 复制管理器实现

#include <AYNetwork.h>

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
    // TODO: 实现Replication流程
}

void ReplicationManager::onReceive(BitStream& stream) {
    // TODO: 实现客户端接收流程
}

void ReplicationManager::forceReplicate(uint32_t netId) {
    // TODO: 实现强制同步
}

void ReplicationManager::setExtension(INetworkExtension* ext) {
    _extension = ext;
}

} // namespace ayt::net