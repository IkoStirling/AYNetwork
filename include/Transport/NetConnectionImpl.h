#pragma once
// NetConnectionImpl.h - GnsConnection → NetConnection adapter (R4.1)
//
// R4.1 (2026-08): GnsConnection 仍独自管理传输（不强行继承 NetConnection，
// 避免传输层污染）。NetConnectionImpl 持 GnsConnection* 非拥有指针
// （生命周期由 AYNetworkSubSystem::_netConns 兜底），把 NetConnection
// 接口的 send/disconnect 转发到 _gns。getAddress/getHostId 缓存自 ctor
// 时刻（避免每帧 GNS getter 调用）；getPing/isConnected 仍每帧查 GNS。
//
// Use case: ReplicationManager::tick per-conn 路径 (R4.1 Interest Management)
// 通过 getConnections() 拿到 NetConnection*，再 sendTo() 走 sendTo →
// NetConnection::send → _gns->send(channel, data, size) → _rawSend 4-channel
// switch (R4.0 已有)。Game 端用 setUserData(FVector3*) 存玩家世界坐标做
// 距离裁剪（不引入新 API，不耦合 AYEntity）。
//
// AYNetwork.h umbrella 不引这个头（保持 R4.0 习惯 — 上层按需 include）。

#include <AYNetwork.h>
#include <cstdint>
#include <string>

namespace ayt::net
{

class GnsConnection;

class NetConnectionImpl : public NetConnection {
public:
    NetConnectionImpl(GnsConnection* gns, uint32_t netId);
    ~NetConnectionImpl() override = default;

    uint32_t getId() const override { return _id; }
    uint32_t getHostId() const override { return _hostId; }
    const char* getAddress() const override { return _address.c_str(); }
    bool isConnected() const override;
    int getPing() const override;

    void send(uint8_t channel, const void* data, size_t size) override;
    void disconnect(const char* reason = nullptr) override;

    void setUserData(void* data) override { _userData = data; }
    void* getUserData() const override { return _userData; }

    // R4.1: 测试 seam — 暴露底层 GnsConnection* 用于 E2E 多 conn 测试。
    GnsConnection* getInner() const { return _gns; }

private:
    GnsConnection* _gns = nullptr;   // 非拥有；由 AYNetworkSubSystem::_netConns 管理
    uint32_t _id = 0;
    uint32_t _hostId = 0;
    std::string _address;            // 缓存自 ctor，避免每帧 getter 调用
    void* _userData = nullptr;
};

} // namespace ayt::net
