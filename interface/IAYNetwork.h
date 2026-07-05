#pragma once
// IAYNetwork.h - 网络子系统接口

#include <AYCore.h>
#include <ISubSystem.h>
#include <functional>
#include <vector>
#include <cstdint>
#include <unordered_map>

// Transport layer
#include <AYConnection.h>
#include <KcpConnection.h>
#include <UdpSocket.h>

// Protocol layer
#include <PacketHeader.h>
#include <PacketAssembler.h>
#include <SequenceNumber.h>

// Replication layer (ReplicationManager is defined in this file)
#include <ReplicationSystem.h>
#include <NetDataComponent.h>

namespace ayt::net
{


// =============================================================================
// 常量
// =============================================================================
constexpr uint8_t CHANNEL_RELIABLE = 0;
constexpr uint8_t CHANNEL_UNRELIABLE = 1;
constexpr uint8_t CHANNEL_FRAGMENTED = 2;
constexpr uint8_t CHANNEL_ACK = 3;

constexpr uint32_t INVALID_NET_ID = 0;

// =============================================================================
// 连接模式
// =============================================================================
enum class ConnectionMode : uint8_t {
    Disconnected,
    Client,      // 连接到服务器
    Server,      // 监听服务器
    ListenServer // 监听服务器（也是客户端）
};

// =============================================================================
// 网络子系统接口
// =============================================================================
class INetworkSubSystem : public ISubSystem {
public:
    virtual ~INetworkSubSystem() = default;

    // ===== 连接管理 =====
    virtual void connect(const char* address, uint16_t port) = 0;
    virtual void listen(uint16_t port) = 0;
    virtual void disconnect() = 0;
    virtual bool isConnected() const = 0;
    virtual ConnectionMode getMode() const = 0;

    // ===== 消息发送 =====
    virtual void send(uint8_t channel, const void* data, size_t size) = 0;
    virtual void sendTo(NetConnection* conn, uint8_t channel, const void* data, size_t size) = 0;
    virtual void broadcast(uint8_t channel, const void* data, size_t size) = 0;
    virtual void broadcastExcept(NetConnection* exclude, uint8_t channel, const void* data, size_t size) = 0;

    // ===== 消息接收 =====
    using MessageHandler = std::function<void(NetConnection* conn, uint8_t channel, const void* data, size_t size)>;
    virtual void onMessage(uint8_t channel, MessageHandler handler) = 0;

    // ===== 连接状态 =====
    using ConnectionHandler = std::function<void(NetConnection* conn, bool connected)>;
    virtual void onConnectionChange(ConnectionHandler handler) = 0;

    // ===== 服务器专用 =====
    using AcceptCallback = std::function<bool(NetConnection*)>;
    using KickCallback = std::function<void(NetConnection*, const char* reason)>;

    virtual void setAcceptCallback(AcceptCallback callback) = 0;
    virtual void kickConnection(NetConnection* conn, const char* reason) = 0;
    virtual const std::vector<NetConnection*>& getConnections() = 0;

    // ===== 查询 =====
    virtual NetConnection* getConnection() const = 0;
    virtual uint32_t getHostId() const = 0;

    // ===== 扩展点 =====
    virtual void setExtension(INetworkExtension* ext) = 0;

    // ===== Replication =====
    virtual ReplicationManager* getReplicationManager() = 0;
};

// =============================================================================
// 连接接口
// =============================================================================
class NetConnection {
public:
    virtual ~NetConnection() = default;

    virtual uint32_t getId() const = 0;
    virtual uint32_t getHostId() const = 0;
    virtual const char* getAddress() const = 0;
    virtual bool isConnected() const = 0;
    virtual int getPing() const = 0;

    virtual void send(uint8_t channel, const void* data, size_t size) = 0;
    virtual void disconnect(const char* reason = nullptr) = 0;

    // 游戏数据关联
    virtual void setUserData(void* data) = 0;
    virtual void* getUserData() const = 0;
};

// =============================================================================
// 可复制对象接口
// =============================================================================
class IReplicable {
public:
    virtual ~IReplicable() = default;

    virtual uint32_t getNetId() const = 0;
    virtual void setNetId(uint32_t id) = 0;

    virtual float getReplicationPriority() const { return 1.0f; }
    virtual void replicate(BitStream& stream) = 0;
    virtual void onReplicate(const BitStream& stream) = 0;
    virtual uint8_t getReplicationChannel() const { return CHANNEL_RELIABLE; }
    virtual bool shouldReplicate() const { return true; }
};

// =============================================================================
// 游戏扩展接口
// =============================================================================
class INetworkExtension {
public:
    virtual ~INetworkExtension() = default;

    // 连接相关
    virtual bool onIncomingConnection(NetConnection* conn) { return true; }
    virtual void onConnectionDisconnected(NetConnection* conn) {}

    // 消息相关
    virtual void onRawMessage(NetConnection* conn, uint8_t channel, const void* data, size_t size) {}

    // Replication 相关
    virtual void onPreReplicate(IReplicable* obj, BitStream& stream, std::vector<NetConnection*>& targets) {}
    virtual void onPostReplicate(IReplicable* obj) {}
};

// =============================================================================
// BitStream - 位流序列化
// =============================================================================
class BitStream {
public:
    BitStream();
    BitStream(void* data, size_t size);
    ~BitStream();

    // 写入
    void writeBits(const void* data, size_t bitCount);
    void writeByte(uint8_t byte);
    void writeInt(int32_t value, int32_t minValue, int32_t maxValue);
    void writeFloat(float value, float minValue, float maxValue);
    void writeString(const char* str);

    // 读取
    void readBits(void* data, size_t bitCount);
    uint8_t readByte();
    int32_t readInt(int32_t minValue, int32_t maxValue);
    float readFloat(float minValue, float maxValue);
    void readString(char* out, size_t maxLen);

    // 操作
    void reset();
    void resetForRead();
    size_t getBitPosition() const { return _bitPosition; }
    size_t getBitCount() const { return _bitCount; }
    void* getData() { return _data; }
    const void* getData() const { return _data; }
    size_t getSize() const { return (_bitCount + 7) / 8; }

private:
    void* _data = nullptr;
    size_t _allocatedSize = 0;
    size_t _bitPosition = 0;
    size_t _bitCount = 0;
    bool _ownsData = false;

    void ensureCapacity(size_t additionalBytes);
};

// =============================================================================
// ReplicationManager - 复制管理器
// =============================================================================
class ReplicationManager {
public:
    ReplicationManager(INetworkSubSystem* network);
    ~ReplicationManager();

    void registerObject(IReplicable* obj, uint32_t netId);
    void unregisterObject(uint32_t netId);
    IReplicable* findObject(uint32_t netId) const;

    void replicate();
    void onReceive(BitStream& stream);
    void forceReplicate(uint32_t netId);

    void setExtension(INetworkExtension* ext);

private:
    INetworkSubSystem* _network = nullptr;
    INetworkExtension* _extension = nullptr;
    std::unordered_map<uint32_t, IReplicable*> _objects;
    std::vector<IReplicable*> _replicationQueue;
};

// =============================================================================
// 便捷宏
// =============================================================================
#define NETWORK_SUBSYSTEM() (ayt::game::GameLoop::instance().getNetwork())

} // namespace ayt::net