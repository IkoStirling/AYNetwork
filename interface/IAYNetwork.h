#pragma once
// IAYNetwork.h - 网络子系统接口

#include <AYCore.h>
#include <AYGameLoop.h>   // P0 audit fix (2026-07-26): ISubSystem lives in AYGameLoop's IAYGameLoop.h, not in a separate ISubSystem.h
#include <functional>
#include <vector>
#include <cstdint>
#include <unordered_map>

namespace ayt::net
{

// P0 audit fix (2026-07-26): forward-declare types referenced before their
// full definition below. INetworkSubSystem methods mention NetConnection,
// INetworkExtension, BitStream, and ReplicationManager — all of which are
// defined later in this file. Without forward declarations MSVC rejects the
// declarations with C2061 ("undeclared identifier") even though the same
// file eventually defines them.
class NetConnection;
class IReplicable;
class INetworkExtension;
class BitStream;
class ReplicationManager;


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
// R1 done (2026-07-27): DisconnectReason — applied enum that travels with
// every disconnect so receivers can react meaningfully (kick UI, reconnect,
// ban, log telemetry). design §4.2 mandates this enum be delivered via
// onConnectionChange. R1 implements a uint8 wire form so the enum is
// stable across R2/R3 changes.
// =============================================================================
enum class DisconnectReason : uint8_t {
    Unknown              = 0,    // GNS closed without app-level reason
    UserQuit             = 1,    // Local user requested disconnect
    Timeout              = 2,    // Heartbeat / connection idle
    Kicked               = 3,    // Server kicked the client
    ProtocolMismatch     = 4,    // handshake rejected (version, etc.)
    HostShutdown         = 5,    // Server shutting down
    ConnectionLost       = 6,    // Network error / GNS ProblemDetectedLocally
};

// =============================================================================
// R1 done (2026-07-27): Handshake protocol.
//
// Wire format (host byte order, no PacketHeader to keep R1 independent of
// R2 protocol layer — PacketHeader/Assembler is R2 work):
//
//   Client -> Server:  [u8 msgType=1][u32 protocolVersion][u8 clientNameLen][clientName bytes...]
//   Server -> Client:  [u8 msgType=2][u32 protocolVersion][u8 reasonCode=0 (accept)]
//                   or [u8 msgType=3][u8 reasonCode=DisconnectReason (reject)]
//
// Protocol negotiation rules:
//   - protocolVersion must match exactly. Mismatch -> server sends REJECT.
//   - On REJECT the client transitions to Disconnected with reasonCode.
//   - On WELCOME both sides transition from Handshaking to Connected (Ready).
//   - The handshake is RELIABLE; uses CHANNEL_RELIABLE.
//
// Handshake only fires when the application has registered a
// protocolVersion (via AYNetworkSubSystem::setProtocolVersion). If never
// set, GnsConnection::initClient/listen will skip the handshake and treat
// GNS Connected as Ready (back-compat for the R1.A loopback tests).
// =============================================================================
enum class HandshakeMsgType : uint8_t {
    Hello   = 1,    // client -> server
    Welcome = 2,    // server -> client (accept)
    Reject  = 3,    // server -> client (reject)
};

// Wire-level constants for the handshake header. Kept short so a single
// GNS reliable message comfortably fits in one MTU.
constexpr uint32_t kProtocolVersion   = 1;
constexpr uint8_t  kHandshakeMaxNameLen = 32;

// R2 (2026-07-27): PacketHeader v2 msgType namespace. 0xFFFF is reserved
// for handshake (HandshakeMsgType lives in the body). 0 is the default app
// msgType before R3 ReplicationManager assigns per-system ranges.
constexpr uint16_t kMsgTypeHandshake = 0xFFFF;
constexpr uint16_t kMsgTypeApp       = 0;

// R2: schema version stamped into every PacketHeader. Bump on breaking
// wire-format changes (rare; major version bumps imply a parallel header
// migration). 1 = R2 baseline.
constexpr uint16_t kSchemaVersion = 1;

// =============================================================================
// 网络子系统接口
// =============================================================================
// P0 audit fix (2026-07-26): qualify ISubSystem with ::ayt::game — the file
// was relying on accidental namespace resolution. Inside `namespace ayt::net`,
// an unqualified `ISubSystem` is looked up in `ayt::net` first, then `ayt`,
// then `::`. `ISubSystem` lives in `ayt::game`, so the lookup silently
// resolved through `using` declarations from headers — fragile. Make it
// explicit.
class INetworkSubSystem : public ::ayt::game::ISubSystem {
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
    // R1 done (2026-07-27): onConnectionChange now carries DisconnectReason
    // so receivers can distinguish a normal disconnect from a kick / version
    // mismatch / host shutdown. reason is meaningful only when connected=false;
    // for connected=true it's DisconnectReason::Unknown.
    using ConnectionHandler = std::function<void(NetConnection* conn, bool connected, DisconnectReason reason)>;
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

    // P0 audit fix (2026-07-26): frame-driven tick entry point invoked by
    // INetworkSubSystem::update every frame. R1 routes real per-frame work
    // (GNS callback dispatch, dirty-mark flush, snapshot diffing) through here.
    void tick(float deltaTime);

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
// P0 audit fix (2026-07-26): NETWORK_SUBSYSTEM() was defined here but pointed
// at ayt::game::GameLoop::instance().getNetwork(), which does not exist in
// AYGameLoop. Removed until R1 wires up the GameLoop accessor.
//
// Usage post-R1 (planned):
//   auto* net = ayt::game::GameLoop::instance().getNetwork();
//   net->connect("127.0.0.1", 7777);

} // namespace ayt::net