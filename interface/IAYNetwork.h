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

// R3.0 (2026-07-27): ReplicationManager holds `const ayt::reflect::ITypeInfo*`.
// We do NOT forward-declare ayt::reflect::ITypeInfo here — a forward
// declaration placed inside namespace ayt::net would be parsed as
// ayt::net::ayt::reflect::ITypeInfo, which collides with the real type once
// ReflectSerializer.cpp (or any consumer) includes <ayreflect/IReflect.h>.
// Callers that need the full type must include <ayreflect/IReflect.h>
// themselves. AYNetwork's CMakeLists PUBLIC-links AYReflect so the include
// path is available to consumers without extra setup.


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

// R3.0 (2026-07-27): Replication msgType slots. Body format documented in
// ReflectSerializer.h. Sent over CHANNEL_RELIABLE via PacketCodec seal.
constexpr uint16_t kMsgTypeReplication  = 0x0001;  // server → clients: full snapshot of one registered entity
constexpr uint16_t kMsgTypeEntitySpawn  = 0x0002;  // server → clients: register new replicated entity (carries typeHash)
constexpr uint16_t kMsgTypeEntityDespawn = 0x0003;  // server → clients: unregister replicated entity

// R3.1 (2026-07-27): Delta update msgType slot. Body wire format is identical
// to kMsgTypeReplication (same 8B header + N field records) — the receiver
// doesn't need to distinguish; deserialization uses the same path. The only
// difference is which fields are included (only dirty ones, not all).
constexpr uint16_t kMsgTypeDelta        = 0x0004;  // server → clients: dirty-fields-only delta update (R3.1)

// R2: schema version stamped into every PacketHeader. Bump on breaking
// wire-format changes (rare; major version bumps imply a parallel header
// migration). 1 = R2 baseline.
constexpr uint16_t kSchemaVersion = 1;

// =============================================================================
// R3.0 (2026-07-27): WireTypeId — closed enumeration of primitive field types
// that the AYReflect-driven ReflectSerializer knows how to pack into the wire.
// Each id maps to exactly one C++ primitive type. Reserved ids 12..15 are
// for R3.1+ (nested struct / array / pointer) — receivers must reject unknown
// ids rather than silently ignore them.
// =============================================================================
enum class WireTypeId : uint8_t {
    Bool   = 0,
    Int8   = 1,
    Int16  = 2,
    Int32  = 3,
    Int64  = 4,
    UInt8  = 5,
    UInt16 = 6,
    UInt32 = 7,
    UInt64 = 8,
    Float  = 9,
    Double = 10,
    String = 11,
    // 12..15 reserved for R3.1+
};

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
// 可复制对象接口 (R3.1 收口 2026-07-27)
//
// R1/R2 stub 时代用户必须实现 replicate(BitStream&) / onReplicate(BitStream&) 两个
// 虚函数 — 即"手写 wire format"。R3.0 起这两个函数被标记为 DEPRECATED，不再要求
// 用户实现；AYReflect 自动遍历字段（按 FieldAttribute::NetReplicate 过滤），由
// ReplicationManager::serializeObject / deserializeObject 走 ITypeInfo 元数据。
// R3.1 起两个 deprecated 虚函数已**真正删除**（全树 grep 验证无用户实现）。
//
// 仍然保留 getNetId / setNetId / getReplicationPriority / getReplicationChannel，
// 因为 ReplicationManager::registerObject(IReplicable*, ...) 包装路径会读它们。
// =============================================================================
class IReplicable {
public:
    virtual ~IReplicable() = default;

    virtual uint32_t getNetId() const = 0;
    virtual void setNetId(uint32_t id) = 0;

    virtual float getReplicationPriority() const { return 1.0f; }
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

    // R3.0 (2026-07-27): raw primitive writers for replication wire format.
    // These are NOT quantized — they emit fixed-width little-endian bytes so
    // the receiver can decode deterministically regardless of value range.
    // Use writeBits / writeByte when on-wire size must match an exact byte
    // layout (PacketHeader framing, ReplicationFrame fields).
    void writeBool(bool v)            { writeByte(v ? 1 : 0); }
    void writeInt8(int8_t v)          { writeByte(static_cast<uint8_t>(v)); }
    void writeInt16(int16_t v)        { writeByte(static_cast<uint8_t>(v & 0xFF)); writeByte(static_cast<uint8_t>((v >> 8) & 0xFF)); }
    void writeInt32Raw(int32_t v)     { writeByte(static_cast<uint8_t>(v)); writeByte(static_cast<uint8_t>(v >> 8)); writeByte(static_cast<uint8_t>(v >> 16)); writeByte(static_cast<uint8_t>(v >> 24)); }
    void writeInt64(int64_t v)        { for (int i = 0; i < 8; ++i) writeByte(static_cast<uint8_t>(v >> (i * 8))); }
    void writeUInt8(uint8_t v)        { writeByte(v); }
    void writeUInt16(uint16_t v)      { writeByte(static_cast<uint8_t>(v & 0xFF)); writeByte(static_cast<uint8_t>((v >> 8) & 0xFF)); }
    void writeUInt32(uint32_t v)      { writeByte(static_cast<uint8_t>(v)); writeByte(static_cast<uint8_t>(v >> 8)); writeByte(static_cast<uint8_t>(v >> 16)); writeByte(static_cast<uint8_t>(v >> 24)); }
    void writeUInt64(uint64_t v)      { for (int i = 0; i < 8; ++i) writeByte(static_cast<uint8_t>(v >> (i * 8))); }
    void writeFloatRaw(float v)       { writeUInt32(*reinterpret_cast<uint32_t*>(&v)); }
    void writeDouble(double v)        { writeUInt64(*reinterpret_cast<uint64_t*>(&v)); }

    // 读取
    void readBits(void* data, size_t bitCount);
    uint8_t readByte();
    int32_t readInt(int32_t minValue, int32_t maxValue);
    float readFloat(float minValue, float maxValue);
    void readString(char* out, size_t maxLen);

    // R3.0 raw primitive readers — inverse of writers above.
    bool     readBool()               { return readByte() != 0; }
    int8_t   readInt8()               { return static_cast<int8_t>(readByte()); }
    int16_t  readInt16()              { int16_t v = readByte(); v |= (int16_t(readByte()) << 8); return v; }
    int32_t  readInt32Raw()           { int32_t v = readByte(); v |= (int32_t(readByte()) << 8); v |= (int32_t(readByte()) << 16); v |= (int32_t(readByte()) << 24); return v; }
    int64_t  readInt64()              { int64_t v = 0; for (int i = 0; i < 8; ++i) v |= (int64_t(readByte()) << (i * 8)); return v; }
    uint8_t  readUInt8()              { return readByte(); }
    uint16_t readUInt16()             { uint16_t v = readByte(); v |= (uint16_t(readByte()) << 8); return v; }
    uint32_t readUInt32()             { uint32_t v = readByte(); v |= (uint32_t(readByte()) << 8); v |= (uint32_t(readByte()) << 16); v |= (uint32_t(readByte()) << 24); return v; }
    uint64_t readUInt64()             { uint64_t v = 0; for (int i = 0; i < 8; ++i) v |= (uint64_t(readByte()) << (i * 8)); return v; }
    float    readFloatRaw()           { uint32_t bits = readUInt32(); return *reinterpret_cast<float*>(&bits); }
    double   readDouble()             { uint64_t bits = readUInt64(); return *reinterpret_cast<double*>(&bits); }

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
//
// R3.0 (2026-07-27): registerObject takes a (void*, ITypeInfo*) pair so that
// replication walks AYReflect metadata directly — no per-class virtual needed.
// The old IReplicable overload is kept as a deprecated convenience wrapper that
// pulls netId / channel / priority off the IReplicable getters.
//
// R3.1 (2026-07-27): dirty-tracking + Delta frame. Each ReflectedEntry keeps
// a CRC32C hash of the last-broadcast value for every NetReplicate field.
// On tick(), the manager compares current hashes to the stored ones, emits
// either a Full Snapshot (kMsgTypeReplication on CHANNEL_RELIABLE) for the
// initial transmission OR a Delta frame (kMsgTypeDelta on CHANNEL_UNRELIABLE)
// carrying only dirty fields. forceReplicate(netId) forces a full resend on
// the next tick.
//
// Storage is a single map keyed by netId. _objects[netId] holds a ReflectedEntry
// that carries both the (void*, ITypeInfo*) used for serialization AND the
// optional IReplicable* for callers that still want priority / channel hooks.
// =============================================================================
class ReplicationManager {
public:
    ReplicationManager(INetworkSubSystem* network);
    ~ReplicationManager();

    // ---- R3.0 primary entry ----
    // Register an object for replication. `obj` must outlive the manager (or
    // until unregisterObject). `type` is the AYReflect ITypeInfo for T; use
    // TypeRegistryImpl::findType<T>() to obtain. On the server this also
    // broadcasts a kMsgTypeEntitySpawn frame to all clients so they allocate
    // matching slots. On a client it just records the local mapping.
    void registerObject(void* obj, const ayt::reflect::ITypeInfo* type, uint32_t netId);
    void unregisterObject(uint32_t netId);

    // ---- R1 deprecated wrapper (kept) ----
    void registerObject(IReplicable* obj, uint32_t netId);

    // ---- Lookups ----
    // Returns the ITypeInfo* registered for netId, or nullptr if not registered.
    const ayt::reflect::ITypeInfo* findType(uint32_t netId) const;
    void*                         findObject(uint32_t netId) const;
    IReplicable*                  findIReplicable(uint32_t netId) const;

    // ---- Frame-driven tick entry ----
    // Invoked by INetworkSubSystem::update every frame on the AUTHORITY (server).
    //
    // R3.1 dirty-tracking behavior:
    //   - First tick after registerObject: emits a kMsgTypeReplication full
    //     snapshot on CHANNEL_RELIABLE (so a fresh client has a baseline).
    //   - Steady-state ticks: compares CRC32C(field_value) per NetReplicate
    //     field; emits a kMsgTypeDelta frame on CHANNEL_UNRELIABLE carrying
    //     ONLY the dirty fields. No frame is emitted if nothing changed.
    //   - forceReplicate(netId): the next tick after this call uses Full
    //     Snapshot again (one-shot override via _initialized=false).
    //
    // On clients this is a no-op — the authority gate is enforced here
    // (§6.6 v1=Server 权威).
    void tick(float deltaTime);

    // ---- Receive path ----
    // Called by GnsConnection::onRawData when msgType ∈ {kMsgTypeReplication,
    // kMsgTypeEntitySpawn, kMsgTypeEntityDespawn, kMsgTypeDelta}. Looks up
    // the local type/object by netId and walks AYReflect to deserialize
    // field values back into memory. R3.1: kMsgTypeDelta uses the same
    // deserializeObject path as kMsgTypeReplication — wire format is
    // identical, only the fieldCount differs.
    // Returns true if the frame was consumed (registered object exists), false
    // otherwise (frame dropped — e.g. client→server authority gate).
    bool onReceive(BitStream& stream, NetConnection* from);

    // R3.1 real implementation: marks the entry as not-yet-initialized so
    // the next tick() emits a Full Snapshot regardless of dirty state.
    // Useful after a client reconnects, after a teleport, or after the user
    // explicitly changes a server-side field that all clients must observe.
    void forceReplicate(uint32_t netId);

    // R3.1 debug: returns the count of dirty NetReplicate fields pending on
    // the next tick(). 0 means steady state (no frame will be emitted for
    // this netId). SIZE_MAX means the netId isn't registered or has no
    // type info (legacy IReplicable path).
    size_t getDirtyFieldCount(uint32_t netId) const;

    void setExtension(INetworkExtension* ext);

    // ---- Test seams ----
    // Exposed for case 5 (AuthorityServerDropsClientReplicate). Returns count
    // of currently registered replicated objects on this manager.
    size_t getRegisteredCount() const { return _objects.size(); }

    // Test convenience: force a ConnectionMode without an INetworkSubSystem.
    // The default-constructed manager (no _network) acts as a CLIENT; tests
    // that want to exercise the server-authority broadcast path can call
    // this. The setter is a deliberate seam — production code paths always
    // use _network->getMode() and never call this.
    void setModeForTesting(ConnectionMode mode) { _forcedMode = mode; }
    ConnectionMode getEffectiveMode() const {
        if (_forcedMode != ConnectionMode::Disconnected) return _forcedMode;
        return _network ? _network->getMode() : ConnectionMode::Disconnected;
    }

    // Test convenience: a sink that receives every sealed frame the manager
    // would have broadcast on the wire. When set, tick() and the register/
    // unregister EntitySpawn/Despawn paths route through this instead of
    // _network->broadcast(). Production code does not set this.
    using BroadcastSink = std::function<void(uint8_t channel, const void* data, size_t size)>;
    void setBroadcastSinkForTesting(BroadcastSink sink) { _broadcastSink = std::move(sink); }

private:
    INetworkSubSystem* _network = nullptr;
    INetworkExtension* _extension = nullptr;

    // Test-only seam: when != Disconnected, overrides _network->getMode().
    // Lets unit tests exercise the server-authority broadcast path without
    // wiring a full INetworkSubSystem.
    ConnectionMode _forcedMode = ConnectionMode::Disconnected;
    BroadcastSink  _broadcastSink = nullptr;

    // Forward-declared below; single map shared by both register paths.
    struct ReflectedEntry;
    std::unordered_map<uint32_t, ReflectedEntry> _objects;
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