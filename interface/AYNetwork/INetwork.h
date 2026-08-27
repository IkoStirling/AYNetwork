#pragma once
// AYNetwork/INetwork.h - 网络子系统接口

#include <AYCore.h>
#include <AYGameLoop.h>   // P0 audit fix (2026-07-26): ISubSystem lives in AYGameLoop's IAYGameLoop.h, not in a separate ISubSystem.h
#include <functional>
#include <bit>
#include <memory>
#include <vector>
#include <cstdint>
#include <map>
#include <unordered_map>
#include <unordered_set>

// R5.3 (2026-08-24): IReplayRecorder lives in the AYReplay foundation
// module; it transitively pulls in only <AYReplay/ReplayTypes.h> which
// has no AYNetwork dependency — so we can safely include it here.
#include <AYReplay/IReplayRecorder.h>

// R5.4 (2026-08-25): TransportFaultProfile is a header-only struct
// declared inside ayt::net — no transitive AYNetwork deps. Included here
// so callers can pass a profile to setTransportFaultProfile without an
// extra include. The profile's channel masks are hardcoded values (0x01,
// 0x02, 0x04, 0x08, 0x0F) — they intentionally do NOT depend on the
// CHANNEL_* constants below — so this include is safe to put BEFORE them.
#include <AYNetwork/TransportFaultProfile.h>
#include <AYNetwork/P2P.h>

// R5.5 (2026-08-25): ProfilerSnapshot is a header-only POD struct also
// declared inside ayt::net — no transitive AYNetwork deps. We include it
// here so the pull-API virtuals below can take ProfilerSnapshot by
// reference and callers don't need a separate include.
#include <AYNetwork/Profiler/ProfilerSnapshot.h>

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
class SnapshotInterpolator;  // R5.0 forward declaration
class ReflectSerializer;     // R5.2: AckTail re-uses ReflectSerializer::AckTail

// R5.3 (2026-08-24): Replay recorder. We do NOT forward-declare
// ayt::replay::IReplayRecorder here because doing so inside namespace
// ayt::net creates `ayt::net::ayt::replay::IReplayRecorder`, which
// collides with the real type once any consumer pulls in
// <AYReplay/IReplayRecorder.h>. Same rationale as the comment below for
// ayt::reflect::ITypeInfo. Callers must use the fully-qualified name
// `::ayt::replay::IReplayRecorder*` (we cannot help it).

// R3.0 (2026-07-27): ReplicationManager holds `const ayt::reflect::ITypeInfo*`.
// We do NOT forward-declare ayt::reflect::ITypeInfo here — a forward
// declaration placed inside namespace ayt::net would be parsed as
// ayt::net::ayt::reflect::ITypeInfo, which collides with the real type once
// ReflectSerializer.cpp (or any consumer) includes <AYReflect/IReflect.h>.
// Callers that need the full type must include <AYReflect/IReflect.h>
// themselves. AYNetwork's CMakeLists PUBLIC-links AYReflect so the include
// path is available to consumers without extra setup.

// R4.0 (2026-07-29): forward-declare RpcHandler — referenced by
// INetworkSubSystem::getRpcHandler(). Same rule: consumers that need
// the full type include <RPC/AYNetwork/RPC/AYNetwork/RPC/AYNetwork/RPC/RpcHandler.h>. AYNetwork's CMakeLists
// compiles RpcHandler.cpp into the same library so the link is
// automatic.
class RpcHandler;


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

// R5.2 (2026-08-24): Per-entity proxy kind. Drives whether a ghost is
// predicted (AutonomousProxy), interpolated (SimulatedProxy), or just
// replicated. Default SimulatedProxy keeps R3-R5.1 behavior byte-for-byte.
// Server kind is reserved for future use (e.g. server-only ghosts that
// never replicate to clients).
enum class ProxyKind : uint8_t {
    Server           = 0,
    AutonomousProxy  = 1,
    SimulatedProxy   = 2,
};

// Hard runtime budgets for untrusted network input.  Defaults are deliberately
// conservative for a game process; applications may lower them before
// connect()/listen().  Zero is never interpreted as "unlimited".
struct NetworkLimits {
    uint32_t maxConnections = 256;
    uint32_t maxPumpMessages = 512;
    uint32_t maxPumpBytes = 2u * 1024u * 1024u;
    uint32_t maxQueuedInboundMessages = 4096;
    uint32_t maxQueuedInboundBytes = 8u * 1024u * 1024u;
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
// INetworkSubSystem defaults to kProtocolVersion and therefore performs the
// handshake. Version 0 is an explicit legacy/test opt-out for callers that
// construct GnsConnection directly.
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
// AYNetwork/Replication/ReflectSerializer.h. Sent over CHANNEL_RELIABLE via
// PacketCodec seal.
constexpr uint16_t kMsgTypeReplication  = 0x0001;  // server → clients: full snapshot of one registered entity
constexpr uint16_t kMsgTypeEntitySpawn  = 0x0002;  // server → clients: register entity (carries schemaHash)
constexpr uint16_t kMsgTypeEntityDespawn = 0x0003;  // server → clients: unregister replicated entity

// R3.1 (2026-07-27): Delta update msgType slot. Body wire format is identical
// to kMsgTypeReplication (same 14B header + N field records) — the receiver
// doesn't need to distinguish; deserialization uses the same path. The only
// difference is which fields are included (only dirty ones, not all).
constexpr uint16_t kMsgTypeDelta        = 0x0004;  // server → clients: dirty-fields-only delta update (R3.1)

// R4.1-B: minimal 3-vector for interest / distance culling. Stored on
// NetConnection::setUserData (viewer) and via ReplicationManager::setObjectLocation.
struct NetVec3 {
    float x = 0.f;
    float y = 0.f;
    float z = 0.f;
};

// R4.0 (2026-07-29): RPC msgType namespace. 0x0005..0x000F reserved for
// future R3.3 back-compat shadow; 0x0010..0x0012 carry the RPC
// request / response / reject triplet. wire schemaVersion=1 unchanged
// (R3.x receiver hits onPacketBody demux mismatch and silently drops
// the frame — same drop semantics as R3.2 WireTypeId 12..15 nested).
//
// Direction in production:
//   kMsgTypeRpcRequest  — caller → server (Server RPC) OR caller → 1 client (Client RPC) OR server → all clients (Multicast)
//   kMsgTypeRpcResponse — RPC server → original caller (return value or void)
//   kMsgTypeRpcReject   — RPC server → caller (validator deny / unknown method / parse fail)
constexpr uint16_t kMsgTypeRpcRequest   = 0x0010;
constexpr uint16_t kMsgTypeRpcResponse  = 0x0011;
constexpr uint16_t kMsgTypeRpcReject    = 0x0012;
// R4.1-B: application-level ACK echo for RequiresAck frames (CHANNEL_ACK).
constexpr uint16_t kMsgTypeAppAck       = 0x0013;
// R5.2 (2026-08-24): per-connection client input batch (one msgType carries
// N input records stacked contiguously). Body format documented in
// AYNetwork/Prediction/ClientInputCodec.h. Sent client → server over
// CHANNEL_UNRELIABLE for input traffic (loss tolerated; server reconciles).
constexpr uint16_t kMsgTypeClientInput  = 0x0014;

// R2: schema version stamped into every PacketHeader. Bump on breaking
// wire-format changes (rare; major version bumps imply a parallel header
// migration). 1 = R2 baseline.
constexpr uint16_t kSchemaVersion = 2;

// =============================================================================
// R3.0 (2026-07-27): WireTypeId — closed enumeration of primitive field types
// that the AYReflect-driven ReflectSerializer knows how to pack into the wire.
// Each id maps to exactly one C++ primitive type.
// =============================================================================
// R3.2 (2026-07-28): ids 12..15 cover nested struct + container types. See
// design.md §5.1 wire format for the per-id byte layout. R3.0/R3.1 receivers
// hit `default: return false` in resolveWireTypeId / deserializeObject for
// these ids and silently drop the entire frame (next tick Full re-sync heals
// the state). This keeps wire schemaVersion = 1 unchanged across R3.0..R3.2.
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
    // R3.2 nested types:
    NestedStruct  = 12,  // struct with its own ITypeInfo (recursive)
    FixedArray    = 13,  // std::array<T, N> — N ≤ 255
    DynamicArray  = 14,  // std::vector<T> — size u32 (no upper bound)
    StringMap     = 15,  // std::map<std::string, V> — heterogeneous key/value
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
    virtual bool isListening() const { return false; }
    virtual ConnectionMode getMode() const = 0;

    // ===== P2P / NAT traversal =====
    // Configure once after initialize() and before any process-wide GNS
    // connection/listener becomes active (GNS identity changes close active
    // links). The signaling backend is replaceable and contains no GNS types.
    // Existing IP connect()/listen() remain fully supported.
    virtual bool configureP2P(const P2PConfig& config,
                              std::shared_ptr<ISignalingTransport> signaling) {
        (void)config; (void)signaling; return false;
    }
    virtual bool listenP2P() { return false; }
    virtual bool connectP2P(const PeerId& remotePeer) {
        (void)remotePeer; return false;
    }
    virtual bool isP2PConfigured() const { return false; }
    virtual PeerId getLocalPeerId() const { return {}; }
    virtual P2PConnectionInfo getP2PConnectionInfo(NetConnection* connection = nullptr) const {
        (void)connection; return {};
    }
    // Session/roster v1 is a read-mostly facade over live engine connections;
    // it does not turn the signaling relay into a lobby or game-state server.
    virtual P2PSessionInfo getP2PSessionInfo() const { return {}; }
    virtual std::vector<P2PPeerInfo> getP2PPeers() const { return {}; }
    virtual NetConnection* findP2PPeer(const PeerId& peer) const {
        (void)peer; return nullptr;
    }
    virtual bool disconnectP2PPeer(const PeerId& peer,
                                   const char* reason = nullptr) {
        (void)peer; (void)reason; return false;
    }

    // Must be configured before connect()/listen().  The production default
    // is kProtocolVersion; version 0 is an explicit legacy/test opt-out.
    virtual void setProtocolVersion(uint32_t version) { (void)version; }
    virtual uint32_t getProtocolVersion() const { return 0; }

    virtual void setLimits(const NetworkLimits& limits) { (void)limits; }
    virtual NetworkLimits getLimits() const { return {}; }

    // ===== 消息发送 =====
    virtual void send(uint8_t channel, const void* data, size_t size) = 0;
    virtual void sendTo(NetConnection* conn, uint8_t channel, const void* data, size_t size) = 0;
    virtual void broadcast(uint8_t channel, const void* data, size_t size) = 0;
    virtual void broadcastExcept(NetConnection* exclude, uint8_t channel, const void* data, size_t size) = 0;

    // Internal protocol path for bytes already sealed by PacketCodec. Default
    // adapters preserve source compatibility; the GNS subsystem overrides
    // these to avoid wrapping a second kMsgTypeApp envelope.
    virtual void sendEncoded(uint8_t channel, const void* data, size_t size) {
        send(channel, data, size);
    }
    virtual void sendEncodedTo(NetConnection* conn, uint8_t channel,
                               const void* data, size_t size) {
        sendTo(conn, channel, data, size);
    }
    virtual void broadcastEncoded(uint8_t channel, const void* data, size_t size) {
        broadcast(channel, data, size);
    }

    // ===== 消息接收 =====
    // Application handlers always receive the decoded PacketCodec body. The
    // transport envelope (header and CRC) is validated and removed before
    // this callback runs.
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

    // ===== R4.0 RPC =====
    // Owns the dispatcher for envelope msgType 0x0010..0x0012. Wired into
    // AYNetworkSubSystem::update — when a sealed frame's
    // PacketHeader.msgType matches a kMsgTypeRpc* slot, the subsystem hands
    // the body to this RpcHandler instead of the ReplicationManager.
    // Always non-null after R4.0 (subsystem ctors construct the
    // RpcHandler in lock-step with the ReplicationManager). Returns the
    // same instance for the lifetime of the subsystem.
    virtual RpcHandler* getRpcHandler() = 0;

    // R5.3 (2026-08-24): opt-in replay recorder. Pass nullptr to disable.
    // Must be called from the main thread before listen()/connect() and
    // before the first Egress tick. Internally forwarded to both the
    // ReplicationManager and RpcHandler.
    virtual void setReplayRecorder(ayt::replay::IReplayRecorder* rec) = 0;
    virtual ayt::replay::IReplayRecorder* getReplayRecorder() const   = 0;

    // R5.4 (2026-08-25): per-connection transport-fault profile. Pass an
    // `isNoOp()` profile or call `clearTransportFaultProfile(netId)` to
    // disable. Profiles are pure data — the actual fault logic lives in
    // `TransportFaultInterceptor` (per-`GnsConnection`, see src/Transport/).
    // NetId is the AYNetwork connection id (NOT the GNS handle). Profiles
    // can be installed/cleared any time after subsystem init; they take
    // effect on the next send/recv tick. Not thread-safe; main-thread only.
    // RNG is seeded from `profile.randomSeed` if non-zero, otherwise from
    // a per-process default — the seed is what makes loss/dup/reorder
    // draws reproducible across runs.
    //
    // Non-pure default impls (no-op) so existing test stubs that inherit
    // `INetworkSubSystem` don't need to override them. Production
    // (`AYNetworkSubSystem`) overrides all three to forward to its
    // internal `TransportFaultController`.
    virtual void setTransportFaultProfile(uint32_t /*netId*/,
                                          const TransportFaultProfile& /*profile*/) {}
    virtual void clearTransportFaultProfile(uint32_t /*netId*/) {}
    virtual const TransportFaultProfile* getTransportFaultProfile(uint32_t /*netId*/) const { return nullptr; }

    // R5.5 (2026-08-25): bandwidth / connection profiler pull API.
    // Default impls are no-ops so existing test stubs that inherit
    // `INetworkSubSystem` don't need to override them. Production
    // (`AYNetworkSubSystem`) overrides all four to its internal
    // `ProfilerRegistry`. Pull is from the main thread only — the
    // profiler is single-threaded by design (record* / snapshot / window
    // tick all run on the update path).
    //
    // getProfilerSnapshot copies one snapshot for a single connection
    // (the conn's AYNetwork netId). getProfilerSnapshots clears `out`
    // and appends one entry per known connection (typically ≤ 64).
    // setProfilerSinkForTesting installs an after-each-update callback.
    // setProfilerDumpInterval enables the periodic [AYProfiler] stderr
    // dump every N window-ticks (0 = off, default).
    virtual void getProfilerSnapshot(ProfilerSnapshot& /*out*/, uint32_t /*netId*/) {}
    virtual void getProfilerSnapshots(std::vector<ProfilerSnapshot>& /*out*/) {}
    virtual void setProfilerSinkForTesting(
        std::function<void(const ProfilerSnapshot&)> /*sink*/) {}
    virtual void setProfilerDumpInterval(uint32_t /*ticks*/) {}

    // R6 C9 (2026-08-25): test seam — direct access to the underlying
    // ReplicationManager so the state-equal test suite can populate the
    // object registry without going through a real GNS-driven broadcast.
    // Default null impl keeps test stubs that don't need it non-abstract.
    // Production code never calls this.
    virtual ReplicationManager* getReplicationManagerForTesting() { return nullptr; }

    // R6 C9 (2026-08-25): determinism contract — a single 64-bit hash that
    // captures the observable state of the network subsystem after every
    // public mutation. State-equal hashing: two runs with identical
    // inputs (clock seam, RNG seed, RPC calls, replicated bytes) MUST
    // produce identical hashes; failures of byte-equal wire records are
    // acceptable as long as the post-decode state matches.
    //
    // Default impl returns 0 so existing test stubs that inherit
    // `INetworkSubSystem` don't need to override it. Production
    // (`AYNetworkSubSystem`) overrides with an FNV-1a 64-bit mix over
    // `_objects` reflected bytes + `_pendingCalls` keys + the current
    // `_serverTick` + per-connection `_ackedSeq` cursors + `_nextFragmentId`.
    enum class HashKind : uint8_t {
        StateOnly       = 0, // replication + cursors (default)
        StatePlusProfiler = 1, // includes profiler counters
    };
    virtual uint64_t computeStateHash(HashKind /*kind*/ = HashKind::StateOnly) { return 0; }

    // R6.5-3 (2026-08-25): replay-pump bridge seam. `tickRecordedEvent`
    // is invoked by an external `NetworkReplayEventDecoder` driver to
    // feed a single recorded event (in its raw adapter pack layout —
    // see `NetworkReplayRecorderAdapter`) into the live subsystem. The
    // implementation strips the adapter-prefix bytes and routes to the
    // matching live seam (ReplicationManager::onReceive / onClientInput /
    // setObjectProxyKind, RpcHandler::onRpcXxx). The default impl is a
    // no-op so existing test stubs that inherit `INetworkSubSystem`
    // don't need to override it. Production (`AYNetworkSubSystem`) wires
    // the demux table at src/AYNetworkSubSystem.cpp.
    //
    // `installReplayRngSeed` is a companion seam for the replay
    // pump: when a deterministic replay session installs an RNG seed
    // (recorded in ReplayFileHeader.randomSeed) the live subsystem can
    // latch it for future RngApi consumers. Today this is a stub — R7
    // will route through `TransportFaultController::setSessionSeed`.
    // `hasReplayRngSeed` / `getReplayRngSeed` are read-back accessors so
    // tests can verify the recorded seed round-tripped through the
    // subsystem. Default impls return false / 0 so test stubs stay
    // non-abstract.
    virtual void tickRecordedEvent(uint32_t /*eventType*/,
                                   const uint8_t* /*payload*/,
                                   size_t /*size*/) {}
    virtual void installReplayRngSeed(uint64_t /*seed*/) {}
    virtual bool     hasReplayRngSeed() const { return false; }
    virtual uint64_t getReplayRngSeed() const { return 0; }
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
    //
    // R4.1 (2026-08): onPreReplicate 签名升级。
    // 旧签名 (IReplicable* obj, BitStream& stream, vector<NetConnection*>& targets)
    // 是 R1 时代 stub，R4.0 主路径用 (void* obj, const ITypeInfo* type, uint32_t
    // netId) 反射注册 (AYNetwork/INetwork.h:415)，旧签名从未真正调用。R4.1 统一到
    // 反射路径 (void*, ITypeInfo*, netId)：
    //   - `obj`  -- 反射注册时的对象指针 (game 直接读字段)
    //   - `type` -- AYReflect 类型元数据 (ITypeInfo::getField 等可查)
    //   - `netId`-- stable identifier 用于日志/调试
    //   - `targets`-- mutable; 扩展可清空 entries 排除特定 conn。ReplicationManager
    //               已在 fire 前做完 distance cull，所以 extension 看到的 conn
    //               集合已是"距离内"的子集。
    // 默认 impl: no-op。
    virtual void onPreReplicate(void* obj, const ayt::reflect::ITypeInfo* type,
                                uint32_t netId,
                                std::vector<NetConnection*>& targets) {}
    // R4.1-B: per-viewer relevancy gate applied before distance cull and
    // onPreReplicate. Return false to exclude `viewer` from `targets`.
    virtual bool isRelevant(NetConnection* viewer, void* obj,
                            const ayt::reflect::ITypeInfo* type,
                            uint32_t netId) {
        (void)viewer; (void)obj; (void)type; (void)netId;
        return true;
    }
    // R4.1: 同步升级签名 (R1 旧签名 no-op default 保留向后兼容)。
    virtual void onPostReplicate(void* obj, const ayt::reflect::ITypeInfo* type,
                                 uint32_t netId) {}
    // R4.1-B: per-field callback after a NetReplicate field is applied on receive.
    // Fires only for fields tagged FieldAttribute::RepNotify. `fieldName` is the
    // stable IFieldInfo::getName() string.
    virtual void onRepNotify(void* obj, const ayt::reflect::ITypeInfo* type,
                             uint32_t netId, const char* fieldName) {
        (void)obj; (void)type; (void)netId; (void)fieldName;
    }
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
    void writeFloatRaw(float v)       { writeUInt32(std::bit_cast<uint32_t>(v)); }
    void writeDouble(double v)        { writeUInt64(std::bit_cast<uint64_t>(v)); }

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
    float    readFloatRaw()           { return std::bit_cast<float>(readUInt32()); }
    double   readDouble()             { return std::bit_cast<double>(readUInt64()); }

    // 操作
    void reset();
    void resetForRead();
    size_t getBitPosition() const { return _bitPosition; }
    size_t getBitCount() const { return _bitCount; }
    // R5.0 (2026-08-24): seek to a previously captured bit position. Used by
    // the onReceive fallback path that probes both R5.0 (with prefix) and
    // R3.x (no prefix) layouts before committing to one. Clamped to
    // [0, getBitCount()] to keep callers safe.
    void setBitPosition(size_t bits);
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
    // TypeRegistryImpl::findType<T>() to obtain. On authority, tick() sends
    // Spawn + Full independently to each newly visible peer. On a client this
    // only records the local mapping.
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
    //     Snapshot again for every currently visible peer.
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
    bool onReceive(uint16_t messageType, BitStream& stream, NetConnection* from);

    // R3.1 real implementation: marks the entry as not-yet-initialized so
    // the next tick() emits a Full Snapshot regardless of dirty state.
    // Useful after a client reconnects, after a teleport, or after the user
    // explicitly changes a server-side field that all clients must observe.
    void forceReplicate(uint32_t netId);

    // R5.1: tell the authority that `netId` teleported this tick. The next
    // tick() emits a Full Snapshot with the kFlagTeleport flag set AND
    // forces the per-peer init reset (so the Full is sent even when no
    // fields are dirty). After emission the marker is cleared — callers
    // that want a continuous teleport must call this every tick.
    //
    // Use this for: spawn resync, level transitions, position snapping,
    // physics-driven hard cuts (ragdoll on, vehicle entry). Anything that
    // would otherwise produce a visible lerp sweep across the world.
    void markTeleported(uint32_t netId);

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

    // R4.1-A: client-side EntitySpawn announcements (netId → schemaHash) received
    // from the authority. Game code calls registerObject after allocating the
    // local object; until then replicate frames for that netId are dropped.
    bool peekSpawnAnnouncement(uint32_t netId, uint64_t& schemaHashOut) const;
    size_t spawnAnnouncementCount() const { return _spawnAnnouncements.size(); }

    // Authority-only: resend EntitySpawn for an already-registered netId.
    // When targetConn is non-null, the spawn goes only to that connection
    // (late joiner). Otherwise all connected clients receive the announcement.
    // Also schedules a full snapshot on the next tick() via forceReplicate.
    bool rebroadcastEntitySpawn(uint32_t netId, NetConnection* targetConn = nullptr);

    // ---- R4.1-B Interest Management ----
    // interestRadius <= 0 disables distance culling (broadcast / all conns).
    // Viewer position: NetConnection::setUserData(NetVec3*).
    // Object position: setObjectLocation(netId, loc) each tick or on move.
    void setInterestRadius(float interestRadius);
    float getInterestRadius() const { return _interestRadius; }
    void setObjectLocation(uint32_t netId, NetVec3 location);
    bool getObjectLocation(uint32_t netId, NetVec3& out) const;

    // R5.1 (teleport): debug/test seam. Returns true if `netId` currently
    // has a pending teleport marker. Production code never calls this.
    bool isTeleportPending(uint32_t netId) const {
        return _teleportPending.count(netId) > 0;
    }

    // ---- R5.2 (2026-08-24) Client Prediction ----
    // Authority-only: declares `netId` as client-owned (the client's
    // AutonomousProxy predicts this ghost locally and uploads inputs).
    // SimulatedProxy is the default — clients only interpolate.
    // Calling on a client is a no-op (asserted via the comment; the
    // setter still records the value but it has no wire effect).
    // ownerConnectionId is required for AutonomousProxy on an authority so
    // ACK tails can be emitted only to the owning peer. Zero means no owner.
    void setObjectProxyKind(uint32_t netId, ProxyKind kind,
                            uint32_t ownerConnectionId = 0);
    ProxyKind getObjectProxyKind(uint32_t netId) const;

    // Sugar for game code: true on the locally-controlling client, false
    // everywhere else. Equivalent to
    //   getObjectProxyKind(id) == ProxyKind::AutonomousProxy && !isAuthority()
    // but reads more naturally at the call site.
    bool isLocallyControlled(uint32_t netId) const;

    // Input ring capacity (default 32, drop-oldest). Matches SnapshotBuffer
    // cap. Tests can shrink/extend to exercise overflow.
    void     setInputRingCapacity(uint32_t cap) { _inputRingCapacity = cap; }
    uint32_t getInputRingCapacity() const { return _inputRingCapacity; }

    // Server-side: read-only peek of the last input seq that the server has
    // acknowledged for `connectionId`. 0 means "no acks yet" or
    // "unknown connection". Test-only surface.
    uint32_t getLastAckedInputTick(uint32_t connectionId) const;

    // Server-side: plug the gameplay-side input application callback. The
    // callback is invoked once per consumed client input per fixed tick
    // (FixedPrePhysics). Default is no-op.
    // connectionId / inputSeq identify the record (one per client message);
    // payload / payloadSize carry the opaque gameplay-decoded bytes.
    using InputApplicationFn = std::function<void(uint32_t connectionId,
                                                  uint32_t inputSeq,
                                                  const uint8_t* payload,
                                                  size_t payloadSize)>;
    void setInputApplicationFn(InputApplicationFn fn) {
        _inputApplicationFn = std::move(fn);
    }

    // Drives server-side consumption of queued client inputs for the
    // current fixed tick. No-op on clients. Caller is the phased driver
    // (AYNetworkSubSystem at FixedPrePhysics start).
    void consumeClientInputs(uint32_t simTick);

    // Server-side: receive a client input frame and queue it for the next
    // consumeClientInputs. Called by the dispatch table when
    // kMsgTypeClientInput arrives. Returns true on accept.
    bool onClientInput(uint32_t connectionId,
                       const uint8_t* body, size_t bodySize);

    // Server-side: build an AckTail destined for `connectionId`. Returns
    // AckTail{present=false} when no AutonomousProxy ghost owned by this
    // connection is in the current frame, or when the manager isn't on
    // the authority. Caller passes the result into the Full Snapshot
    // emitter (ReflectSerializer::writeAckTail).
    struct AckTailInfo {
        uint32_t lastAckedInputTick = 0;
        uint32_t serverCommandAge   = 0;
        bool     present            = false;
    };
    AckTailInfo buildAckTailForConnection(uint32_t connectionId,
                                          uint32_t serverCommandAge) const;

    // Test seam: read-only access to the underlying prediction manager.
    // Production code should not need this; tests use it to drive ring
    // checks without going through the dispatch table.
    class PredictionManager* getPredictionManagerForTesting() const { return _prediction; }

    // ---- R5.0 Snapshot Interpolation ----
    // Server side: tick(dt) increments an internal monotonic serverTick
    // counter at the configured tick rate (default 30 Hz). The counter is
    // stamped into every emitted Full Snapshot / Delta frame body so
    // receivers can interpolate. Production callers invoke advanceServerTick
    // once per frame; the accumulator carries the sub-tick fractional time
    // so the count stays accurate when dtSec drifts.
    void     setServerTickRate(double hz);
    double   getServerTickRate() const { return _serverTickRate; }
    void     advanceServerTick(double dtSec);
    uint32_t getServerTick() const { return _serverTick; }
    // Test-only seam: inject a deterministic tick value (used by tests to
    // reproduce specific bracket positions). Production code paths never
    // call this.
    void     setServerTickForTesting(uint32_t t) { _serverTick = t; }

    // Client side: hand the manager a SnapshotInterpolator instance so
    // onReceive can push every successfully-deserialized snapshot into the
    // interpolation buffer. The interpolator is externally owned (typically
    // by the application or by INetworkSubSystem); lifetime must outlive
    // the manager.
    void setSnapshotInterpolator(SnapshotInterpolator* si);

    // R6 C9 (2026-08-25): state-equal hash helpers. Read-only views onto
    // the replicated-object registry. `knownNetIdsForHash` returns the
    // set of registered netIds (callers sort for deterministic order).
    // `serializeObjectForHash` writes the current reflected byte payload
    // for `netId` into `out` and returns true on success; false means
    // netId isn't registered, has no type info, or no longer has live
    // memory. Both are public so the state-hash implementation can live
    // outside this class without friending the implementation site.
    std::vector<uint32_t> knownNetIdsForHash() const;
    bool serializeObjectForHash(uint32_t netId, std::vector<uint8_t>& out) const;

    // R6 C9 (2026-08-25): per-connection last-acked-input cursor (server-side
    // only — clients always read 0). Returns the set of connectionIds that
    // have ever had an input acknowledged, and the value each one is at.
    // The state-equal hash folds (connectionId, lastAckedSeq) pairs in
    // sorted connectionId order so two recordings of identical play
    // produce the same hash regardless of the connection-accept order
    // (connectionId itself is stable per C5 B-10 stub).
    std::vector<uint32_t> ackedSeqKeysForHash() const;
    uint32_t             ackedSeqForHash(uint32_t connectionId) const;

private:
    INetworkSubSystem* _network = nullptr;
    INetworkExtension* _extension = nullptr;
    SnapshotInterpolator* _snapshotInterpolator = nullptr;  // R5.0 client sink

public:
    // R4.1: authority gate. Server (dedicated) AND ListenServer (host) are
    // both server-authoritative. Returns true if the effective mode is one
    // of those. Promoted public in R5.3 (2026-08-24) so the Replay wire-tap
    // hooks (NetworkReplayRecorderAdapter) can gate per-call; behavior is
    // unchanged — callers outside this class are read-only observers.
    bool isAuthority() const;

    // R5.3 (2026-08-24) Replay integration. Wire-tap hooks in tick()/register/
    // unregister paths consult this pointer. Setting nullptr disables
    // recording. Lifetime is managed by the caller (typically the subsystem);
    // the recorder MUST outlive any tick that consults it. Foundation
    // IReplayRecorder is included at file scope (line 14 of this header)
    // so the unqualified name resolves to ::ayt::replay::IReplayRecorder.
    void setReplayRecorder(ayt::replay::IReplayRecorder* r) { _replay = r; }
    ayt::replay::IReplayRecorder* getReplayRecorder() const { return _replay; }

    // R5.5 (2026-08-25): profiler send hook. Called for every sealed
    // replication frame the manager would broadcast (Full / Delta /
    // Spawn / Despawn). Signature mirrors the GnsConnection transport
    // hook: (connNetId, msgType, bytes, ghostNetId). Default null =
    // no-op. Lifetime managed by the caller (typically the subsystem).
    // The hook fires AFTER sendSealedToConnection returns success, so
    // bytes reflects the post-broadcast count (consistent with the
    // transport-layer hooks).
    using ProfilerSendHook = std::function<void(uint32_t connNetId,
                                                uint16_t msgType,
                                                uint64_t bytes,
                                                uint32_t ghostNetId)>;
    void setProfilerSendHook(ProfilerSendHook hook) { _profilerHook = std::move(hook); }

private:

    std::vector<NetConnection*> buildInterestTargets(
        void* obj, const ayt::reflect::ITypeInfo* type, uint32_t netId,
        NetVec3 objLoc, bool hasObjLoc) const;
    bool sendSealedToTargets(void* obj, const ayt::reflect::ITypeInfo* type, uint32_t netId,
                             NetVec3 objLoc, bool hasObjLoc,
                             uint8_t channel, const void* data, size_t size);
    bool sendSealedToConnection(NetConnection* target, uint8_t channel,
                                const void* data, size_t size);
    NetConnection* findConnectedTarget(uint32_t connectionId) const;

    // Test-only seam fields (see public setModeForTesting / setBroadcastSinkForTesting).
    ConnectionMode _forcedMode = ConnectionMode::Disconnected;
    BroadcastSink  _broadcastSink = nullptr;
    float _interestRadius = 0.f;
    float _interestRadiusSq = 0.f;

    // R5.0 server tick accounting. _serverTickAccumulatorUs carries the
    // sub-tick fractional time between calls so the tick count stays
    // accurate even if dtSec drifts.
    //
    // R6 (2026-08-25): converted from double to uint64 microseconds (B-02,
    // M-17). The accumulator now tracks `dtSec * tickRate` in
    // microsecond units, removing platform-dependent float rounding
    // from the serverTick truncation path.
    uint32_t _serverTick = 0;
    uint64_t _serverTickAccumulatorUs = 0;
    double   _serverTickRate = 30.0;

    // Forward-declared below; single map shared by both register paths.
    struct ReflectedEntry;
    // R6 C2 (2026-08-25): std::map (was std::unordered_map). tick() iterates
    // _objects to enumerate ghosts; sorting by netId makes the wire-side
    // frame order deterministic across runs (state-equal replay).
    std::map<uint32_t, ReflectedEntry> _objects;
    std::unordered_map<uint32_t, uint64_t> _spawnAnnouncements;

    // R5.1 (teleport): per-netId one-shot marker set by markTeleported().
    // tick() drains the set: each marked netId emits a Full Snapshot with
    // the kFlagTeleport flag and the marker is removed.
    std::unordered_set<uint32_t> _teleportPending;

    // R5.2 (2026-08-24) prediction scaffolding.
    // Per-netId ProxyKind. Default SimulatedProxy keeps R3-R5.1 byte behavior.
    std::unordered_map<uint32_t, ProxyKind> _proxyKinds;
    std::unordered_map<uint32_t, uint32_t> _proxyOwnerConnections;
    // Default input ring capacity (32, drop-oldest). Mutable via
    // setInputRingCapacity (test seam).
    uint32_t _inputRingCapacity = 32;
    // Server-side: per-connection last-acked input seq, copied out into Full
    // Snapshot ack tail bytes. Cleared by the prediction manager on ack.
    std::unordered_map<uint32_t, uint32_t> _lastAckedInputTick;
    // Gameplay-side callback wired by the application; called once per
    // consumed client input per fixed tick. Default no-op keeps server-only
    // tests (no app) green.
    InputApplicationFn _inputApplicationFn;
    // Owns per-connection input rings + per-ghost predicted state. Set in
    // the ctor; tests may swap via setPredictionManagerForTesting.
    class PredictionManager* _prediction = nullptr;

    // R5.3 (2026-08-24): Replay recorder pointer (non-owning). See public
    // setter above for lifetime contract.
    ayt::replay::IReplayRecorder* _replay = nullptr;

    // R5.5 (2026-08-25): profiler send hook. Set by AYNetworkSubSystem at
    // construction time; default null keeps test stubs zero-cost.
    ProfilerSendHook _profilerHook;
};

#if defined(AYNETWORK_BUILD_TESTS)
// R4.1-A: heap-allocate a NetworkSubSystem for integration tests (defined in
// AYNetworkSubSystem.cpp). Caller owns the pointer; delete via INetworkSubSystem*.
INetworkSubSystem* createNetworkSubSystemForTest();
#endif

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
