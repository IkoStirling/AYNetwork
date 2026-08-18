// AYNetworkSubSystem.cpp - 网络子系统实现

#include <AYNetwork.h>
#include <AYNetwork/NetworkModule.h>
#include <AYGameLoop.h>
#include <AYGameLoop/SubSystemRegistry.h>
#include <AYNetwork/Transport/GnsConnection.h>
#include <AYNetwork/Protocol/PacketCodec.h>
#include <AYNetwork/RPC/RpcHandler.h>
#include <AYNetwork/Transport/NetConnectionImpl.h>
// R1.A (2026-07-27): pull in EResult + AcceptConnection signature. The
// GnsConnection.cpp TU-private includes are sufficient because s_gns is a
// fully-typed pointer in this TU — we just need the constants.
#include <steam/steamclientpublic.h>
#include <steam/isteamnetworkingsockets.h>
#include <algorithm>
#include <cstdio>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace ayt::net
{

// =============================================================================
// NetworkSubSystem - 子系统实现
// =============================================================================
class NetworkSubSystem : public INetworkSubSystem {
    struct ServerClientRecord {
        std::unique_ptr<GnsConnection> transport;
        std::unique_ptr<NetConnectionImpl> facade;
        bool extensionDisconnectNotified = false;
    };

    enum class DeferredControlType : uint8_t {
        None,
        Connect,
        Listen,
        Disconnect,
        Shutdown,
    };

    struct DeferredControl {
        DeferredControlType type = DeferredControlType::None;
        std::string address;
        uint16_t port = 0;
    };

public:
    const char* getName() const override { return "Network"; }
    const ::ayt::game::SubSystemDescriptor& getDescriptor() const override {
        static ::ayt::game::SubSystemDescriptor desc = {
            .name = "Network",
            .dependencies = {},  // 无依赖
            .basePriority = 100,  // 早期初始化
            .timeType = ::ayt::game::SubSystemDescriptor::TimeType::Real,
            .phases = ::ayt::game::phaseBit(::ayt::game::FramePhase::Ingress)
                    | ::ayt::game::phaseBit(::ayt::game::FramePhase::FixedPrePhysics)
                    | ::ayt::game::phaseBit(::ayt::game::FramePhase::Egress),
            .clock = ::ayt::game::ClockDomain::RealWall,
            .phasePriority = 100,
            .reads = {},
            .writes = {"Simulation.World"}
        };
        return desc;
    }

    bool initialize() override {
        if (_initialized) return true;
        // R1 (2026-07-26): bring up GNS once per process. Ref-counted, so
        // subsequent initialise/shutdown cycles are safe.
        if (!gns::init()) {
            ::printf("[Network] Failed to init GameNetworkingSockets\n");
            return false;
        }
        _initialized = true;
        ::printf("[Network] Initialized (GNS ready)\n");
        return true;
    }

    void update(float deltaTime) override {
        // P0 audit fix (2026-07-26): no longer empty.
        // R1 (2026-07-26): forward every per-frame callback to the active
        // GnsConnection. ReplicationManager tick stays here so its deltaTime
        // advances even when there's no active connection (matches the
        // expected GameLoop semantics).
        // R1.A (2026-07-27): pump client conn (if any), server parent (if
        // listening), AND all server children (accepted client connections).
        pumpTransport();
        _replicationManager.tick(deltaTime);
        _rpcHandler.tick(deltaTime);
    }

    void tick(::ayt::game::FramePhase phase,
              const ::ayt::game::FrameContext& context) override {
        if (phase == ::ayt::game::FramePhase::Ingress) {
            _ingressTargetSimTick = context.simTick + 1;
            _stagedIngress = true;
            pumpTransport();
            _stagedIngress = false;
        } else if (phase == ::ayt::game::FramePhase::FixedPrePhysics) {
            // Apply packets assigned to this simulation tick once. All data
            // observed at this frame's Ingress belongs before its first
            // catch-up tick and is never replayed by later catch-up ticks.
            drainSimulationInbound(context.simTick);
            _rpcHandler.tick(context.fixedDeltaTime);
        } else if (phase == ::ayt::game::FramePhase::Egress) {
            // Observe the completed World state when producing replication.
            _replicationManager.tick(context.deltaTime);
        }
    }

    // Legacy direct-call compatibility. The staged loop uses tick() above and
    // never enters this adapter for an explicitly phased subsystem.
    void fixedUpdate(float fixedDeltaTime) override {
        update(fixedDeltaTime);
    }

    void shutdown() override {
        if (!_initialized) return;
        if (GnsConnection::isPumping()) {
            _deferredControl = {DeferredControlType::Shutdown, {}, 0};
            return;
        }
        shutdownNow();
    }

    void shutdownNow() {
        disconnectNow();
        {
            std::lock_guard<std::mutex> lock(_simulationInboundMutex);
            _simulationInbound.clear();
            _simulationInboundBytes = 0;
        }
        _ingressTargetSimTick = 0;
        gns::shutdown();
        _initialized = false;
        ::printf("[Network] Shutdown\n");
    }

    // ===== R1.A: server-side accept factory =====
    // Called from GnsConnection's global status callback when an incoming
    // connection arrives on _serverConn's listen socket. AcceptConnection +
    // SetConnectionPollGroup + adopt, then route through onMessage wiring.
    GnsConnection* adoptIncomingClient(HSteamNetConnection incoming) {
        if (!_serverConn) {
            ::fprintf(stderr, "[Network] incoming conn %u but server not listening\n", incoming);
            return nullptr;
        }
        if (_serverClients.size() >= _limits.maxConnections) {
            GnsConnection::s_gns->CloseConnection(
                incoming, 0, "server connection limit reached", false);
            ++_rejectedConnections;
            return nullptr;
        }
        // Accept the connection on GNS side first.
        EResult r = GnsConnection::s_gns->AcceptConnection(incoming);
        if (r != k_EResultOK) {
            ::fprintf(stderr, "[Network] AcceptConnection failed: %d\n", r);
            return nullptr;
        }
        // Create the server-child GnsConnection, install it in our list.
        auto child = std::make_unique<GnsConnection>();
        child->setProtocolVersion(_protocolVersion);
        child->adoptIncomingConnection(incoming);

        // R1.A: route onData to the registered message handler (if any).
        GnsConnection* rawChild = child.get();
        auto netConn = std::make_unique<NetConnectionImpl>(rawChild, allocateNetId());
        NetConnectionImpl* netPtr = netConn.get();

        // Both public admission APIs participate in the same atomic gate.
        bool accepted = !_acceptCallback || _acceptCallback(netPtr);
        if (accepted && _extension) {
            accepted = _extension->onIncomingConnection(netPtr);
        }
        if (!accepted) {
            rawChild->disconnect("connection admission rejected");
            ++_rejectedConnections;
            return nullptr;
        }

        child->onData(makeDataHandler(netPtr));

        if (_connectionHandler) {
            rawChild->onStateChange([this, rawChild, netPtr](GnsConnectionState /*oldS*/,
                                                             GnsConnectionState newS) {
                const bool connected = (newS == GnsConnectionState::Ready)
                    || (newS == GnsConnectionState::Connected
                        && rawChild->getProtocolVersion() == 0);
                const DisconnectReason reason = connected
                    ? DisconnectReason::Unknown
                    : rawChild->getLastDisconnectReason();
                _connectionHandler(netPtr, connected, reason);
            });
            // If handshake finished before the handler was installed, synthesize
            // the connected callback so late-join rebroadcast is not missed.
            if (rawChild->isConnected()) {
                _connectionHandler(netPtr, true, DisconnectReason::Unknown);
            }
        } else if (_pendingConnHandler) {
            rawChild->onStateChange([this, rawChild](GnsConnectionState /*oldS*/,
                                                     GnsConnectionState newS) {
                const bool connected = (newS == GnsConnectionState::Ready)
                    || (newS == GnsConnectionState::Connected
                        && rawChild->getProtocolVersion() == 0);
                const DisconnectReason reason = connected
                    ? DisconnectReason::Unknown
                    : rawChild->getLastDisconnectReason();
                _pendingConnHandler(connected, reason);
            });
        }

        GnsConnection* raw = child.get();
        ServerClientRecord record;
        record.transport = std::move(child);
        record.facade = std::move(netConn);
        _serverClients.push_back(std::move(record));
        ::printf("[Network] accepted incoming client (now %zu clients)\n", _serverClients.size());
        return raw;
    }

    GnsConnection::DataHandler makeDataHandler(NetConnection* from) {
        return [this, from](const uint8_t* data, size_t len) {
            dispatchIncoming(from, data, len);
        };
    }

    void dispatchIncoming(NetConnection* from, const uint8_t* data, size_t len) {
        if (!data || len < PacketCodec::kHeaderSize) {
            if (_messageHandlers[CHANNEL_RELIABLE]) {
                _messageHandlers[CHANNEL_RELIABLE](from, CHANNEL_RELIABLE, data, len);
            }
            return;
        }
        auto decoded = PacketCodec::decode(data, len);
        if (!decoded.ok) {
            if (_messageHandlers[CHANNEL_RELIABLE]) {
                _messageHandlers[CHANNEL_RELIABLE](from, CHANNEL_RELIABLE, data, len);
            }
            return;
        }
        const uint8_t channel = decoded.header.channel;
        switch (decoded.header.msgType) {
            case kMsgTypeRpcRequest:
            case kMsgTypeRpcResponse:
            case kMsgTypeRpcReject:
            case kMsgTypeDelta:
            case kMsgTypeReplication:
            case kMsgTypeEntitySpawn:
            case kMsgTypeEntityDespawn:
                if (_stagedIngress) {
                    queueSimulationInbound(decoded.header.msgType,
                                           from != nullptr ? from->getId() : 0,
                                           decoded.body.data(), decoded.body.size());
                } else {
                    applySimulationInbound(decoded.header.msgType, from,
                                           decoded.body.data(), decoded.body.size());
                }
                return;
            default:
                if (_messageHandlers[channel]) {
                    _messageHandlers[channel](from, channel, data, len);
                }
                return;
        }
    }

    struct PendingSimulationInbound {
        uint64_t targetSimTick = 0;
        uint16_t messageType = 0;
        uint32_t fromNetId = 0;
        std::vector<uint8_t> body;
    };

    void queueSimulationInbound(uint16_t messageType,
                                uint32_t fromNetId,
                                const uint8_t* body,
                                size_t bodySize) {
        std::lock_guard<std::mutex> lock(_simulationInboundMutex);
        if ((body == nullptr && bodySize != 0) ||
            bodySize > _limits.maxQueuedInboundBytes ||
            _simulationInbound.size() >= _limits.maxQueuedInboundMessages ||
            bodySize > _limits.maxQueuedInboundBytes - _simulationInboundBytes) {
            ++_droppedInboundMessages;
            return;
        }

        PendingSimulationInbound pending;
        pending.targetSimTick = _ingressTargetSimTick != 0
            ? _ingressTargetSimTick
            : 1;
        pending.messageType = messageType;
        pending.fromNetId = fromNetId;
        if (body != nullptr && bodySize != 0) {
            pending.body.assign(body, body + bodySize);
        }
        _simulationInboundBytes += pending.body.size();
        _simulationInbound.push_back(std::move(pending));
    }

    NetConnection* findConnectionById(uint32_t netId) const {
        if (netId == 0) return nullptr;
        if (_clientNetConn && _clientNetConn->getId() == netId) {
            return _clientNetConn.get();
        }
        for (const auto& connection : _serverClients) {
            if (connection.facade && connection.facade->getId() == netId) {
                return connection.facade.get();
            }
        }
        return nullptr;
    }

    void applySimulationInbound(uint16_t messageType,
                                NetConnection* from,
                                uint8_t* body,
                                size_t bodySize) {
        BitStream bodyStream(body, bodySize);
        bodyStream.resetForRead();
        switch (messageType) {
        case kMsgTypeRpcRequest:
            (void)_rpcHandler.onRpcRequest(bodyStream, from);
            break;
        case kMsgTypeRpcResponse:
            (void)_rpcHandler.onRpcResponse(bodyStream, from);
            break;
        case kMsgTypeRpcReject:
            (void)_rpcHandler.onRpcReject(bodyStream, from);
            break;
        case kMsgTypeReplication:
        case kMsgTypeDelta:
        case kMsgTypeEntitySpawn:
        case kMsgTypeEntityDespawn:
            (void)_replicationManager.onReceive(bodyStream, from);
            break;
        default:
            break;
        }
    }

    void drainSimulationInbound(uint64_t simTick) {
        std::vector<PendingSimulationInbound> due;
        {
            std::lock_guard<std::mutex> lock(_simulationInboundMutex);
            while (!_simulationInbound.empty()
                   && _simulationInbound.front().targetSimTick <= simTick) {
                due.push_back(std::move(_simulationInbound.front()));
                _simulationInboundBytes -= due.back().body.size();
                _simulationInbound.pop_front();
            }
        }

        for (PendingSimulationInbound& pending : due) {
            NetConnection* from = findConnectionById(pending.fromNetId);
            if (pending.fromNetId != 0 && from == nullptr) continue;
            applySimulationInbound(pending.messageType, from,
                                   pending.body.data(), pending.body.size());
        }
    }

    void installInboundRoutes() {
        if (_clientConn && _clientNetConn) {
            _clientConn->onData(makeDataHandler(_clientNetConn.get()));
        }
    }

    uint32_t allocateNetId() {
        do {
            ++_nextNetId;
        } while (_nextNetId == INVALID_NET_ID);
        return _nextNetId;
    }

    void pumpTransport() {
        const GnsPumpBudget budget{
            _limits.maxPumpMessages,
            _limits.maxPumpBytes,
        };
        const GnsPumpResult result = GnsConnection::pump(budget);
        if (result.budgetExhausted) ++_pumpBudgetExhaustions;
        reapDisconnectedClients();
        processDeferredControl();
    }

    void reapDisconnectedClients() {
        auto it = _serverClients.begin();
        while (it != _serverClients.end()) {
            if (!it->transport ||
                it->transport->getState() == GnsConnectionState::Disconnected) {
                if (_extension && it->facade &&
                    !it->extensionDisconnectNotified) {
                    _extension->onConnectionDisconnected(it->facade.get());
                    it->extensionDisconnectNotified = true;
                }
                it = _serverClients.erase(it);
            } else {
                ++it;
            }
        }
    }

    void clearServerClients(const char* reason) {
        for (auto& record : _serverClients) {
            if (_extension && record.facade &&
                !record.extensionDisconnectNotified) {
                _extension->onConnectionDisconnected(record.facade.get());
                record.extensionDisconnectNotified = true;
            }
            if (record.transport) record.transport->disconnect(reason);
        }
        _serverClients.clear();
    }

    void processDeferredControl() {
        if (_deferredControl.type == DeferredControlType::None ||
            GnsConnection::isPumping()) {
            return;
        }
        DeferredControl control = std::move(_deferredControl);
        _deferredControl = {};
        switch (control.type) {
        case DeferredControlType::Connect:
            connectNow(control.address.c_str(), control.port);
            break;
        case DeferredControlType::Listen:
            listenNow(control.port);
            break;
        case DeferredControlType::Disconnect:
            disconnectNow();
            break;
        case DeferredControlType::Shutdown:
            shutdownNow();
            break;
        case DeferredControlType::None:
            break;
        }
    }

    // ===== 连接管理 =====
    void connect(const char* address, uint16_t port) override {
        if (GnsConnection::isPumping()) {
            _deferredControl = {
                DeferredControlType::Connect,
                address ? address : "",
                port,
            };
            return;
        }
        connectNow(address, port);
    }

    void connectNow(const char* address, uint16_t port) {
        if (_clientConn) {
            _clientConn->disconnect("superseded by connect()");
            _clientConn.reset();
            _clientNetConn.reset();
        }
        _clientConn = std::make_unique<GnsConnection>();
        _clientConn->setProtocolVersion(_protocolVersion);
        _clientConn->initClient(address, port);
        _clientNetConn = std::make_unique<NetConnectionImpl>(
            _clientConn.get(), allocateNetId());
        _mode = ConnectionMode::Client;
        installInboundRoutes();
        if (_connectionHandler) {
            _clientConn->onStateChange([this](GnsConnectionState /*oldS*/, GnsConnectionState newS) {
                bool connected = (newS == GnsConnectionState::Ready) ||
                                 (newS == GnsConnectionState::Connected &&
                                  _clientConn->getProtocolVersion() == 0);
                DisconnectReason reason = connected
                    ? DisconnectReason::Unknown
                    : _clientConn->getLastDisconnectReason();
                _connectionHandler(_clientNetConn.get(), connected, reason);
            });
        }
    }

    void listen(uint16_t port) override {
        if (GnsConnection::isPumping()) {
            _deferredControl = {DeferredControlType::Listen, {}, port};
            return;
        }
        listenNow(port);
    }

    void listenNow(uint16_t port) {
        if (_serverConn) {
            _serverConn->disconnect("superseded by listen()");
            _serverConn.reset();
        }
        clearServerClients("superseded by listen()");

        _serverConn = std::make_unique<GnsConnection>();
        _serverConn->setProtocolVersion(_protocolVersion);
        _serverConn->initServer(port);
        const HSteamListenSocket listener = _serverConn->getInnerListenSocket();
        GnsConnection::setAdoptFactory(
            listener,
            [this](HSteamNetConnection incoming) -> GnsConnection* {
                return this->adoptIncomingClient(incoming);
            });
        // R4.1: listen() means "host is also a player" — design §6.6
        // distinguishes Server (dedicated) vs ListenServer (host). R4.0
        // collapsed these; R4.1 fixes the typo so ReplicationManager's
        // authority gate (which now accepts both via isAuthority()) sees
        // the right mode.
        _mode = ConnectionMode::ListenServer;
    }

    void disconnect() override {
        if (GnsConnection::isPumping()) {
            _deferredControl = {DeferredControlType::Disconnect, {}, 0};
            return;
        }
        disconnectNow();
    }

    void disconnectNow() {
        if (_clientConn) {
            _clientConn->disconnect("client disconnect");
            _clientConn.reset();
            _clientNetConn.reset();
        }
        if (_serverConn) {
            _serverConn->disconnect("server shutdown");
            _serverConn.reset();
        }
        clearServerClients("server shutdown");
        {
            std::lock_guard<std::mutex> lock(_simulationInboundMutex);
            _simulationInbound.clear();
            _simulationInboundBytes = 0;
        }
        _mode = ConnectionMode::Disconnected;
    }

    bool isConnected() const override {
        if (_clientConn && _clientConn->isConnected()) return true;
        for (auto& child : _serverClients) {
            if (child.transport && child.transport->isConnected()) return true;
        }
        return false;
    }

    bool isListening() const override {
        return _serverConn &&
            _serverConn->getInnerListenSocket() != k_HSteamListenSocket_Invalid;
    }

    ConnectionMode getMode() const override {
        return _mode;
    }

    void setProtocolVersion(uint32_t version) override {
        if (_clientConn || _serverConn || !_serverClients.empty()) {
            ::fprintf(stderr,
                "[Network] setProtocolVersion ignored while transport is active\n");
            return;
        }
        _protocolVersion = version;
    }

    uint32_t getProtocolVersion() const override { return _protocolVersion; }

    void setLimits(const NetworkLimits& limits) override {
        _limits.maxConnections = std::max(1u, limits.maxConnections);
        _limits.maxPumpMessages = std::max(1u, limits.maxPumpMessages);
        _limits.maxPumpBytes = std::max(1u, limits.maxPumpBytes);
        _limits.maxQueuedInboundMessages =
            std::max(1u, limits.maxQueuedInboundMessages);
        _limits.maxQueuedInboundBytes =
            std::max(1u, limits.maxQueuedInboundBytes);
    }

    NetworkLimits getLimits() const override { return _limits; }

    // ===== 消息发送 =====
    void send(uint8_t channel, const void* data, size_t size) override {
        if (_clientConn) {
            _clientConn->send(channel, data, size);
        }
    }

    void sendTo(NetConnection* conn, uint8_t channel, const void* data, size_t size) override {
        // R4.1: per-connection send. NetConnectionImpl::send forwards to the
        // wrapped GnsConnection::send which routes through the 4-channel
        // _rawSend switch (R4.0). Single-call indirection cost; identical
        // wire behavior to broadcast().
        if (!conn) return;
        conn->send(channel, data, size);
    }

    void broadcast(uint8_t channel, const void* data, size_t size) override {
        // R1.A: actually iterate all server children and send to each.
        for (auto& child : _serverClients) {
            if (child.transport && child.transport->isConnected()) {
                child.transport->send(channel, data, size);
            }
        }
    }

    void broadcastExcept(NetConnection* exclude, uint8_t channel, const void* data, size_t size) override {
        for (auto& record : _serverClients) {
            if (!record.facade || !record.facade->isConnected()) continue;
            if (exclude && record.facade.get() == exclude) continue;
            record.facade->send(channel, data, size);
        }
    }

    // ===== 消息接收 =====
    void onMessage(uint8_t channel, MessageHandler handler) override {
        _messageHandlers[channel] = handler;
        installInboundRoutes();
    }

    // ===== 连接状态 =====
    void onConnectionChange(ConnectionHandler handler) override {
        _connectionHandler = handler;
        if (_clientConn) {
            _clientConn->onStateChange([this](GnsConnectionState oldS, GnsConnectionState newS) {
                if (_connectionHandler) {
                    // R1 done: surface DisconnectReason. connected=true ->
                    // Unknown. connected=false -> whatever the GnsConnection
                    // captured (UserQuit, ConnectionLost, etc.).
                    bool connected = (newS == GnsConnectionState::Ready) ||
                                     (newS == GnsConnectionState::Connected &&
                                      _clientConn->getProtocolVersion() == 0);
                    DisconnectReason reason = connected
                        ? DisconnectReason::Unknown
                        : _clientConn->getLastDisconnectReason();
                    _connectionHandler(nullptr, connected, reason);
                }
            });
        }
        _pendingConnHandler = [this](bool connected, DisconnectReason reason) {
            if (_connectionHandler) _connectionHandler(nullptr, connected, reason);
        };
    }

    // ===== 服务器专用 =====
    void setAcceptCallback(AcceptCallback callback) override {
        _acceptCallback = callback;
    }

    void kickConnection(NetConnection* conn, const char* reason) override {
        // R4.1: real impl — NetConnection::disconnect forwards through the
        // adapter to the underlying GnsConnection.
        if (!conn) return;
        conn->disconnect(reason ? reason : "kicked by server");
    }

    const std::vector<NetConnection*>& getConnections() override {
        _connections.clear();
        _connections.reserve(_serverClients.size());
        for (auto& record : _serverClients) {
            if (record.facade && record.facade->isConnected()) {
                _connections.push_back(record.facade.get());
            }
        }
        return _connections;
    }

    // ===== 查询 =====
    NetConnection* getConnection() const override {
        return _clientNetConn.get();
    }

    uint32_t getHostId() const override {
        return _hostId;
    }

    // ===== 扩展点 =====
    void setExtension(INetworkExtension* ext) override {
        _extension = ext;
        _replicationManager.setExtension(ext);
    }

    // ===== Replication =====
    ReplicationManager* getReplicationManager() override {
        return &_replicationManager;
    }

    // ===== R4.0 RPC =====
    RpcHandler* getRpcHandler() override {
        return &_rpcHandler;
    }

private:
    ConnectionMode _mode = ConnectionMode::Disconnected;
    bool _initialized = false;
    bool _connected = false;
    uint32_t _hostId = 0;

    NetConnection* _connection = nullptr;
    std::vector<NetConnection*> _connections;

    MessageHandler _messageHandlers[256];
    ConnectionHandler _connectionHandler;
    AcceptCallback _acceptCallback;
    INetworkExtension* _extension = nullptr;

    // R1.A: subsystem owns one client conn (or none), one server parent
    // (listen socket, or none), and a list of server children (accepted clients).
    std::unique_ptr<GnsConnection> _clientConn;
    std::unique_ptr<NetConnectionImpl> _clientNetConn;
    std::unique_ptr<GnsConnection> _serverConn;
    std::vector<ServerClientRecord> _serverClients;
    uint32_t _nextNetId = 0;
    uint32_t _protocolVersion = kProtocolVersion;
    NetworkLimits _limits{};
    DeferredControl _deferredControl{};
    uint64_t _rejectedConnections = 0;
    uint64_t _pumpBudgetExhaustions = 0;
    uint64_t _droppedInboundMessages = 0;

    // Pending handler captured at onConnectionChange time so adoptIncomingClient()
    // can install it on each new server child.
    std::function<void(bool, DisconnectReason)> _pendingConnHandler;

    ReplicationManager _replicationManager{this};
    // R4.0 (2026-07-29): mirrors _replicationManager ownership. Routed by
    // onMessage via PacketHeader.msgType envelope kind.
    RpcHandler _rpcHandler{this};
    std::mutex _simulationInboundMutex;
    std::deque<PendingSimulationInbound> _simulationInbound;
    size_t _simulationInboundBytes = 0;
    uint64_t _ingressTargetSimTick = 0;
    bool _stagedIngress = false;
};

// 注册宏 — may be stripped from static libs; callers should also invoke
// registerNetworkSubSystem() explicitly (Editor Play, tests).
REGISTER_SUBSYSTEM(NetworkSubSystem, {}, 100);

INetworkSubSystem* findRegisteredNetworkSubSystem()
{
    auto* system = ::ayt::game::SubSystemRegistry::instance().findSubSystem("Network");
    return dynamic_cast<INetworkSubSystem*>(system);
}

void registerNetworkSubSystem()
{
    static bool registered = false;
    if (registered) {
        return;
    }
    registered = true;
    if (findRegisteredNetworkSubSystem() != nullptr) {
        return;
    }
    ::ayt::game::IGameLoop::instance().registerSubSystem(new NetworkSubSystem());
}

#if defined(AYNETWORK_BUILD_TESTS)
INetworkSubSystem* createNetworkSubSystemForTest() {
    return new NetworkSubSystem();
}
#endif

} // namespace ayt::net
