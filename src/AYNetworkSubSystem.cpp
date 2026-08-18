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
#include <cstdio>
#include <deque>
#include <memory>
#include <mutex>
#include <vector>

namespace ayt::net
{

// =============================================================================
// NetworkSubSystem - 子系统实现
// =============================================================================
class NetworkSubSystem : public INetworkSubSystem {
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
        // R1 (2026-07-26): bring up GNS once per process. Ref-counted, so
        // subsequent initialise/shutdown cycles are safe.
        if (!gns::init()) {
            ::printf("[Network] Failed to init GameNetworkingSockets\n");
            return false;
        }
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
        if (_clientConn) {
            _clientConn->update();
        }
        if (_serverConn) {
            _serverConn->update();
        }
        for (auto& child : _serverClients) {
            if (child) child->update();
        }
        _replicationManager.tick(deltaTime);
        _rpcHandler.tick(deltaTime);
    }

    void tick(::ayt::game::FramePhase phase,
              const ::ayt::game::FrameContext& context) override {
        if (phase == ::ayt::game::FramePhase::Ingress) {
            _ingressTargetSimTick = context.simTick + 1;
            _stagedIngress = true;
            if (_clientConn) _clientConn->update();
            if (_serverConn) _serverConn->update();
            for (auto& child : _serverClients) {
                if (child) child->update();
            }
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
        disconnect();
        {
            std::lock_guard<std::mutex> lock(_simulationInboundMutex);
            _simulationInbound.clear();
        }
        _ingressTargetSimTick = 0;
        // R1.A: clear adopt factory so a stray incoming connection during
        // shutdown doesn't try to register through us.
        GnsConnection::setAdoptFactory(nullptr);
        gns::shutdown();
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
        // Accept the connection on GNS side first.
        EResult r = GnsConnection::s_gns->AcceptConnection(incoming);
        if (r != k_EResultOK) {
            ::fprintf(stderr, "[Network] AcceptConnection failed: %d\n", r);
            return nullptr;
        }
        // Create the server-child GnsConnection, install it in our list.
        auto child = std::make_unique<GnsConnection>();
        child->adoptIncomingConnection(incoming);

        // R1.A: route onData to the registered message handler (if any).
        GnsConnection* rawChild = child.get();
        NetConnectionImpl* netPtr = nullptr;
        {
            auto netConn = std::make_unique<NetConnectionImpl>(rawChild, ++_nextNetId);
            netPtr = netConn.get();
            _netConns.push_back(std::move(netConn));
        }

        // R4.1: INetworkExtension::onIncomingConnection gate. Returning false
        // rejects the connection. Fire BEFORE pushing into _serverClients so
        // the extension's decision is atomic with the accept.
        if (_extension && !_extension->onIncomingConnection(netPtr)) {
            rawChild->disconnect("rejected by extension");
            _netConns.pop_back();
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
        _serverClients.push_back(std::move(child));
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
        PendingSimulationInbound pending;
        pending.targetSimTick = _ingressTargetSimTick != 0
            ? _ingressTargetSimTick
            : 1;
        pending.messageType = messageType;
        pending.fromNetId = fromNetId;
        if (body != nullptr && bodySize != 0) {
            pending.body.assign(body, body + bodySize);
        }

        std::lock_guard<std::mutex> lock(_simulationInboundMutex);
        _simulationInbound.push_back(std::move(pending));
    }

    NetConnection* findConnectionById(uint32_t netId) const {
        if (netId == 0) return nullptr;
        if (_clientNetConn && _clientNetConn->getId() == netId) {
            return _clientNetConn.get();
        }
        for (const auto& connection : _netConns) {
            if (connection && connection->getId() == netId) return connection.get();
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

    // ===== 连接管理 =====
    void connect(const char* address, uint16_t port) override {
        if (_clientConn) {
            _clientConn->disconnect("superseded by connect()");
            _clientConn.reset();
            _clientNetConn.reset();
        }
        _clientConn = std::make_unique<GnsConnection>();
        _clientConn->initClient(address, port);
        _clientNetConn = std::make_unique<NetConnectionImpl>(_clientConn.get(), ++_nextNetId);
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
        if (_serverConn) {
            _serverConn->disconnect("superseded by listen()");
            _serverConn.reset();
        }
        // R1.A: clear any leftover server children from a previous listen.
        // R4.1: fire onConnectionDisconnected for each before dropping.
        for (size_t i = 0; i < _netConns.size(); ++i) {
            if (_extension && _netConns[i]) _extension->onConnectionDisconnected(_netConns[i].get());
        }
        for (auto& child : _serverClients) {
            if (child) child->disconnect("superseded by listen()");
        }
        _serverClients.clear();
        _netConns.clear();
        _nextNetId = 0;

        _serverConn = std::make_unique<GnsConnection>();
        _serverConn->initServer(port);
        // R4.1-A: adopt factory is registered only while listening so multiple
        // subsystem instances (e.g. integration tests) can initialize GNS
        // without clobbering the server's incoming-connection handler.
        GnsConnection::setAdoptFactory([this](HSteamNetConnection incoming) -> GnsConnection* {
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
        if (_clientConn) {
            _clientConn->disconnect("client disconnect");
            _clientConn.reset();
            _clientNetConn.reset();
        }
        if (_serverConn) {
            GnsConnection::setAdoptFactory(nullptr);
            _serverConn->disconnect("server shutdown");
            _serverConn.reset();
        }
        // R1.A: tear down all server children too. R4.1: fire
        // onConnectionDisconnected BEFORE reset so the extension sees
        // the conn in its valid state.
        for (auto& c : _netConns) {
            if (c && _extension) _extension->onConnectionDisconnected(c.get());
        }
        for (auto& child : _serverClients) {
            if (child) child->disconnect("server shutdown");
        }
        _serverClients.clear();
        _netConns.clear();
        _nextNetId = 0;
        _mode = ConnectionMode::Disconnected;
    }

    bool isConnected() const override {
        if (_clientConn && _clientConn->isConnected()) return true;
        if (_serverConn && _serverConn->isConnected()) return true;
        // R1.A: server is also "connected" if at least one client is connected.
        for (auto& child : _serverClients) {
            if (child && child->isConnected()) return true;
        }
        return false;
    }

    ConnectionMode getMode() const override {
        return _mode;
    }

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
            if (child && child->isConnected()) {
                child->send(channel, data, size);
            }
        }
    }

    void broadcastExcept(NetConnection* exclude, uint8_t channel, const void* data, size_t size) override {
        for (auto& netConn : _netConns) {
            if (!netConn || !netConn->isConnected()) continue;
            if (exclude && netConn.get() == exclude) continue;
            netConn->send(channel, data, size);
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
        // R4.1: rebuild on each call from _netConns (the owning vector).
        // R4.0 returned a permanently-empty _connections (never populated);
        // that's the bug. N≤100 → rebuild cost is negligible.
        _connections.clear();
        _connections.reserve(_netConns.size());
        for (auto& c : _netConns) {
            if (c) _connections.push_back(c.get());
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
    std::vector<std::unique_ptr<GnsConnection>> _serverClients;

    // R4.1: mirror of _serverClients that owns the NetConnectionImpl
    // adapters. Populated in adoptIncomingClient; cleared on listen()/disconnect().
    // _netConns is the source of truth for getConnections() (rebuilt into
    // _connections on each call to avoid stale-pointer bugs).
    std::vector<std::unique_ptr<NetConnectionImpl>> _netConns;
    uint32_t _nextNetId = 0;

    // Pending handler captured at onConnectionChange time so adoptIncomingClient()
    // can install it on each new server child.
    std::function<void(bool, DisconnectReason)> _pendingConnHandler;

    ReplicationManager _replicationManager{this};
    // R4.0 (2026-07-29): mirrors _replicationManager ownership. Routed by
    // onMessage via PacketHeader.msgType envelope kind.
    RpcHandler _rpcHandler{this};
    std::mutex _simulationInboundMutex;
    std::deque<PendingSimulationInbound> _simulationInbound;
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
