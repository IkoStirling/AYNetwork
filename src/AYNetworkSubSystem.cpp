// AYNetworkSubSystem.cpp - 网络子系统实现

#include <AYNetwork.h>
#include <AYNetworkModule.h>
#include <AYGameLoop.h>
#include <AYSubSystemRegistry.h>
#include <GnsConnection.h>
#include <PacketCodec.h>
#include <RPC/RpcHandler.h>
#include <Transport/NetConnectionImpl.h>
// R1.A (2026-07-27): pull in EResult + AcceptConnection signature. The
// GnsConnection.cpp TU-private includes are sufficient because s_gns is a
// fully-typed pointer in this TU — we just need the constants.
#include <steam/steamclientpublic.h>
#include <steam/isteamnetworkingsockets.h>
#include <cstdio>
#include <memory>

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
            .basePriority = 100  // 早期初始化
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

    // P0 audit fix (2026-07-26): ISubSystem requires fixedUpdate(float). Network
    // doesn't need a separate fixed-rate update path — forward to update() so
    // fixed-timestep clients (physics-coupled) get the same behaviour.
    void fixedUpdate(float fixedDeltaTime) override {
        update(fixedDeltaTime);
    }

    void shutdown() override {
        disconnect();
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

        if (_pendingConnHandler) {
            rawChild->onStateChange([this, rawChild](GnsConnectionState /*oldS*/, GnsConnectionState newS) {
                bool connected = (newS == GnsConnectionState::Ready) ||
                                 (newS == GnsConnectionState::Connected &&
                                  rawChild->getProtocolVersion() == 0);
                DisconnectReason reason = connected
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
        BitStream bodyStream(decoded.body.data(), decoded.body.size());
        switch (decoded.header.msgType) {
            case kMsgTypeRpcRequest:
                bodyStream.resetForRead();
                _rpcHandler.onRpcRequest(bodyStream, from);
                return;
            case kMsgTypeRpcResponse:
                bodyStream.resetForRead();
                _rpcHandler.onRpcResponse(bodyStream, from);
                return;
            case kMsgTypeRpcReject:
                bodyStream.resetForRead();
                _rpcHandler.onRpcReject(bodyStream, from);
                return;
            case kMsgTypeReplication:
            case kMsgTypeDelta:
            case kMsgTypeEntitySpawn:
            case kMsgTypeEntityDespawn:
                bodyStream.resetForRead();
                _replicationManager.onReceive(bodyStream, from);
                return;
            default:
                if (_messageHandlers[channel]) {
                    _messageHandlers[channel](from, channel, data, len);
                }
                return;
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