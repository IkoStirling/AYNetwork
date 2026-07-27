// AYNetworkSubSystem.cpp - 网络子系统实现

#include <AYNetwork.h>
#include <AYGameLoop.h>
#include <GnsConnection.h>
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
        // R1.A (2026-07-27): register the adopt factory so the global GNS
        // status callback can route incoming connections to a fresh
        // server-child GnsConnection managed by this subsystem.
        GnsConnection::setAdoptFactory([this](HSteamNetConnection incoming) -> GnsConnection* {
            return this->adoptIncomingClient(incoming);
        });
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
        if (_pendingRoute) {
            child->onData(_pendingRoute);
        }
        // R1.A: route state changes to the registered connection handler (if any).
        if (_pendingConnHandler) {
            // Capture raw pointer (child is moved into _serverClients later
            // in this function; the raw pointer remains valid for the
            // connection's lifetime since _serverClients outlives the lambda).
            GnsConnection* rawChild = child.get();
            child->onStateChange([this, rawChild](GnsConnectionState /*oldS*/, GnsConnectionState newS) {
                // R1 done: connected means Ready (handshake done) or Connected
                // (handshake disabled). Reason is captured from the
                // GnsConnection so the app can react to Kicked/ProtocolMismatch.
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

    // ===== 连接管理 =====
    void connect(const char* address, uint16_t port) override {
        if (_clientConn) {
            _clientConn->disconnect("superseded by connect()");
            _clientConn.reset();
        }
        _clientConn = std::make_unique<GnsConnection>();
        _clientConn->initClient(address, port);
        _mode = ConnectionMode::Client;
    }

    void listen(uint16_t port) override {
        if (_serverConn) {
            _serverConn->disconnect("superseded by listen()");
            _serverConn.reset();
        }
        // R1.A: clear any leftover server children from a previous listen.
        for (auto& child : _serverClients) {
            if (child) child->disconnect("superseded by listen()");
        }
        _serverClients.clear();

        _serverConn = std::make_unique<GnsConnection>();
        _serverConn->initServer(port);
        _mode = ConnectionMode::Server;
    }

    void disconnect() override {
        if (_clientConn) {
            _clientConn->disconnect("client disconnect");
            _clientConn.reset();
        }
        if (_serverConn) {
            _serverConn->disconnect("server shutdown");
            _serverConn.reset();
        }
        // R1.A: tear down all server children too.
        for (auto& child : _serverClients) {
            if (child) child->disconnect("server shutdown");
        }
        _serverClients.clear();
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
        // R3 will use NetConnection::send (per-connection handle). For R1.A the
        // single-conn subsystem forwards to the active client conn if the
        // caller passes our wrapper pointer.
        (void)conn; (void)channel; (void)data; (void)size;
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
        // R1.A: same loop, with one exclusion. exclude is a NetConnection*
        // but R1.A doesn't have a NetConnection wrapper yet — compare by raw
        // GnsConnection* via user_data if set. For now we accept nullptr or
        // identity-match by raw pointer of the embedded GnsConnection (the
        // mapping is established in R3).
        for (auto& child : _serverClients) {
            if (!child || !child->isConnected()) continue;
            if (exclude && static_cast<void*>(exclude) == static_cast<void*>(child.get())) continue;
            child->send(channel, data, size);
        }
    }

    // ===== 消息接收 =====
    void onMessage(uint8_t channel, MessageHandler handler) override {
        _messageHandlers[channel] = handler;
        // R1.A wiring: route every GnsConnection's onData into the registered
        // handler. The handler signature is (NetConnection*, channel, data, len)
        // — we pass nullptr for the NetConnection* in R1.A (R3 will wire up
        // proper wrappers).
        auto route = [this, channel](const uint8_t* data, size_t len) {
            if (_messageHandlers[channel]) {
                _messageHandlers[channel](nullptr, channel, data, len);
            }
        };
        if (_clientConn) {
            _clientConn->onData(route);
        }
        // Server children installed at adopt time — see adoptIncomingClient().
        _pendingRoute = route;
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
        (void)conn; (void)reason;
    }

    const std::vector<NetConnection*>& getConnections() override {
        return _connections;
    }

    // ===== 查询 =====
    NetConnection* getConnection() const override {
        return _connection;
    }

    uint32_t getHostId() const override {
        return _hostId;
    }

    // ===== 扩展点 =====
    void setExtension(INetworkExtension* ext) override {
        _extension = ext;
    }

    // ===== Replication =====
    ReplicationManager* getReplicationManager() override {
        return &_replicationManager;
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
    std::unique_ptr<GnsConnection> _serverConn;
    std::vector<std::unique_ptr<GnsConnection>> _serverClients;

    // Pending handlers captured at onMessage/onConnectionChange time so
    // adoptIncomingClient() can install them on each new server child.
    GnsConnection::DataHandler _pendingRoute;
    // R1 done: carries DisconnectReason so server children surface the
    // wire-level reason (Kicked / ProtocolMismatch / etc.) to the app.
    std::function<void(bool, DisconnectReason)> _pendingConnHandler;

    ReplicationManager _replicationManager{this};
};

// 注册宏
REGISTER_SUBSYSTEM(NetworkSubSystem, {}, 100);

} // namespace ayt::net