// AYNetworkSubSystem.cpp - 网络子系统实现

#include <AYNetwork.h>
#include <AYGameLoop/GameLoop.h>
#include <cstdio>

namespace ayt::net
{

// =============================================================================
// NetworkSubSystem - 子系统实现
// =============================================================================
class NetworkSubSystem : public INetworkSubSystem {
public:
    const char* getName() const override { return "Network"; }
    const SubSystemDescriptor& getDescriptor() const override {
        static SubSystemDescriptor desc = {
            .name = "Network",
            .dependencies = {},  // 无依赖
            .basePriority = 100  // 早期初始化
        };
        return desc;
    }

    bool initialize() override {
        ::printf("[Network] Initialized\n");
        return true;
    }

    void update(float deltaTime) override {
        // 驱动网络循环
    }

    void shutdown() override {
        disconnect();
        ::printf("[Network] Shutdown\n");
    }

    // ===== 连接管理 =====
    void connect(const char* address, uint16_t port) override {
        // TODO: 实现 TCP 连接
    }

    void listen(uint16_t port) override {
        // TODO: 实现 TCP 监听
    }

    void disconnect() override {
        // TODO: 断开连接
    }

    bool isConnected() const override {
        return _mode == ConnectionMode::Client && _connected;
    }

    ConnectionMode getMode() const override {
        return _mode;
    }

    // ===== 消息发送 =====
    void send(uint8_t channel, const void* data, size_t size) override {
        // TODO: 实现
    }

    void sendTo(NetConnection* conn, uint8_t channel, const void* data, size_t size) override {
        // TODO: 实现
    }

    void broadcast(uint8_t channel, const void* data, size_t size) override {
        // TODO: 实现
    }

    void broadcastExcept(NetConnection* exclude, uint8_t channel, const void* data, size_t size) override {
        // TODO: 实现
    }

    // ===== 消息接收 =====
    void onMessage(uint8_t channel, MessageHandler handler) override {
        _messageHandlers[channel] = handler;
    }

    // ===== 连接状态 =====
    void onConnectionChange(ConnectionHandler handler) override {
        _connectionHandler = handler;
    }

    // ===== 服务器专用 =====
    void setAcceptCallback(AcceptCallback callback) override {
        _acceptCallback = callback;
    }

    void kickConnection(NetConnection* conn, const char* reason) override {
        // TODO: 实现
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

    ReplicationManager _replicationManager{this};
};

// 注册宏
REGISTER_SUBSYSTEM(NetworkSubSystem, {}, 100);

} // namespace ayt::net