#pragma once
// AYConnection.h - Connection state machine

#include <AYCore.h>
#include <UdpSocket.h>
#include <KcpConnection.h>
#include <cstdint>
#include <functional>
#include <string>

namespace ayt::net
{

// =============================================================================
// Connection State
// =============================================================================
enum class ConnectionState : uint8_t {
    Disconnected,
    Connecting,
    Connected,
    Disconnecting
};

// =============================================================================
// AYConnection - Connection state machine with KCP transport
// =============================================================================
class AYConnection {
public:
    using StateHandler = std::function<void(ConnectionState oldState, ConnectionState newState)>;
    using DataHandler = std::function<void(const uint8_t* data, size_t len)>;

    AYConnection();
    ~AYConnection();

    // Initialize as client
    void initClient(const char* address, uint16_t port);

    // Initialize as server
    void initServer(uint16_t port);

    // State
    ConnectionState getState() const { return _state; }
    bool isConnected() const { return _state == ConnectionState::Connected; }

    // Send data through KCP
    int send(const void* data, size_t len);

    // Update (call every frame)
    void update(uint32_t currentTime);

    // Connect/Disconnect
    void connect();
    void disconnect(const char* reason = nullptr);

    // Callbacks
    void onStateChange(StateHandler handler);
    void onData(DataHandler handler);

    // Address
    const char* getAddress() const { return _address.c_str(); }
    uint16_t getPort() const { return _port; }

private:
    void setState(ConnectionState newState);

    ConnectionState _state = ConnectionState::Disconnected;
    std::string _address;
    uint16_t _port = 0;
    uint32_t _connectionId = 0;

    UdpSocket _socket;
    KcpConnection _kcp;

    StateHandler _stateHandler;
    DataHandler _dataHandler;
};

} // namespace ayt::net