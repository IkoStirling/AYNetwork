#pragma once
// AYNetwork/Transport/AYNetwork/Transport/AYNetwork/Transport/UdpSocket.h - UDP socket wrapper

#include <AYCore.h>
#include <cstdint>
#include <string>

namespace ayt::net
{

// =============================================================================
// UdpSocket - UDP socket abstraction
// =============================================================================
class UdpSocket {
public:
    UdpSocket();
    ~UdpSocket();

    // Create socket
    bool create();
    void close();

    // Bind and listen
    bool bind(uint16_t port);
    bool bind(const char* address, uint16_t port);

    // Connect (for client)
    bool connect(const char* address, uint16_t port);

    // Send/Receive
    int sendTo(const char* address, uint16_t port, const void* data, size_t len);
    int receiveFrom(char* address, uint16_t* port, void* buf, size_t len);

    // Non-blocking
    void setNonBlocking(bool nonBlocking);
    bool isNonBlocking() const { return _nonBlocking; }

    // Socket options
    void setReuseAddr(bool reuse);
    void setBroadcast(bool broadcast);

    // Validity
    bool isValid() const { return _sockfd >= 0; }
    uint16_t getBoundPort() const;
    static bool resolveIPv4(const char* address, std::string& resolved);

    // Get native handle
    int getHandle() const { return _sockfd; }

private:
    int _sockfd = -1;
    bool _nonBlocking = false;
};

} // namespace ayt::net
