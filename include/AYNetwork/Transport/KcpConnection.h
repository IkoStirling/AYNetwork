#pragma once
// AYNetwork/Transport/AYNetwork/Transport/AYNetwork/Transport/KcpConnection.h - KCP reliable connection wrapper

#include <AYCore.h>
#include <cstdint>

namespace ayt::net
{

// =============================================================================
// KCP Mode
// =============================================================================
enum class KcpMode : uint8_t {
    ReliableOrdered = 0,  // Reliable + ordered (like TCP)
    Unreliable = 1,       // Unreliable (like UDP)
    Ordered = 2           // Ordered but unreliable
};

// =============================================================================
// KCP Configuration
// =============================================================================
struct KcpConfig {
    uint32_t conv = 0;        // Connection ID
    uint32_t mtu = 1400;      // Maximum transmission unit
    uint32_t wndSize = 64;    // Window size
    uint32_t noDelay = 1;     // 0/1: normal/fast mode
    uint32_t interval = 10;   // Update interval (ms)
    uint32_t resend = 2;      // Fast resend
    uint32_t nc = 1;          // Congestion control switch
};

// =============================================================================
// KcpConnection - KCP connection wrapper
// =============================================================================
class KcpConnection {
public:
    KcpConnection();
    ~KcpConnection();

    void init(const KcpConfig& config);
    int send(const uint8_t* data, size_t len);
    int receive(uint8_t* buf, size_t len);
    void update(uint32_t currentTime);
    bool isConnected() const;
    int getPing() const;

    // Low-level KCP access
    void* getKcpHandle() const { return _kcp; }

private:
    void* _kcp = nullptr;
    int _sockfd = -1;
    KcpConfig _config;
    bool _connected = false;
};

} // namespace ayt::net