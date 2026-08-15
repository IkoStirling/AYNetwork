// UdpSocket.cpp - UDP socket wrapper

#include <AYNetwork/Transport/UdpSocket.h>
#include <atomic>
#include <cstring>

#if defined(AYT_WINDOWS)
    #include <winsock2.h>
    #include <ws2tcpip.h>
    typedef int socklen_t;
#else
    #include <sys/socket.h>
    #include <netinet/in.h>
    #include <arpa/inet.h>
    #include <unistd.h>
    #include <fcntl.h>
    #include <errno.h>
#endif

namespace ayt::net
{

// =============================================================================
// Winsock lifecycle (Windows only)
// =============================================================================
// WSACleanup is ref-counted: first socket to call WSAStartup bumps the counter,
// last socket to close calls WSACleanup. This avoids the bug where one socket
// closing would tear down Winsock for every other live socket in the process.
//
// P0 audit fix (2026-07-26): original code called WSACleanup on every close(),
// which breaks multi-socket scenarios (e.g. server with N client connections).
#if defined(AYT_WINDOWS)
namespace {
    std::atomic<uint32_t> g_wsaRefCount{0};

    void wsaAcquire() {
        if (g_wsaRefCount.fetch_add(1) == 0) {
            WSADATA wsaData;
            WSAStartup(MAKEWORD(2, 2), &wsaData);
        }
    }

    void wsaRelease() {
        if (g_wsaRefCount.fetch_sub(1) == 1) {
            WSACleanup();
        }
    }
}
#endif

UdpSocket::UdpSocket()
    : _sockfd(-1)
    , _nonBlocking(false)
{
}

UdpSocket::~UdpSocket() {
    close();
}

bool UdpSocket::create() {
#if defined(AYT_WINDOWS)
    wsaAcquire();
#endif

    _sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (_sockfd < 0) {
#if defined(AYT_WINDOWS)
        wsaRelease();
#endif
        return false;
    }
    return true;
}

void UdpSocket::close() {
    if (_sockfd >= 0) {
#if defined(AYT_WINDOWS)
        closesocket(_sockfd);
        wsaRelease();
#else
        ::close(_sockfd);
#endif
        _sockfd = -1;
    }
}

bool UdpSocket::bind(uint16_t port) {
    return bind("0.0.0.0", port);
}

bool UdpSocket::bind(const char* address, uint16_t port) {
    struct sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, address, &addr.sin_addr);

    if (::bind(_sockfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        return false;
    }
    return true;
}

bool UdpSocket::connect(const char* address, uint16_t port) {
    struct sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, address, &addr.sin_addr);

    if (::connect(_sockfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        return false;
    }
    return true;
}

int UdpSocket::sendTo(const char* address, uint16_t port, const void* data, size_t len) {
    struct sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, address, &addr.sin_addr);

    return sendto(_sockfd, (const char*)data, (int)len, 0,
                  (struct sockaddr*)&addr, sizeof(addr));
}

int UdpSocket::receiveFrom(char* address, uint16_t* port, void* buf, size_t len) {
    struct sockaddr_in addr;
    socklen_t addrLen = sizeof(addr);
    std::memset(&addr, 0, sizeof(addr));

    int received = recvfrom(_sockfd, (char*)buf, (int)len, 0,
                           (struct sockaddr*)&addr, &addrLen);

    if (received > 0) {
        if (address) {
            inet_ntop(AF_INET, &addr.sin_addr, address, INET_ADDRSTRLEN);
        }
        if (port) {
            *port = ntohs(addr.sin_port);
        }
    }
    return received;
}

void UdpSocket::setNonBlocking(bool nonBlocking) {
    _nonBlocking = nonBlocking;
#if defined(AYT_WINDOWS)
    u_long mode = nonBlocking ? 1 : 0;
    ioctlsocket(_sockfd, FIONBIO, &mode);
#else
    int flags = fcntl(_sockfd, F_GETFL, 0);
    if (nonBlocking) {
        fcntl(_sockfd, F_SETFL, flags | O_NONBLOCK);
    } else {
        fcntl(_sockfd, F_SETFL, flags & ~O_NONBLOCK);
    }
#endif
}

void UdpSocket::setReuseAddr(bool reuse) {
    int optval = reuse ? 1 : 0;
    setsockopt(_sockfd, SOL_SOCKET, SO_REUSEADDR, (const char*)&optval, sizeof(optval));
}

void UdpSocket::setBroadcast(bool broadcast) {
    int optval = broadcast ? 1 : 0;
    setsockopt(_sockfd, SOL_SOCKET, SO_BROADCAST, (const char*)&optval, sizeof(optval));
}

} // namespace ayt::net