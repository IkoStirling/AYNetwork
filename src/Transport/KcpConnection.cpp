// KcpConnection.cpp - KCP reliable connection wrapper

#include <AYNetwork/Transport/KcpConnection.h>

namespace ayt::net
{

KcpConnection::KcpConnection()
    : _kcp(nullptr)
    , _sockfd(-1)
    , _connected(false)
{
}

KcpConnection::~KcpConnection() {
    if (_kcp) {
        _kcp = nullptr;
    }
}

void KcpConnection::init(const KcpConfig& config) {
    _config = config;
}

int KcpConnection::send(const uint8_t* data, size_t len) {
    if (!_kcp) return -1;
    return -1;
}

int KcpConnection::receive(uint8_t* buf, size_t len) {
    if (!_kcp) return -1;
    return -1;
}

void KcpConnection::update(uint32_t currentTime) {
    if (_kcp) {
    }
}

bool KcpConnection::isConnected() const {
    return _connected;
}

int KcpConnection::getPing() const {
    return 0;
}

} // namespace ayt::net