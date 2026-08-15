// NetConnectionImpl.cpp - GnsConnection → NetConnection 适配器实现 (R4.1)
//
// 类定义见 include/Transport/AYNetwork/Transport/AYNetwork/Transport/AYNetwork/Transport/NetConnectionImpl.h。本 TU 只做方法实现。

#include <AYNetwork/Transport/NetConnectionImpl.h>
#include <AYNetwork/Transport/GnsConnection.h>

namespace ayt::net
{

NetConnectionImpl::NetConnectionImpl(GnsConnection* gns, uint32_t netId)
    : _gns(gns)
    , _id(netId)
    , _hostId(gns ? gns->getInnerConnection() : 0)
    , _address(gns ? std::string(gns->getAddress()) : std::string{})
{}

bool NetConnectionImpl::isConnected() const {
    return _gns && _gns->isConnected();
}

int NetConnectionImpl::getPing() const {
    return _gns ? _gns->getPing() : 0;
}

void NetConnectionImpl::send(uint8_t channel, const void* data, size_t size) {
    if (_gns) _gns->send(channel, data, size);
}

void NetConnectionImpl::disconnect(const char* reason /*= nullptr*/) {
    if (_gns) _gns->disconnect(reason);
}

} // namespace ayt::net
