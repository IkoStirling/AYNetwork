// AYConnection.cpp - Connection state machine

#include <AYConnection.h>

namespace ayt::net
{

AYConnection::AYConnection()
    : _state(ConnectionState::Disconnected)
{
}

AYConnection::~AYConnection() {
    disconnect();
}

void AYConnection::initClient(const char* address, uint16_t port) {
    _address = address;
    _port = port;
    _socket.create();
}

void AYConnection::initServer(uint16_t port) {
    _port = port;
    _socket.create();
    _socket.setReuseAddr(true);
    _socket.bind(port);
    _socket.setNonBlocking(true);
}

int AYConnection::send(const void* data, size_t len) {
    if (_state != ConnectionState::Connected) {
        return -1;
    }
    return _kcp.send((const uint8_t*)data, len);
}

void AYConnection::update(uint32_t currentTime) {
    _kcp.update(currentTime);
}

void AYConnection::connect() {
    if (_state == ConnectionState::Disconnected) {
        setState(ConnectionState::Connecting);
    }
}

void AYConnection::disconnect(const char* reason) {
    if (_state != ConnectionState::Disconnected) {
        setState(ConnectionState::Disconnecting);
        setState(ConnectionState::Disconnected);
    }
}

void AYConnection::onStateChange(StateHandler handler) {
    _stateHandler = handler;
}

void AYConnection::onData(DataHandler handler) {
    _dataHandler = handler;
}

void AYConnection::setState(ConnectionState newState) {
    if (_state != newState) {
        ConnectionState oldState = _state;
        _state = newState;
        if (_stateHandler) {
            _stateHandler(oldState, newState);
        }
    }
}

} // namespace ayt::net