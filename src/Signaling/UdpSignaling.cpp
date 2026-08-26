// UdpSignaling.cpp - self-hosted rendezvous signaling implementation.

#include <AYNetwork/Signaling/UdpSignaling.h>
#include <AYNetwork/Transport/UdpSocket.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

namespace ayt::net
{
namespace
{

constexpr uint32_t kSignalMagic = 0x47535941u; // "AYSG" little-endian
constexpr uint8_t kSignalVersion = 1;
constexpr size_t kWireHeaderBytes = 16;
constexpr size_t kMaxPeerIdBytes = 63;

enum class SignalPacketType : uint8_t {
    Register = 1,
    Signal = 2,
    Heartbeat = 3,
    Unregister = 4,
};

struct SignalPacketView {
    SignalPacketType type{};
    PeerId from;
    PeerId to;
    const uint8_t* payload = nullptr;
    size_t payloadSize = 0;
};

uint64_t monotonicMs() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count());
}

void putU32(std::vector<uint8_t>& out, size_t offset, uint32_t value) {
    out[offset + 0] = static_cast<uint8_t>(value & 0xffu);
    out[offset + 1] = static_cast<uint8_t>((value >> 8) & 0xffu);
    out[offset + 2] = static_cast<uint8_t>((value >> 16) & 0xffu);
    out[offset + 3] = static_cast<uint8_t>((value >> 24) & 0xffu);
}

uint32_t getU32(const uint8_t* data, size_t offset) {
    return static_cast<uint32_t>(data[offset + 0]) |
           (static_cast<uint32_t>(data[offset + 1]) << 8) |
           (static_cast<uint32_t>(data[offset + 2]) << 16) |
           (static_cast<uint32_t>(data[offset + 3]) << 24);
}

bool encodePacket(SignalPacketType type,
                  const PeerId& from,
                  const PeerId& to,
                  const void* payload,
                  size_t payloadSize,
                  size_t maxDatagramBytes,
                  std::vector<uint8_t>& out) {
    if (!from.isValid() || from.value.size() > kMaxPeerIdBytes) return false;
    if (!to.value.empty() && (!to.isValid() || to.value.size() > kMaxPeerIdBytes)) return false;
    if ((!payload && payloadSize != 0) || payloadSize > UINT32_MAX) return false;
    const size_t total = kWireHeaderBytes + from.value.size() + to.value.size() + payloadSize;
    if (total > maxDatagramBytes || total > 65507u) return false;

    out.assign(total, 0);
    putU32(out, 0, kSignalMagic);
    out[4] = kSignalVersion;
    out[5] = static_cast<uint8_t>(type);
    out[6] = static_cast<uint8_t>(from.value.size());
    out[7] = static_cast<uint8_t>(to.value.size());
    putU32(out, 8, static_cast<uint32_t>(payloadSize));

    size_t cursor = kWireHeaderBytes;
    std::memcpy(out.data() + cursor, from.value.data(), from.value.size());
    cursor += from.value.size();
    std::memcpy(out.data() + cursor, to.value.data(), to.value.size());
    cursor += to.value.size();
    if (payloadSize != 0) std::memcpy(out.data() + cursor, payload, payloadSize);
    return true;
}

bool decodePacket(const uint8_t* data, size_t size, SignalPacketView& out) {
    if (!data || size < kWireHeaderBytes || getU32(data, 0) != kSignalMagic ||
        data[4] != kSignalVersion || getU32(data, 12) != 0) {
        return false;
    }
    const auto type = static_cast<SignalPacketType>(data[5]);
    if (type < SignalPacketType::Register || type > SignalPacketType::Unregister) return false;
    const size_t fromLen = data[6];
    const size_t toLen = data[7];
    const size_t payloadLen = getU32(data, 8);
    if (fromLen == 0 || fromLen > kMaxPeerIdBytes || toLen > kMaxPeerIdBytes) return false;
    const size_t expected = kWireHeaderBytes + fromLen + toLen + payloadLen;
    if (expected != size) return false;

    out.type = type;
    out.from.value.assign(reinterpret_cast<const char*>(data + kWireHeaderBytes), fromLen);
    out.to.value.assign(reinterpret_cast<const char*>(data + kWireHeaderBytes + fromLen), toLen);
    out.payload = data + kWireHeaderBytes + fromLen + toLen;
    out.payloadSize = payloadLen;
    if (!out.from.isValid() || (!out.to.value.empty() && !out.to.isValid())) return false;
    if (type == SignalPacketType::Signal && (out.to.value.empty() || payloadLen == 0)) return false;
    if (type != SignalPacketType::Signal && (toLen != 0 || payloadLen != 0)) return false;
    return true;
}

} // namespace

struct UdpSignalingClient::Impl {
    explicit Impl(UdpSignalingClientConfig c) : config(std::move(c)) {}

    bool sendPacket(SignalPacketType type, const PeerId& destination,
                    const void* data, size_t size) {
        std::vector<uint8_t> wire;
        if (!encodePacket(type, localPeer, destination, data, size,
                          config.maxDatagramBytes, wire)) {
            return false;
        }
        std::lock_guard<std::mutex> lock(sendMutex);
        if (!running.load(std::memory_order_acquire)) return false;
        return socket.sendTo(resolvedServerAddress.c_str(), config.serverPort,
                             wire.data(), wire.size()) == static_cast<int>(wire.size());
    }

    UdpSignalingClientConfig config;
    UdpSocket socket;
    PeerId localPeer;
    std::atomic<bool> running{false};
    std::mutex sendMutex;
    uint64_t lastHeartbeatMs = 0;
    std::vector<uint8_t> receiveBuffer;
    std::string resolvedServerAddress;
};

UdpSignalingClient::UdpSignalingClient(UdpSignalingClientConfig config)
    : _impl(std::make_unique<Impl>(std::move(config))) {}

UdpSignalingClient::~UdpSignalingClient() {
    stop();
}

bool UdpSignalingClient::start(const PeerId& localPeer) {
    if (!_impl || !localPeer.isValid() || _impl->config.serverAddress.empty() ||
        _impl->config.serverPort == 0 || _impl->config.maxDatagramBytes < kWireHeaderBytes + 1) {
        return false;
    }
    stop();
    if (!UdpSocket::resolveIPv4(_impl->config.serverAddress.c_str(),
                                _impl->resolvedServerAddress)) {
        return false;
    }
    if (!_impl->socket.create() || !_impl->socket.bind(0)) {
        _impl->socket.close();
        return false;
    }
    _impl->socket.setNonBlocking(true);
    _impl->localPeer = localPeer;
    _impl->receiveBuffer.resize(std::min<size_t>(_impl->config.maxDatagramBytes, 65507u));
    _impl->running.store(true, std::memory_order_release);
    _impl->lastHeartbeatMs = monotonicMs();
    if (!_impl->sendPacket(SignalPacketType::Register, {}, nullptr, 0)) {
        stop();
        return false;
    }
    return true;
}

void UdpSignalingClient::stop() {
    if (!_impl || !_impl->running.load(std::memory_order_acquire)) return;
    (void)_impl->sendPacket(SignalPacketType::Unregister, {}, nullptr, 0);
    {
        std::lock_guard<std::mutex> lock(_impl->sendMutex);
        _impl->running.store(false, std::memory_order_release);
        _impl->socket.close();
    }
    _impl->localPeer = {};
    _impl->receiveBuffer.clear();
    _impl->resolvedServerAddress.clear();
}

bool UdpSignalingClient::isRunning() const {
    return _impl && _impl->running.load(std::memory_order_acquire);
}

bool UdpSignalingClient::sendSignal(const PeerId& destination,
                                    const void* data, size_t size) {
    return _impl && destination.isValid() && size != 0 &&
           _impl->sendPacket(SignalPacketType::Signal, destination, data, size);
}

size_t UdpSignalingClient::poll(const ReceiveHandler& handler, size_t maxMessages) {
    if (!_impl || !isRunning() || !handler || maxMessages == 0) return 0;
    const uint64_t now = monotonicMs();
    if (_impl->config.heartbeatIntervalMs != 0 &&
        now - _impl->lastHeartbeatMs >= _impl->config.heartbeatIntervalMs) {
        (void)_impl->sendPacket(SignalPacketType::Heartbeat, {}, nullptr, 0);
        _impl->lastHeartbeatMs = now;
    }

    size_t delivered = 0;
    for (size_t i = 0; i < maxMessages; ++i) {
        char address[64]{};
        uint16_t port = 0;
        const int received = _impl->socket.receiveFrom(
            address, &port, _impl->receiveBuffer.data(), _impl->receiveBuffer.size());
        if (received <= 0) break;
        // The UDP backend is intentionally unauthenticated, but clients must
        // still reject packets injected directly by arbitrary endpoints.
        // Production backends additionally authenticate the peer envelope.
        if (_impl->resolvedServerAddress != address ||
            _impl->config.serverPort != port) continue;
        SignalPacketView packet;
        if (!decodePacket(_impl->receiveBuffer.data(), static_cast<size_t>(received), packet)) continue;
        if (packet.type != SignalPacketType::Signal || packet.to != _impl->localPeer) continue;
        handler(packet.from, packet.payload, packet.payloadSize);
        ++delivered;
    }
    return delivered;
}

struct UdpSignalingServer::Impl {
    struct Endpoint {
        std::string address;
        uint16_t port = 0;
        uint64_t lastSeenMs = 0;
    };

    explicit Impl(UdpSignalingServerConfig c) : config(std::move(c)) {}

    UdpSignalingServerConfig config;
    UdpSocket socket;
    bool running = false;
    std::unordered_map<std::string, Endpoint> peers;
    std::vector<uint8_t> receiveBuffer;
};

UdpSignalingServer::UdpSignalingServer(UdpSignalingServerConfig config)
    : _impl(std::make_unique<Impl>(std::move(config))) {}

UdpSignalingServer::~UdpSignalingServer() {
    stop();
}

bool UdpSignalingServer::start() {
    if (!_impl || _impl->config.maxPeers == 0 ||
        _impl->config.maxDatagramBytes < kWireHeaderBytes + 1) return false;
    stop();
    if (!_impl->socket.create() ||
        !_impl->socket.bind(_impl->config.bindAddress.c_str(), _impl->config.port)) {
        _impl->socket.close();
        return false;
    }
    _impl->socket.setNonBlocking(true);
    _impl->receiveBuffer.resize(std::min<size_t>(_impl->config.maxDatagramBytes, 65507u));
    _impl->running = true;
    return true;
}

void UdpSignalingServer::stop() {
    if (!_impl) return;
    _impl->running = false;
    _impl->socket.close();
    _impl->peers.clear();
    _impl->receiveBuffer.clear();
}

bool UdpSignalingServer::isRunning() const { return _impl && _impl->running; }
uint16_t UdpSignalingServer::getBoundPort() const {
    return isRunning() ? _impl->socket.getBoundPort() : 0;
}
size_t UdpSignalingServer::getPeerCount() const { return _impl ? _impl->peers.size() : 0; }

size_t UdpSignalingServer::pump(size_t maxMessages) {
    if (!isRunning() || maxMessages == 0) return 0;
    size_t processed = 0;
    for (size_t i = 0; i < maxMessages; ++i) {
        char address[64]{};
        uint16_t port = 0;
        const int received = _impl->socket.receiveFrom(
            address, &port, _impl->receiveBuffer.data(), _impl->receiveBuffer.size());
        if (received <= 0) break;
        SignalPacketView packet;
        if (!decodePacket(_impl->receiveBuffer.data(), static_cast<size_t>(received), packet)) continue;

        auto existing = _impl->peers.find(packet.from.value);
        const bool sameEndpoint = existing != _impl->peers.end() &&
            existing->second.address == address && existing->second.port == port;
        if (packet.type == SignalPacketType::Unregister) {
            if (sameEndpoint) _impl->peers.erase(existing);
            ++processed;
            continue;
        }
        // Only an explicit registration may move an existing peer id to a new
        // endpoint (e.g. after NAT rebinding). This prevents an arbitrary
        // signal/heartbeat datagram from silently hijacking a live route.
        if (existing != _impl->peers.end() && !sameEndpoint &&
            packet.type != SignalPacketType::Register) {
            continue;
        }
        if (existing == _impl->peers.end() && _impl->peers.size() >= _impl->config.maxPeers) {
            continue;
        }
        _impl->peers[packet.from.value] = {address, port, monotonicMs()};

        if (packet.type == SignalPacketType::Signal) {
            const auto target = _impl->peers.find(packet.to.value);
            if (target != _impl->peers.end()) {
                (void)_impl->socket.sendTo(target->second.address.c_str(), target->second.port,
                    _impl->receiveBuffer.data(), static_cast<size_t>(received));
            }
        }
        ++processed;
    }
    pruneExpired();
    return processed;
}

void UdpSignalingServer::pruneExpired() {
    if (!_impl || _impl->config.peerTimeoutMs == 0) return;
    const uint64_t now = monotonicMs();
    for (auto it = _impl->peers.begin(); it != _impl->peers.end();) {
        if (now - it->second.lastSeenMs >= _impl->config.peerTimeoutMs) {
            it = _impl->peers.erase(it);
        } else {
            ++it;
        }
    }
}

} // namespace ayt::net
