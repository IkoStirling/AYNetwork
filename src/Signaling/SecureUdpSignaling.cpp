// SecureUdpSignaling.cpp - authenticated, replay-resistant room signaling.

#include <AYNetwork/Signaling/SecureUdpSignaling.h>
#include <AYNetwork/Transport/UdpSocket.h>
#include <AYCrypto.h>
#include "SecureSignalingProtocol.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <deque>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

namespace ayt::net
{
namespace
{

constexpr uint32_t kSecureMagic = 0x32535941u; // "AYS2" little-endian
constexpr uint8_t kSecureVersion = 2;
constexpr size_t kHeaderBytes = 40;
constexpr size_t kTagBytes = 32;
constexpr size_t kMaxIdBytes = 63;

enum class PacketType : uint8_t {
    Register = 1,
    RegisterChallenge = 2,
    RegisterConfirm = 3,
    RegisterAck = 4,
    Signal = 5,
    Heartbeat = 6,
    HeartbeatAck = 7,
    Unregister = 8,
    Error = 9,
};

struct PacketView {
    PacketType type{};
    PeerId from;
    PeerId to;
    SignalingRoomId room;
    uint64_t clientNonce = 0;
    uint64_t serverNonce = 0;
    uint64_t sequence = 0;
    const uint8_t* payload = nullptr;
    size_t payloadSize = 0;
    const uint8_t* tag = nullptr;
    size_t authenticatedSize = 0;
};

uint64_t monotonicMs() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count());
}

uint64_t unixSeconds() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}

void putU32(std::vector<uint8_t>& out, size_t offset, uint32_t value) {
    for (size_t i = 0; i < 4; ++i) out[offset + i] = static_cast<uint8_t>(value >> (i * 8));
}

void putU64(std::vector<uint8_t>& out, size_t offset, uint64_t value) {
    for (size_t i = 0; i < 8; ++i) out[offset + i] = static_cast<uint8_t>(value >> (i * 8));
}

uint32_t getU32(const uint8_t* data, size_t offset) {
    uint32_t value = 0;
    for (size_t i = 0; i < 4; ++i) value |= static_cast<uint32_t>(data[offset + i]) << (i * 8);
    return value;
}

uint64_t getU64(const uint8_t* data, size_t offset) {
    uint64_t value = 0;
    for (size_t i = 0; i < 8; ++i) value |= static_cast<uint64_t>(data[offset + i]) << (i * 8);
    return value;
}

bool computeTag(const SignalingToken& token,
                const uint8_t* data, size_t size,
                uint8_t* output) {
    if (!token.isValid() || !data || !output) return false;
    auto hmac = ayt::crypto::createHMAC(ayt::crypto::HashAlgo::SHA256);
    if (!hmac || !hmac->setKey(token.bytes.data(), token.bytes.size())) return false;
    hmac->init();
    hmac->update(data, size);
    hmac->final(output);
    return true;
}

bool verifyTag(const SignalingToken& token,
               const uint8_t* data, size_t size,
               const uint8_t* tag) {
    uint8_t expected[kTagBytes]{};
    return computeTag(token, data, size, expected) &&
           ayt::crypto::secureMemCompare(expected, tag, kTagBytes) == 0;
}

bool encodePacket(PacketType type,
                  const PeerId& from,
                  const PeerId& to,
                  const SignalingRoomId& room,
                  uint64_t clientNonce,
                  uint64_t serverNonce,
                  uint64_t sequence,
                  const void* payload,
                  size_t payloadSize,
                  const SignalingToken& token,
                  size_t maxDatagramBytes,
                  std::vector<uint8_t>& out) {
    if (!from.isValid() || !room.isValid() || !token.isValid() || sequence == 0 ||
        (!to.value.empty() && !to.isValid()) || (!payload && payloadSize != 0) ||
        from.value.size() > kMaxIdBytes || to.value.size() > kMaxIdBytes ||
        room.value.size() > kMaxIdBytes || payloadSize > UINT32_MAX) return false;
    const size_t authenticated = kHeaderBytes + from.value.size() + to.value.size() +
                                 room.value.size() + payloadSize;
    const size_t total = authenticated + kTagBytes;
    if (total > maxDatagramBytes || total > 65507u) return false;

    out.assign(total, 0);
    putU32(out, 0, kSecureMagic);
    out[4] = kSecureVersion;
    out[5] = static_cast<uint8_t>(type);
    out[6] = static_cast<uint8_t>(from.value.size());
    out[7] = static_cast<uint8_t>(to.value.size());
    out[8] = static_cast<uint8_t>(room.value.size());
    putU32(out, 12, static_cast<uint32_t>(payloadSize));
    putU64(out, 16, clientNonce);
    putU64(out, 24, serverNonce);
    putU64(out, 32, sequence);
    size_t cursor = kHeaderBytes;
    std::memcpy(out.data() + cursor, from.value.data(), from.value.size());
    cursor += from.value.size();
    std::memcpy(out.data() + cursor, to.value.data(), to.value.size());
    cursor += to.value.size();
    std::memcpy(out.data() + cursor, room.value.data(), room.value.size());
    cursor += room.value.size();
    if (payloadSize != 0) std::memcpy(out.data() + cursor, payload, payloadSize);
    return computeTag(token, out.data(), authenticated, out.data() + authenticated);
}

bool decodePacket(const uint8_t* data, size_t size, PacketView& out) {
    if (!data || size < kHeaderBytes + kTagBytes || getU32(data, 0) != kSecureMagic ||
        data[4] != kSecureVersion || data[9] != 0 || data[10] != 0 || data[11] != 0) {
        return false;
    }
    const auto type = static_cast<PacketType>(data[5]);
    if (type < PacketType::Register || type > PacketType::Error) return false;
    const size_t fromLen = data[6];
    const size_t toLen = data[7];
    const size_t roomLen = data[8];
    const size_t payloadLen = getU32(data, 12);
    if (fromLen == 0 || fromLen > kMaxIdBytes || toLen > kMaxIdBytes ||
        roomLen == 0 || roomLen > kMaxIdBytes) return false;
    const size_t authenticated = kHeaderBytes + fromLen + toLen + roomLen + payloadLen;
    if (authenticated + kTagBytes != size) return false;

    out.type = type;
    out.clientNonce = getU64(data, 16);
    out.serverNonce = getU64(data, 24);
    out.sequence = getU64(data, 32);
    size_t cursor = kHeaderBytes;
    out.from.value.assign(reinterpret_cast<const char*>(data + cursor), fromLen);
    cursor += fromLen;
    out.to.value.assign(reinterpret_cast<const char*>(data + cursor), toLen);
    cursor += toLen;
    out.room.value.assign(reinterpret_cast<const char*>(data + cursor), roomLen);
    cursor += roomLen;
    out.payload = data + cursor;
    out.payloadSize = payloadLen;
    out.authenticatedSize = authenticated;
    out.tag = data + authenticated;
    if (!out.from.isValid() || !out.room.isValid() || out.sequence == 0 ||
        (!out.to.value.empty() && !out.to.isValid())) return false;
    if (out.clientNonce == 0 ||
        (type != PacketType::Register && out.serverNonce == 0)) return false;
    if (type == PacketType::Signal && (out.to.value.empty() || payloadLen == 0)) return false;
    if (type != PacketType::Signal && type != PacketType::Error &&
        (!out.to.value.empty() || payloadLen != 0)) return false;
    return true;
}

uint64_t randomNonZeroU64() {
    uint64_t value = 0;
    while (value == 0) ayt::crypto::generateRandomBytes(reinterpret_cast<uint8_t*>(&value), sizeof(value));
    return value;
}

SecureSignalingError decodeError(const PacketView& packet) {
    if (packet.payloadSize != 1) return SecureSignalingError::ProtocolError;
    const auto value = static_cast<SecureSignalingError>(packet.payload[0]);
    return value <= SecureSignalingError::ProtocolError
        ? value : SecureSignalingError::ProtocolError;
}

} // namespace

bool SignalingRoomId::isValid() const {
    if (value.empty() || value.size() > kMaxIdBytes) return false;
    for (unsigned char ch : value) {
        const bool alphaNum = (ch >= 'a' && ch <= 'z') ||
                              (ch >= 'A' && ch <= 'Z') ||
                              (ch >= '0' && ch <= '9');
        if (!alphaNum && ch != '-' && ch != '_' && ch != '.' && ch != ':') return false;
    }
    return true;
}

bool SignalingToken::isValid() const {
    uint8_t any = 0;
    for (uint8_t byte : bytes) any |= byte;
    return any != 0;
}

bool parseSignalingTokenHex(const std::string& text, SignalingToken& token) {
    if (text.size() != SignalingToken::kSize * 2) return false;
    SignalingToken parsed;
    const auto nibble = [](char ch) -> int {
        if (ch >= '0' && ch <= '9') return ch - '0';
        if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
        if (ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
        return -1;
    };
    for (size_t i = 0; i < parsed.bytes.size(); ++i) {
        const int hi = nibble(text[i * 2]);
        const int lo = nibble(text[i * 2 + 1]);
        if (hi < 0 || lo < 0) return false;
        parsed.bytes[i] = static_cast<uint8_t>((hi << 4) | lo);
    }
    if (!parsed.isValid()) return false;
    token = parsed;
    return true;
}

std::string signalingTokenToHex(const SignalingToken& token) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string result(token.bytes.size() * 2, '0');
    for (size_t i = 0; i < token.bytes.size(); ++i) {
        result[i * 2] = kHex[token.bytes[i] >> 4];
        result[i * 2 + 1] = kHex[token.bytes[i] & 0x0f];
    }
    return result;
}

struct SecureUdpSignalingClient::Impl {
    struct PendingSignal {
        PeerId destination;
        std::vector<uint8_t> payload;
    };

    explicit Impl(SecureUdpSignalingClientConfig c) : config(std::move(c)) {}

    bool sendPacketLocked(PacketType type, const PeerId& destination,
                          const void* data, size_t size, bool registration = false) {
        std::vector<uint8_t> wire;
        const uint64_t packetServerNonce = registration ? 0 : serverNonce;
        if (!encodePacket(type, localPeer, destination, config.roomId,
                          clientNonce, packetServerNonce, nextSequence++, data, size,
                          config.token, config.maxDatagramBytes, wire)) return false;
        return socket.sendTo(resolvedServerAddress.c_str(), config.serverPort,
                             wire.data(), wire.size()) == static_cast<int>(wire.size());
    }

    void flushPendingLocked() {
        while (ready && !pending.empty()) {
            auto& item = pending.front();
            if (!sendPacketLocked(PacketType::Signal, item.destination,
                                  item.payload.data(), item.payload.size())) break;
            pending.pop_front();
        }
    }

    SecureUdpSignalingClientConfig config;
    UdpSocket socket;
    PeerId localPeer;
    std::string resolvedServerAddress;
    std::vector<uint8_t> receiveBuffer;
    std::deque<PendingSignal> pending;
    mutable std::mutex mutex;
    std::atomic<bool> running{false};
    std::atomic<SecureSignalingState> state{SecureSignalingState::Stopped};
    std::atomic<SecureSignalingError> lastError{SecureSignalingError::None};
    bool ready = false;
    uint64_t clientNonce = 0;
    uint64_t serverNonce = 0;
    uint64_t nextSequence = 1;
    uint64_t lastRegistrationMs = 0;
    uint64_t lastHeartbeatMs = 0;
    uint64_t lastServerMessageMs = 0;
    detail::SignalingReplayWindow inboundReplay;
};

SecureUdpSignalingClient::SecureUdpSignalingClient(SecureUdpSignalingClientConfig config)
    : _impl(std::make_unique<Impl>(std::move(config))) {}

SecureUdpSignalingClient::~SecureUdpSignalingClient() {
    stop();
    if (_impl) {
        ayt::crypto::secureZero(
            _impl->config.token.bytes.data(), _impl->config.token.bytes.size());
    }
}

bool SecureUdpSignalingClient::start(const PeerId& localPeer) {
    if (!_impl) return false;
    stop();
    if (!localPeer.isValid() || !_impl->config.roomId.isValid() ||
        !_impl->config.token.isValid() || _impl->config.serverAddress.empty() ||
        _impl->config.serverPort == 0 ||
        _impl->config.maxDatagramBytes < kHeaderBytes + kTagBytes + 2 ||
        _impl->config.maxPendingSignals == 0) {
        _impl->lastError.store(SecureSignalingError::InvalidConfig);
        _impl->state.store(SecureSignalingState::Failed);
        return false;
    }
    if (!UdpSocket::resolveIPv4(_impl->config.serverAddress.c_str(),
                                _impl->resolvedServerAddress) ||
        !_impl->socket.create() || !_impl->socket.bind(0)) {
        _impl->socket.close();
        _impl->lastError.store(SecureSignalingError::SocketFailure);
        _impl->state.store(SecureSignalingState::Failed);
        return false;
    }
    _impl->socket.setNonBlocking(true);
    std::lock_guard<std::mutex> lock(_impl->mutex);
    _impl->localPeer = localPeer;
    _impl->receiveBuffer.resize(std::min<size_t>(_impl->config.maxDatagramBytes, 65507u));
    _impl->clientNonce = randomNonZeroU64();
    _impl->serverNonce = 0;
    _impl->nextSequence = 1;
    _impl->ready = false;
    _impl->pending.clear();
    _impl->inboundReplay.reset();
    _impl->running.store(true, std::memory_order_release);
    _impl->state.store(SecureSignalingState::Registering);
    _impl->lastError.store(SecureSignalingError::None);
    const uint64_t now = monotonicMs();
    _impl->lastRegistrationMs = now;
    _impl->lastHeartbeatMs = now;
    if (!_impl->sendPacketLocked(PacketType::Register, {}, nullptr, 0, true)) {
        _impl->running.store(false, std::memory_order_release);
        _impl->state.store(SecureSignalingState::Failed);
        _impl->lastError.store(SecureSignalingError::SocketFailure);
        _impl->socket.close();
        return false;
    }
    return true;
}

void SecureUdpSignalingClient::stop() {
    if (!_impl || !_impl->running.load(std::memory_order_acquire)) return;
    std::lock_guard<std::mutex> lock(_impl->mutex);
    if (_impl->ready) (void)_impl->sendPacketLocked(PacketType::Unregister, {}, nullptr, 0);
    _impl->running.store(false, std::memory_order_release);
    _impl->ready = false;
    _impl->state.store(SecureSignalingState::Stopped);
    _impl->socket.close();
    _impl->pending.clear();
    _impl->receiveBuffer.clear();
    _impl->localPeer = {};
    _impl->resolvedServerAddress.clear();
}

bool SecureUdpSignalingClient::isRunning() const {
    return _impl && _impl->running.load(std::memory_order_acquire);
}

bool SecureUdpSignalingClient::sendSignal(const PeerId& destination,
                                          const void* data, size_t size) {
    if (!_impl || !destination.isValid() || !data || size == 0 ||
        size > _impl->config.maxDatagramBytes) return false;
    std::lock_guard<std::mutex> lock(_impl->mutex);
    if (!isRunning()) return false;
    if (!_impl->ready) {
        if (_impl->pending.size() >= _impl->config.maxPendingSignals) {
            _impl->lastError.store(SecureSignalingError::QueueFull);
            return false;
        }
        const auto* bytes = static_cast<const uint8_t*>(data);
        _impl->pending.push_back({destination, {bytes, bytes + size}});
        return true;
    }
    return _impl->sendPacketLocked(PacketType::Signal, destination, data, size);
}

size_t SecureUdpSignalingClient::poll(const ReceiveHandler& handler, size_t maxMessages) {
    if (!_impl || !isRunning() || !handler || maxMessages == 0) return 0;
    const uint64_t now = monotonicMs();
    {
        std::lock_guard<std::mutex> lock(_impl->mutex);
        if (_impl->config.registrationRefreshMs != 0 &&
            now - _impl->lastRegistrationMs >= _impl->config.registrationRefreshMs) {
            (void)_impl->sendPacketLocked(PacketType::Register, {}, nullptr, 0, true);
            _impl->lastRegistrationMs = now;
        } else if (_impl->ready && _impl->config.heartbeatIntervalMs != 0 &&
                   now - _impl->lastHeartbeatMs >= _impl->config.heartbeatIntervalMs) {
            (void)_impl->sendPacketLocked(PacketType::Heartbeat, {}, nullptr, 0);
            _impl->lastHeartbeatMs = now;
        }
    }

    size_t delivered = 0;
    for (size_t i = 0; i < maxMessages; ++i) {
        char address[64]{};
        uint16_t port = 0;
        const int received = _impl->socket.receiveFrom(
            address, &port, _impl->receiveBuffer.data(), _impl->receiveBuffer.size());
        if (received <= 0) break;
        if (_impl->resolvedServerAddress != address || _impl->config.serverPort != port) continue;
        PacketView packet;
        if (!decodePacket(_impl->receiveBuffer.data(), static_cast<size_t>(received), packet)) {
            _impl->lastError.store(SecureSignalingError::ProtocolError);
            continue;
        }
        if (packet.room != _impl->config.roomId ||
            !verifyTag(_impl->config.token, _impl->receiveBuffer.data(),
                       packet.authenticatedSize, packet.tag)) {
            _impl->lastError.store(SecureSignalingError::AuthenticationFailed);
            continue;
        }

        bool shouldDeliver = false;
        {
            std::lock_guard<std::mutex> lock(_impl->mutex);
            if (packet.clientNonce != _impl->clientNonce) continue;
            if (packet.type == PacketType::RegisterChallenge) {
                if (packet.from != _impl->localPeer || packet.serverNonce == 0) continue;
                if (_impl->serverNonce != packet.serverNonce) {
                    _impl->serverNonce = packet.serverNonce;
                    _impl->inboundReplay.reset();
                }
                if (!_impl->inboundReplay.accept(packet.sequence)) continue;
                _impl->ready = false;
                _impl->state.store(SecureSignalingState::Registering);
                if (!_impl->sendPacketLocked(
                        PacketType::RegisterConfirm, {}, nullptr, 0)) {
                    _impl->lastError.store(SecureSignalingError::SocketFailure);
                }
                continue;
            }
            if (packet.type == PacketType::RegisterAck) {
                if (packet.from != _impl->localPeer || packet.serverNonce == 0) continue;
                if (_impl->serverNonce != packet.serverNonce) {
                    _impl->serverNonce = packet.serverNonce;
                    _impl->inboundReplay.reset();
                }
                if (!_impl->inboundReplay.accept(packet.sequence)) continue;
                _impl->ready = true;
                _impl->state.store(SecureSignalingState::Ready);
                _impl->lastError.store(SecureSignalingError::None);
                _impl->lastServerMessageMs = now;
                _impl->flushPendingLocked();
                continue;
            }
            if (!_impl->ready || packet.serverNonce != _impl->serverNonce ||
                !_impl->inboundReplay.accept(packet.sequence)) continue;
            _impl->lastServerMessageMs = now;
            if (packet.type == PacketType::Error) {
                _impl->lastError.store(decodeError(packet));
                continue;
            }
            if (packet.type == PacketType::HeartbeatAck) continue;
            shouldDeliver = packet.type == PacketType::Signal &&
                            packet.to == _impl->localPeer && packet.from.isValid();
        }
        if (shouldDeliver) {
            handler(packet.from, packet.payload, packet.payloadSize);
            ++delivered;
        }
    }
    return delivered;
}

SecureSignalingState SecureUdpSignalingClient::getState() const {
    return _impl ? _impl->state.load() : SecureSignalingState::Stopped;
}

SecureSignalingError SecureUdpSignalingClient::getLastError() const {
    return _impl ? _impl->lastError.load() : SecureSignalingError::InvalidConfig;
}

bool SecureUdpSignalingClient::isReady() const {
    return getState() == SecureSignalingState::Ready;
}

size_t SecureUdpSignalingClient::getPendingSignalCount() const {
    if (!_impl) return 0;
    std::lock_guard<std::mutex> lock(_impl->mutex);
    return _impl->pending.size();
}

struct SecureUdpSignalingServer::Impl {
    struct Endpoint {
        std::string address;
        uint16_t port = 0;
        SignalingRoomId room;
        SignalingToken token;
        uint64_t expiresAtUnixSeconds = 0;
        uint64_t clientNonce = 0;
        uint64_t serverNonce = 0;
        uint64_t nextOutboundSequence = 1;
        uint64_t lastSeenMs = 0;
        uint64_t lastCredentialCheckMs = 0;
        detail::SignalingReplayWindow inboundReplay;
        uint64_t rateWindowStartMs = 0;
        uint32_t ratePackets = 0;
        size_t rateBytes = 0;
    };

    struct PendingRegistration {
        std::string address;
        uint16_t port = 0;
        SignalingRoomId room;
        SignalingToken token;
        uint64_t expiresAtUnixSeconds = 0;
        uint64_t clientNonce = 0;
        uint64_t serverNonce = 0;
        uint64_t nextOutboundSequence = 1;
        uint64_t createdMs = 0;
        detail::SignalingReplayWindow inboundReplay;
    };

    explicit Impl(SecureUdpSignalingServerConfig c) : config(std::move(c)) {}

    bool sendToEndpoint(PacketType type, const PeerId& from, const PeerId& to,
                        const void* data, size_t size, Endpoint& endpoint) {
        std::vector<uint8_t> wire;
        if (!encodePacket(type, from, to, endpoint.room,
                          endpoint.clientNonce, endpoint.serverNonce,
                          endpoint.nextOutboundSequence++, data, size,
                          endpoint.token, config.maxDatagramBytes, wire)) return false;
        return socket.sendTo(endpoint.address.c_str(), endpoint.port,
                             wire.data(), wire.size()) == static_cast<int>(wire.size());
    }

    void sendError(const PeerId& peer, Endpoint& endpoint, SecureSignalingError error) {
        const uint8_t value = static_cast<uint8_t>(error);
        (void)sendToEndpoint(PacketType::Error, peer, {}, &value, 1, endpoint);
    }

    bool sendChallenge(const PeerId& peer, PendingRegistration& pending) {
        std::vector<uint8_t> wire;
        if (!encodePacket(PacketType::RegisterChallenge, peer, {}, pending.room,
                          pending.clientNonce, pending.serverNonce,
                          pending.nextOutboundSequence++, nullptr, 0,
                          pending.token, config.maxDatagramBytes, wire)) return false;
        return socket.sendTo(pending.address.c_str(), pending.port,
                             wire.data(), wire.size()) == static_cast<int>(wire.size());
    }

    bool withinRate(Endpoint& endpoint, size_t bytes, uint64_t now) {
        if (now - endpoint.rateWindowStartMs >= 1000) {
            endpoint.rateWindowStartMs = now;
            endpoint.ratePackets = 0;
            endpoint.rateBytes = 0;
        }
        if ((config.maxPacketsPerPeerPerSecond != 0 &&
             endpoint.ratePackets >= config.maxPacketsPerPeerPerSecond) ||
            (config.maxBytesPerPeerPerSecond != 0 &&
             endpoint.rateBytes + bytes > config.maxBytesPerPeerPerSecond)) return false;
        ++endpoint.ratePackets;
        endpoint.rateBytes += bytes;
        return true;
    }

    SecureUdpSignalingServerConfig config;
    UdpSocket socket;
    bool running = false;
    std::unordered_map<std::string, Endpoint> peers;
    std::unordered_map<std::string, PendingRegistration> pendingRegistrations;
    std::vector<uint8_t> receiveBuffer;
    SecureSignalingServerStats stats;
};

SecureUdpSignalingServer::SecureUdpSignalingServer(SecureUdpSignalingServerConfig config)
    : _impl(std::make_unique<Impl>(std::move(config))) {}

SecureUdpSignalingServer::~SecureUdpSignalingServer() { stop(); }

bool SecureUdpSignalingServer::start() {
    if (!_impl || !_impl->config.resolveCredential || _impl->config.maxPeers == 0 ||
        _impl->config.maxDatagramBytes < kHeaderBytes + kTagBytes + 2) return false;
    stop();
    if (!_impl->socket.create() ||
        !_impl->socket.bind(_impl->config.bindAddress.c_str(), _impl->config.port)) {
        _impl->socket.close();
        return false;
    }
    _impl->socket.setNonBlocking(true);
    _impl->receiveBuffer.resize(std::min<size_t>(_impl->config.maxDatagramBytes, 65507u));
    _impl->stats = {};
    _impl->running = true;
    return true;
}

void SecureUdpSignalingServer::stop() {
    if (!_impl) return;
    _impl->running = false;
    _impl->socket.close();
    for (auto& [peer, endpoint] : _impl->peers) {
        (void)peer;
        ayt::crypto::secureZero(endpoint.token.bytes.data(), endpoint.token.bytes.size());
    }
    for (auto& [peer, pending] : _impl->pendingRegistrations) {
        (void)peer;
        ayt::crypto::secureZero(pending.token.bytes.data(), pending.token.bytes.size());
    }
    _impl->peers.clear();
    _impl->pendingRegistrations.clear();
    _impl->receiveBuffer.clear();
}

bool SecureUdpSignalingServer::isRunning() const { return _impl && _impl->running; }

uint16_t SecureUdpSignalingServer::getBoundPort() const {
    return isRunning() ? _impl->socket.getBoundPort() : 0;
}

size_t SecureUdpSignalingServer::getPeerCount() const {
    return _impl ? _impl->peers.size() : 0;
}

size_t SecureUdpSignalingServer::pump(size_t maxMessages) {
    if (!isRunning() || maxMessages == 0) return 0;
    size_t processed = 0;
    for (size_t i = 0; i < maxMessages; ++i) {
        char address[64]{};
        uint16_t port = 0;
        const int received = _impl->socket.receiveFrom(
            address, &port, _impl->receiveBuffer.data(), _impl->receiveBuffer.size());
        if (received <= 0) break;
        PacketView packet;
        if (!decodePacket(_impl->receiveBuffer.data(), static_cast<size_t>(received), packet)) {
            ++_impl->stats.malformedDrops;
            continue;
        }
        const uint64_t now = monotonicMs();

        if (packet.type == PacketType::Register) {
            if (packet.serverNonce != 0 || !packet.to.value.empty() || packet.payloadSize != 0) {
                ++_impl->stats.malformedDrops;
                continue;
            }
            SecureSignalingCredential credential;
            if (!_impl->config.resolveCredential(packet.from, packet.room, credential) ||
                !credential.token.isValid() ||
                !verifyTag(credential.token, _impl->receiveBuffer.data(),
                           packet.authenticatedSize, packet.tag)) {
                ++_impl->stats.authenticationFailures;
                continue;
            }
            if (credential.expiresAtUnixSeconds != 0 &&
                credential.expiresAtUnixSeconds <= unixSeconds()) {
                ++_impl->stats.authenticationFailures;
                continue;
            }
            auto existing = _impl->peers.find(packet.from.value);
            const bool sameSessionAndEndpoint = existing != _impl->peers.end() &&
                existing->second.clientNonce == packet.clientNonce &&
                existing->second.room == packet.room &&
                existing->second.address == address && existing->second.port == port;
            if (sameSessionAndEndpoint) {
                auto& endpoint = existing->second;
                endpoint.token = credential.token;
                endpoint.expiresAtUnixSeconds = credential.expiresAtUnixSeconds;
                endpoint.lastSeenMs = now;
                endpoint.lastCredentialCheckMs = now;
                if (!endpoint.inboundReplay.accept(packet.sequence)) {
                    ++_impl->stats.replayDrops;
                    continue;
                }
                (void)_impl->sendToEndpoint(
                    PacketType::RegisterAck, packet.from, {}, nullptr, 0, endpoint);
                ++_impl->stats.authenticatedPackets;
                ++processed;
                continue;
            }

            if (existing == _impl->peers.end() &&
                _impl->peers.size() + _impl->pendingRegistrations.size() >=
                    _impl->config.maxPeers) continue;
            auto pendingIt = _impl->pendingRegistrations.find(packet.from.value);
            const bool samePending = pendingIt != _impl->pendingRegistrations.end() &&
                pendingIt->second.clientNonce == packet.clientNonce &&
                pendingIt->second.room == packet.room &&
                pendingIt->second.address == address && pendingIt->second.port == port;
            if (samePending && !pendingIt->second.inboundReplay.accept(packet.sequence)) {
                ++_impl->stats.replayDrops;
                continue;
            }
            if (!samePending) {
                Impl::PendingRegistration pending;
                pending.address = address;
                pending.port = port;
                pending.room = packet.room;
                pending.token = credential.token;
                pending.expiresAtUnixSeconds = credential.expiresAtUnixSeconds;
                pending.clientNonce = packet.clientNonce;
                pending.serverNonce = randomNonZeroU64();
                pending.createdMs = now;
                (void)pending.inboundReplay.accept(packet.sequence);
                pendingIt = _impl->pendingRegistrations.insert_or_assign(
                    packet.from.value, std::move(pending)).first;
            }
            (void)_impl->sendChallenge(packet.from, pendingIt->second);
            ++_impl->stats.authenticatedPackets;
            ++processed;
            continue;
        }

        if (packet.type == PacketType::RegisterConfirm) {
            auto pendingIt = _impl->pendingRegistrations.find(packet.from.value);
            if (pendingIt == _impl->pendingRegistrations.end()) continue;
            auto& pending = pendingIt->second;
            if (pending.address != address || pending.port != port ||
                pending.room != packet.room || pending.clientNonce != packet.clientNonce ||
                pending.serverNonce != packet.serverNonce ||
                (pending.expiresAtUnixSeconds != 0 &&
                 pending.expiresAtUnixSeconds <= unixSeconds()) ||
                !verifyTag(pending.token, _impl->receiveBuffer.data(),
                           packet.authenticatedSize, packet.tag)) {
                ++_impl->stats.authenticationFailures;
                continue;
            }
            if (!pending.inboundReplay.accept(packet.sequence)) {
                ++_impl->stats.replayDrops;
                continue;
            }
            Impl::Endpoint endpoint;
            endpoint.address = pending.address;
            endpoint.port = pending.port;
            endpoint.room = pending.room;
            endpoint.token = pending.token;
            endpoint.expiresAtUnixSeconds = pending.expiresAtUnixSeconds;
            endpoint.clientNonce = pending.clientNonce;
            endpoint.serverNonce = pending.serverNonce;
            endpoint.nextOutboundSequence = pending.nextOutboundSequence;
            endpoint.lastSeenMs = now;
            endpoint.lastCredentialCheckMs = now;
            endpoint.inboundReplay = pending.inboundReplay;
            endpoint.rateWindowStartMs = now;
            auto installed = _impl->peers.insert_or_assign(
                packet.from.value, std::move(endpoint)).first;
            _impl->pendingRegistrations.erase(pendingIt);
            (void)_impl->sendToEndpoint(
                PacketType::RegisterAck, packet.from, {}, nullptr, 0, installed->second);
            ++_impl->stats.authenticatedPackets;
            ++processed;
            continue;
        }

        auto sourceIt = _impl->peers.find(packet.from.value);
        if (sourceIt == _impl->peers.end()) continue;
        auto& source = sourceIt->second;
        if (source.address != address || source.port != port || source.room != packet.room ||
            source.clientNonce != packet.clientNonce || source.serverNonce != packet.serverNonce ||
            (source.expiresAtUnixSeconds != 0 && source.expiresAtUnixSeconds <= unixSeconds()) ||
            !verifyTag(source.token, _impl->receiveBuffer.data(),
                       packet.authenticatedSize, packet.tag)) {
            ++_impl->stats.authenticationFailures;
            continue;
        }
        if (_impl->config.credentialRecheckIntervalMs == 0 ||
            now - source.lastCredentialCheckMs >=
                _impl->config.credentialRecheckIntervalMs) {
            SecureSignalingCredential current;
            if (!_impl->config.resolveCredential(packet.from, packet.room, current) ||
                !current.token.isValid() || current.token != source.token ||
                (current.expiresAtUnixSeconds != 0 &&
                 current.expiresAtUnixSeconds <= unixSeconds())) {
                ayt::crypto::secureZero(
                    source.token.bytes.data(), source.token.bytes.size());
                _impl->peers.erase(sourceIt);
                ++_impl->stats.authenticationFailures;
                continue;
            }
            source.expiresAtUnixSeconds = current.expiresAtUnixSeconds;
            source.lastCredentialCheckMs = now;
        }
        if (!source.inboundReplay.accept(packet.sequence)) {
            ++_impl->stats.replayDrops;
            continue;
        }
        if (!_impl->withinRate(source, static_cast<size_t>(received), now)) {
            ++_impl->stats.rateLimitedDrops;
            _impl->sendError(packet.from, source, SecureSignalingError::RateLimited);
            continue;
        }
        source.lastSeenMs = now;
        ++_impl->stats.authenticatedPackets;

        if (packet.type == PacketType::Unregister) {
            _impl->peers.erase(sourceIt);
        } else if (packet.type == PacketType::Heartbeat) {
            (void)_impl->sendToEndpoint(PacketType::HeartbeatAck, packet.from, {}, nullptr, 0, source);
        } else if (packet.type == PacketType::Signal) {
            auto targetIt = _impl->peers.find(packet.to.value);
            const bool targetExpired = targetIt != _impl->peers.end() &&
                targetIt->second.expiresAtUnixSeconds != 0 &&
                targetIt->second.expiresAtUnixSeconds <= unixSeconds();
            if (targetIt == _impl->peers.end() || targetExpired) {
                if (targetExpired && targetIt != sourceIt) _impl->peers.erase(targetIt);
                _impl->sendError(packet.from, source, SecureSignalingError::PeerUnavailable);
            } else if (targetIt->second.room != source.room) {
                ++_impl->stats.roomMismatchDrops;
                _impl->sendError(packet.from, source, SecureSignalingError::RoomMismatch);
            } else if (_impl->sendToEndpoint(PacketType::Signal, packet.from, packet.to,
                                             packet.payload, packet.payloadSize,
                                             targetIt->second)) {
                ++_impl->stats.forwardedSignals;
            }
        } else {
            ++_impl->stats.malformedDrops;
            continue;
        }
        ++processed;
    }
    pruneExpired();
    return processed;
}

void SecureUdpSignalingServer::pruneExpired() {
    if (!_impl) return;
    const uint64_t now = monotonicMs();
    const uint64_t wall = unixSeconds();
    for (auto it = _impl->peers.begin(); it != _impl->peers.end();) {
        const bool timedOut = _impl->config.peerTimeoutMs != 0 &&
            now - it->second.lastSeenMs >= _impl->config.peerTimeoutMs;
        const bool expired = it->second.expiresAtUnixSeconds != 0 &&
            it->second.expiresAtUnixSeconds <= wall;
        if (timedOut || expired) it = _impl->peers.erase(it);
        else ++it;
    }
    for (auto it = _impl->pendingRegistrations.begin();
         it != _impl->pendingRegistrations.end();) {
        if (now - it->second.createdMs >= 5000) it = _impl->pendingRegistrations.erase(it);
        else ++it;
    }
}

SecureSignalingServerStats SecureUdpSignalingServer::getStats() const {
    return _impl ? _impl->stats : SecureSignalingServerStats{};
}

} // namespace ayt::net
