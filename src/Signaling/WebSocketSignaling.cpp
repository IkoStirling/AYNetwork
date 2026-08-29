#include <AYNetwork/Signaling/WebSocketSignaling.h>

#include <httplib.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <deque>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

namespace ayt::net
{
namespace
{

constexpr char kReadyFrame[] = {'A', 'Y', 'W', 'S', '1'};

void timeout(uint32_t milliseconds, time_t& seconds, time_t& microseconds) {
    seconds = static_cast<time_t>(milliseconds / 1000u);
    microseconds = static_cast<time_t>((milliseconds % 1000u) * 1000u);
}

} // namespace

bool WebSocketSignalingClientConfig::isValid() const {
    return !serverAddress.empty() && serverPort != 0 && !path.empty() &&
           path.front() == '/' && roomId.isValid() && token.isValid() &&
           connectTimeoutMs != 0 && ioTimeoutMs != 0 &&
           maxMessageBytes > 1 && maxMessageBytes <= 1024u * 1024u &&
           maxPendingSignals != 0;
}

struct WebSocketSignalingClient::Impl {
    struct Pending {
        PeerId sender;
        std::vector<uint8_t> payload;
    };

    explicit Impl(WebSocketSignalingClientConfig input)
        : config(std::move(input)) {}

    WebSocketSignalingClientConfig config;
    PeerId localPeer;
    std::unique_ptr<httplib::ws::WebSocketClient> client;
    std::thread reader;
    std::atomic<bool> running{false};
    std::mutex clientMutex;
    std::mutex pendingMutex;
    std::deque<Pending> pending;
};

WebSocketSignalingClient::WebSocketSignalingClient(
    WebSocketSignalingClientConfig config)
    : _impl(std::make_unique<Impl>(std::move(config))) {}

WebSocketSignalingClient::~WebSocketSignalingClient() { stop(); }

bool WebSocketSignalingClient::start(const PeerId& localPeer) {
    if (!_impl || _impl->running.load() || !_impl->config.isValid() ||
        !localPeer.isValid()) return false;
    const std::string scheme = _impl->config.useTls ? "wss://" : "ws://";
    const std::string endpoint = scheme + _impl->config.serverAddress + ':' +
        std::to_string(_impl->config.serverPort) + _impl->config.path;
    httplib::Headers headers{
        {"Authorization", "Bearer " +
            signalingTokenToHex(_impl->config.token)},
        {"X-AY-Peer", localPeer.value},
        {"X-AY-Room", _impl->config.roomId.value},
    };
    auto client = std::make_unique<httplib::ws::WebSocketClient>(
        endpoint, headers);
    time_t seconds = 0;
    time_t microseconds = 0;
    timeout(_impl->config.connectTimeoutMs, seconds, microseconds);
    client->set_connection_timeout(seconds, microseconds);
    timeout(_impl->config.ioTimeoutMs, seconds, microseconds);
    client->set_read_timeout(seconds, microseconds);
    client->set_write_timeout(seconds, microseconds);
#if defined(CPPHTTPLIB_SSL_ENABLED)
    if (_impl->config.useTls) client->enable_system_ca(true);
#else
    if (_impl->config.useTls) return false;
#endif
    if (!client->is_valid() || !client->connect()) return false;
    std::string ready;
    if (client->read(ready) != httplib::ws::Binary ||
        ready.size() != sizeof(kReadyFrame) ||
        !std::equal(ready.begin(), ready.end(), std::begin(kReadyFrame))) {
        client->close(httplib::ws::CloseStatus::PolicyViolation,
                      "signaling authentication failed");
        return false;
    }
    _impl->localPeer = localPeer;
    _impl->client = std::move(client);
    _impl->running.store(true);
    _impl->reader = std::thread([impl = _impl.get()] {
        while (impl->running.load()) {
            std::string frame;
            const auto read = impl->client->read(frame);
            if (read != httplib::ws::Binary || frame.size() < 2 ||
                frame.size() > impl->config.maxMessageBytes) break;
            const size_t senderSize = static_cast<uint8_t>(frame[0]);
            if (senderSize == 0 || senderSize > 63 ||
                1 + senderSize >= frame.size()) break;
            Impl::Pending pending;
            pending.sender = PeerId{frame.substr(1, senderSize)};
            if (!pending.sender.isValid()) break;
            pending.payload.assign(frame.begin() + 1 + senderSize, frame.end());
            std::lock_guard lock(impl->pendingMutex);
            if (impl->pending.size() >= impl->config.maxPendingSignals) {
                impl->pending.pop_front();
            }
            impl->pending.push_back(std::move(pending));
        }
        impl->running.store(false);
    });
    return true;
}

void WebSocketSignalingClient::stop() {
    if (!_impl) return;
    _impl->running.store(false);
    {
        std::lock_guard lock(_impl->clientMutex);
        if (_impl->client) {
            _impl->client->close(httplib::ws::CloseStatus::Normal,
                                 "signaling stopped");
        }
    }
    if (_impl->reader.joinable()) _impl->reader.join();
    std::lock_guard clientLock(_impl->clientMutex);
    _impl->client.reset();
    _impl->localPeer = {};
    std::lock_guard pendingLock(_impl->pendingMutex);
    _impl->pending.clear();
}

bool WebSocketSignalingClient::isRunning() const {
    return _impl && _impl->running.load();
}

bool WebSocketSignalingClient::sendSignal(
    const PeerId& destination, const void* data, size_t size) {
    if (!_impl || !_impl->running.load() || !destination.isValid() || !data ||
        size == 0 || 1 + destination.value.size() + size >
            _impl->config.maxMessageBytes) return false;
    std::vector<char> frame;
    frame.reserve(1 + destination.value.size() + size);
    frame.push_back(static_cast<char>(destination.value.size()));
    frame.insert(frame.end(), destination.value.begin(), destination.value.end());
    const auto* bytes = static_cast<const char*>(data);
    frame.insert(frame.end(), bytes, bytes + size);
    std::lock_guard lock(_impl->clientMutex);
    return _impl->client && _impl->client->send(frame.data(), frame.size());
}

size_t WebSocketSignalingClient::poll(
    const ReceiveHandler& handler, size_t maxMessages) {
    if (!_impl || !handler || maxMessages == 0) return 0;
    std::deque<Impl::Pending> batch;
    {
        std::lock_guard lock(_impl->pendingMutex);
        const size_t count = (std::min)(maxMessages, _impl->pending.size());
        for (size_t index = 0; index < count; ++index) {
            batch.push_back(std::move(_impl->pending.front()));
            _impl->pending.pop_front();
        }
    }
    for (const auto& pending : batch) {
        handler(pending.sender, pending.payload.data(), pending.payload.size());
    }
    return batch.size();
}

} // namespace ayt::net
