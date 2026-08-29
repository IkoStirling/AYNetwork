#include <AYNetwork/Session/DedicatedServerRuntime.h>

#include <sodium.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <future>
#include <map>
#include <set>
#include <unordered_map>
#include <utility>

namespace ayt::net
{
namespace
{

constexpr uint8_t kAdmissionVersion = 1;

uint64_t monotonicNow() {
    return static_cast<uint64_t>(std::chrono::duration_cast<
        std::chrono::milliseconds>(std::chrono::steady_clock::now()
            .time_since_epoch()).count());
}

uint64_t unixNow() {
    return static_cast<uint64_t>(std::chrono::duration_cast<
        std::chrono::seconds>(std::chrono::system_clock::now()
            .time_since_epoch()).count());
}

void appendU64(std::vector<uint8_t>& out, uint64_t value) {
    for (unsigned shift = 0; shift < 64; shift += 8) {
        out.push_back(static_cast<uint8_t>(value >> shift));
    }
}

bool readU64(const uint8_t* bytes, size_t size, size_t& offset,
             uint64_t& value) {
    if (!bytes || offset + 8 > size) return false;
    value = 0;
    for (unsigned shift = 0; shift < 64; shift += 8) {
        value |= static_cast<uint64_t>(bytes[offset++]) << shift;
    }
    return true;
}

bool secretEqual(std::string_view left, std::string_view right) {
    return left.size() == right.size() && !left.empty() &&
           sodium_memcmp(left.data(), right.data(), left.size()) == 0;
}

} // namespace

bool encodeDedicatedAdmission(const DedicatedAdmission& admission,
                              std::vector<uint8_t>& bytes) {
    bytes.clear();
    if (!admission.isValid()) return false;
    bytes.reserve(1 + 8 + 1 + admission.peerId.value.size() + 64);
    bytes.push_back(kAdmissionVersion);
    appendU64(bytes, admission.allocationId);
    bytes.push_back(static_cast<uint8_t>(admission.peerId.value.size()));
    bytes.insert(bytes.end(), admission.peerId.value.begin(),
                 admission.peerId.value.end());
    bytes.insert(bytes.end(), admission.reservationToken.begin(),
                 admission.reservationToken.end());
    return bytes.size() <= kConnectionAdmissionMaxBytes;
}

bool decodeDedicatedAdmission(const uint8_t* bytes, size_t size,
                              DedicatedAdmission& admission) {
    admission = {};
    if (!bytes || size < 1 + 8 + 1 + 1 + 64 ||
        size > kConnectionAdmissionMaxBytes || bytes[0] != kAdmissionVersion) {
        return false;
    }
    DedicatedAdmission parsed;
    size_t offset = 1;
    if (!readU64(bytes, size, offset, parsed.allocationId) || offset >= size) {
        return false;
    }
    const size_t peerSize = bytes[offset++];
    if (peerSize == 0 || peerSize > 63 || offset + peerSize + 64 != size) {
        return false;
    }
    parsed.peerId = PeerId{std::string(
        reinterpret_cast<const char*>(bytes + offset), peerSize)};
    offset += peerSize;
    parsed.reservationToken.assign(
        reinterpret_cast<const char*>(bytes + offset), 64);
    if (!parsed.isValid()) return false;
    admission = std::move(parsed);
    return true;
}

bool deriveDedicatedAdmissionToken(std::string_view reservationToken,
                                   const PeerId& peerId,
                                   std::string& admissionToken) {
    admissionToken.clear();
    if (reservationToken.size() != 64 || !peerId.isValid() ||
        sodium_init() < 0) return false;
    std::array<unsigned char, crypto_generichash_BYTES> digest{};
    if (crypto_generichash(
            digest.data(), digest.size(),
            reinterpret_cast<const unsigned char*>(peerId.value.data()),
            peerId.value.size(),
            reinterpret_cast<const unsigned char*>(reservationToken.data()),
            reservationToken.size()) != 0) {
        return false;
    }
    std::array<char, crypto_generichash_BYTES * 2 + 1> encoded{};
    if (!sodium_bin2hex(encoded.data(), encoded.size(), digest.data(),
                        digest.size())) return false;
    admissionToken.assign(encoded.data(), encoded.size() - 1);
    sodium_memzero(digest.data(), digest.size());
    return admissionToken.size() == 64;
}

bool DedicatedServerRuntimeConfig::isValid() const {
    return !registration.instanceName.empty() &&
           !registration.region.empty() && !registration.buildId.empty() &&
           !registration.address.empty() && registration.port != 0 &&
           registration.capacity != 0 && heartbeatIntervalMs != 0 &&
           allocationPollIntervalMs != 0 && retryIntervalMs != 0 &&
           maximumConsecutiveBackendFailures != 0 && drainTimeoutMs != 0;
}

struct DedicatedServerRuntime::Impl {
    enum class TaskKind { None, Heartbeat, Allocations, Drain, Release, Unregister };
    struct TaskResult {
        TaskKind kind = TaskKind::None;
        OnlineServiceError error = OnlineServiceError::None;
        std::string message;
        DedicatedServerInfo server;
        std::vector<DedicatedAllocation> allocations;
        DedicatedAllocationId allocationId = 0;
    };
    struct AllocationState {
        DedicatedAllocation allocation;
        std::map<std::string, NetConnection*> connections;
        bool everAdmitted = false;
        bool releaseQueued = false;
    };
    struct ConnectionState {
        DedicatedAllocationId allocationId = 0;
        PeerId peerId;
    };

    Impl(INetworkSubSystem& inputNetwork,
         std::shared_ptr<IDedicatedServerService> inputService,
         std::shared_ptr<IDedicatedWorldHost> inputWorlds,
         DedicatedServerRuntimeConfig inputConfig)
        : network(inputNetwork), service(std::move(inputService)),
          worlds(std::move(inputWorlds)), config(std::move(inputConfig)) {
        if (!config.nowMonotonicMilliseconds) {
            config.nowMonotonicMilliseconds = monotonicNow;
        }
        if (!config.nowUnixSeconds) config.nowUnixSeconds = unixNow;
    }

    bool admit(NetConnection* connection, const uint8_t* bytes, size_t size) {
        DedicatedAdmission admission;
        if (status.state == DedicatedServerRuntimeState::Draining ||
            !decodeDedicatedAdmission(bytes, size, admission)) {
            ++status.rejectedAdmissions;
            return false;
        }
        const auto found = allocations.find(admission.allocationId);
        if (found == allocations.end()) {
            ++status.rejectedAdmissions;
            return false;
        }
        AllocationState& state = found->second;
        const uint64_t now = config.nowUnixSeconds();
        const bool namedPlayer = state.allocation.players.empty() ||
            std::find(state.allocation.players.begin(),
                      state.allocation.players.end(), admission.peerId) !=
                state.allocation.players.end();
        std::string expectedAdmission;
        const bool validAdmission = deriveDedicatedAdmissionToken(
            state.allocation.reservationToken, admission.peerId,
            expectedAdmission);
        if (now > state.allocation.expiresAtUnixSeconds || !namedPlayer ||
            !validAdmission ||
            !secretEqual(expectedAdmission, admission.reservationToken) ||
            state.connections.contains(admission.peerId.value) ||
            state.connections.size() >= state.allocation.playerCount) {
            ++status.rejectedAdmissions;
            return false;
        }
        state.connections.emplace(admission.peerId.value, connection);
        state.everAdmitted = true;
        connectionAdmissions.emplace(
            connection, ConnectionState{admission.allocationId,
                                        admission.peerId});
        return true;
    }

    void connectionChanged(NetConnection* connection, bool connected) {
        const auto found = connectionAdmissions.find(connection);
        if (found == connectionAdmissions.end()) return;
        const ConnectionState connectionState = found->second;
        const auto allocation = allocations.find(connectionState.allocationId);
        if (connected) {
            if (allocation != allocations.end()) {
                worlds->playerConnected(connectionState.allocationId,
                                        connectionState.peerId, connection);
            }
            return;
        }
        connectionAdmissions.erase(found);
        if (allocation == allocations.end()) return;
        allocation->second.connections.erase(connectionState.peerId.value);
        worlds->playerDisconnected(connectionState.allocationId,
                                   connectionState.peerId);
        if (allocation->second.everAdmitted &&
            allocation->second.connections.empty()) {
            queueRelease(allocation->first);
        }
    }

    void queueRelease(DedicatedAllocationId id) {
        const auto found = allocations.find(id);
        if (found == allocations.end() || found->second.releaseQueued) return;
        found->second.releaseQueued = true;
        releases.push_back(id);
    }

    void schedule(TaskKind kind, DedicatedAllocationId allocationId = 0) {
        const DedicatedServerCredential currentCredential = credential;
        const auto currentService = service;
        std::string token;
        if (allocationId != 0) {
            const auto found = allocations.find(allocationId);
            if (found == allocations.end()) return;
            token = found->second.allocation.reservationToken;
        }
        task = std::async(std::launch::async,
            [kind, allocationId, token = std::move(token), currentCredential,
             currentService]() mutable {
                TaskResult result;
                result.kind = kind;
                result.allocationId = allocationId;
                if (kind == TaskKind::Heartbeat) {
                    auto response = currentService->heartbeatServer(currentCredential);
                    result.error = response.error;
                    result.message = std::move(response.message);
                    result.server = std::move(response.value);
                } else if (kind == TaskKind::Allocations) {
                    auto response = currentService->listServerAllocations(
                        currentCredential);
                    result.error = response.error;
                    result.message = std::move(response.message);
                    result.allocations = std::move(response.value);
                } else if (kind == TaskKind::Drain) {
                    auto response = currentService->setServerDraining(
                        currentCredential, true);
                    result.error = response.error;
                    result.message = std::move(response.message);
                    result.server = std::move(response.value);
                } else if (kind == TaskKind::Release) {
                    auto response = currentService->releaseAllocation(
                        allocationId, token);
                    result.error = response.error;
                    result.message = std::move(response.message);
                } else if (kind == TaskKind::Unregister) {
                    auto response = currentService->unregisterServer(
                        currentCredential);
                    result.error = response.error;
                    result.message = std::move(response.message);
                }
                return result;
            });
        taskKind = kind;
    }

    void stopAllocation(DedicatedAllocationId id, bool kick) {
        const auto found = allocations.find(id);
        if (found == allocations.end()) return;
        if (kick) {
            for (const auto& [peer, connection] : found->second.connections) {
                (void)peer;
                if (connection) network.kickConnection(
                    connection, "Dedicated allocation ended");
            }
        }
        worlds->stopAuthoritativeWorld(id);
        allocations.erase(found);
    }

    void reconcile(std::vector<DedicatedAllocation> current) {
        std::set<DedicatedAllocationId> seen;
        for (auto& allocation : current) {
            if (!allocation.isValid() || allocation.serverId != credential.serverId) {
                fail(DedicatedServerRuntimeError::BackendUnavailable,
                     OnlineServiceError::InternalError,
                     "backend returned an invalid server allocation");
                return;
            }
            seen.insert(allocation.allocationId);
            auto found = allocations.find(allocation.allocationId);
            if (found != allocations.end()) {
                found->second.allocation = std::move(allocation);
                continue;
            }
            if (status.state == DedicatedServerRuntimeState::Draining) continue;
            if (!worlds->startAuthoritativeWorld(allocation)) {
                fail(DedicatedServerRuntimeError::WorldStartFailed,
                     OnlineServiceError::None,
                     "failed to start authoritative world");
                return;
            }
            AllocationState state;
            state.allocation = std::move(allocation);
            allocations.emplace(state.allocation.allocationId,
                                std::move(state));
        }
        std::vector<DedicatedAllocationId> removed;
        for (const auto& [id, state] : allocations) {
            (void)state;
            if (!seen.contains(id)) removed.push_back(id);
        }
        for (DedicatedAllocationId id : removed) stopAllocation(id, true);
    }

    void finishTask(TaskResult result) {
        taskKind = TaskKind::None;
        if (result.error != OnlineServiceError::None) {
            if (result.kind == TaskKind::Release &&
                result.error == OnlineServiceError::NotFound) {
                stopAllocation(result.allocationId, false);
                return;
            }
            if (result.kind == TaskKind::Release) {
                const auto allocation = allocations.find(result.allocationId);
                if (allocation != allocations.end()) {
                    allocation->second.releaseQueued = false;
                    queueRelease(result.allocationId);
                }
            }
            ++consecutiveFailures;
            status.serviceError = result.error;
            status.message = std::move(result.message);
            retryAt = config.nowMonotonicMilliseconds() + config.retryIntervalMs;
            if (result.error == OnlineServiceError::Unauthorized) {
                fail(DedicatedServerRuntimeError::CredentialRejected,
                     result.error, status.message);
            } else if (consecutiveFailures >=
                       config.maximumConsecutiveBackendFailures) {
                fail(DedicatedServerRuntimeError::BackendUnavailable,
                     result.error, status.message);
            }
            return;
        }
        consecutiveFailures = 0;
        status.serviceError = OnlineServiceError::None;
        status.message.clear();
        if (result.kind == TaskKind::Heartbeat || result.kind == TaskKind::Drain) {
            status.server = std::move(result.server);
            nextHeartbeatAt = config.nowMonotonicMilliseconds() +
                config.heartbeatIntervalMs;
            if (result.kind == TaskKind::Drain) drainPublished = true;
        } else if (result.kind == TaskKind::Allocations) {
            reconcile(std::move(result.allocations));
            nextAllocationPollAt = config.nowMonotonicMilliseconds() +
                config.allocationPollIntervalMs;
        } else if (result.kind == TaskKind::Release) {
            stopAllocation(result.allocationId, false);
        } else if (result.kind == TaskKind::Unregister) {
            forceStop();
        }
    }

    void fail(DedicatedServerRuntimeError error, OnlineServiceError serviceError,
              std::string message) {
        status.state = DedicatedServerRuntimeState::Failed;
        status.error = error;
        status.serviceError = serviceError;
        status.message = std::move(message);
        network.disconnect();
    }

    void forceStop() {
        for (auto& [id, state] : allocations) {
            (void)state;
            worlds->stopAuthoritativeWorld(id);
        }
        allocations.clear();
        connectionAdmissions.clear();
        releases.clear();
        network.disconnect();
        network.setConnectionAdmissionValidator({});
        network.onConnectionChange({});
        credential = {};
        status = {};
    }

    INetworkSubSystem& network;
    std::shared_ptr<IDedicatedServerService> service;
    std::shared_ptr<IDedicatedWorldHost> worlds;
    DedicatedServerRuntimeConfig config;
    DedicatedServerCredential credential;
    DedicatedServerRuntimeStatus status;
    std::map<DedicatedAllocationId, AllocationState> allocations;
    std::unordered_map<NetConnection*, ConnectionState> connectionAdmissions;
    std::vector<DedicatedAllocationId> releases;
    std::future<TaskResult> task;
    TaskKind taskKind = TaskKind::None;
    uint64_t nextHeartbeatAt = 0;
    uint64_t nextAllocationPollAt = 0;
    uint64_t retryAt = 0;
    uint64_t drainDeadline = 0;
    uint32_t consecutiveFailures = 0;
    bool drainPublished = false;
};

DedicatedServerRuntime::DedicatedServerRuntime(
    INetworkSubSystem& network,
    std::shared_ptr<IDedicatedServerService> service,
    std::shared_ptr<IDedicatedWorldHost> worlds,
    DedicatedServerRuntimeConfig config)
    : _impl(std::make_unique<Impl>(network, std::move(service),
                                  std::move(worlds), std::move(config))) {}

DedicatedServerRuntime::~DedicatedServerRuntime() { stop(); }

bool DedicatedServerRuntime::start() {
    if (!_impl || !_impl->service || !_impl->worlds ||
        !_impl->config.isValid() ||
        _impl->status.state != DedicatedServerRuntimeState::Stopped ||
        sodium_init() < 0) {
        if (_impl) {
            _impl->status.state = DedicatedServerRuntimeState::Failed;
            _impl->status.error =
                DedicatedServerRuntimeError::InvalidConfiguration;
        }
        return false;
    }
    auto grant = _impl->service->registerServer(_impl->config.registration);
    if (!grant) {
        _impl->fail(DedicatedServerRuntimeError::RegistrationFailed,
                    grant.error, grant.message);
        return false;
    }
    _impl->credential = grant.value.credential;
    _impl->status.server = grant.value.server;
    _impl->network.setConnectionAdmissionValidator(
        [impl = _impl.get()](NetConnection* connection,
                             const uint8_t* bytes, size_t size) {
            return impl->admit(connection, bytes, size);
        });
    _impl->network.onConnectionChange(
        [impl = _impl.get()](NetConnection* connection, bool connected,
                             DisconnectReason) {
            impl->connectionChanged(connection, connected);
        });
    _impl->network.listen(_impl->config.registration.port);
    if (!_impl->network.isListening()) {
        (void)_impl->service->unregisterServer(_impl->credential);
        _impl->credential = {};
        _impl->fail(DedicatedServerRuntimeError::ListenFailed,
                    OnlineServiceError::None,
                    "failed to start Dedicated listener");
        return false;
    }
    const uint64_t now = _impl->config.nowMonotonicMilliseconds();
    _impl->nextHeartbeatAt = now;
    _impl->nextAllocationPollAt = now;
    _impl->status.state = DedicatedServerRuntimeState::Ready;
    _impl->status.error = DedicatedServerRuntimeError::None;
    return true;
}

void DedicatedServerRuntime::update(float deltaTime) {
    if (!_impl || (_impl->status.state != DedicatedServerRuntimeState::Ready &&
                   _impl->status.state != DedicatedServerRuntimeState::Draining)) {
        return;
    }
    _impl->network.update(deltaTime);
    _impl->worlds->tickAuthoritativeWorlds(deltaTime);
    if (_impl->taskKind != Impl::TaskKind::None && _impl->task.valid() &&
        _impl->task.wait_for(std::chrono::milliseconds(0)) ==
            std::future_status::ready) {
        _impl->finishTask(_impl->task.get());
    }
    if (_impl->status.state == DedicatedServerRuntimeState::Failed ||
        _impl->taskKind != Impl::TaskKind::None) return;
    const uint64_t now = _impl->config.nowMonotonicMilliseconds();
    if (now < _impl->retryAt) return;
    if (!_impl->releases.empty()) {
        const DedicatedAllocationId id = _impl->releases.front();
        _impl->releases.erase(_impl->releases.begin());
        _impl->schedule(Impl::TaskKind::Release, id);
        return;
    }
    if (_impl->status.state == DedicatedServerRuntimeState::Draining &&
        !_impl->drainPublished) {
        _impl->schedule(Impl::TaskKind::Drain);
        return;
    }
    if (_impl->status.state == DedicatedServerRuntimeState::Draining &&
        (_impl->allocations.empty() || now >= _impl->drainDeadline)) {
        if (now >= _impl->drainDeadline) {
            for (auto& [id, state] : _impl->allocations) {
                for (const auto& [peer, connection] : state.connections) {
                    (void)peer;
                    if (connection) _impl->network.kickConnection(
                        connection, "Dedicated server draining");
                }
                _impl->queueRelease(id);
            }
            if (!_impl->releases.empty()) return;
        }
        _impl->schedule(Impl::TaskKind::Unregister);
        return;
    }
    if (now >= _impl->nextHeartbeatAt) {
        _impl->schedule(Impl::TaskKind::Heartbeat);
    } else if (now >= _impl->nextAllocationPollAt) {
        _impl->schedule(Impl::TaskKind::Allocations);
    }
    _impl->status.activeAllocations = _impl->allocations.size();
    _impl->status.connectedPlayers = _impl->connectionAdmissions.size();
}

void DedicatedServerRuntime::beginDrain() {
    if (!_impl || _impl->status.state != DedicatedServerRuntimeState::Ready) {
        return;
    }
    _impl->status.state = DedicatedServerRuntimeState::Draining;
    _impl->drainPublished = false;
    _impl->drainDeadline = _impl->config.nowMonotonicMilliseconds() +
        _impl->config.drainTimeoutMs;
}

void DedicatedServerRuntime::stop() {
    if (!_impl || _impl->status.state == DedicatedServerRuntimeState::Stopped) {
        return;
    }
    if (_impl->task.valid()) _impl->task.wait();
    if (_impl->credential.isValid()) {
        (void)_impl->service->setServerDraining(_impl->credential, true);
        for (const auto& [id, state] : _impl->allocations) {
            (void)_impl->service->releaseAllocation(
                id, state.allocation.reservationToken);
        }
        (void)_impl->service->unregisterServer(_impl->credential);
    }
    _impl->forceStop();
}

DedicatedServerRuntimeStatus DedicatedServerRuntime::getStatus() const {
    if (!_impl) return {};
    DedicatedServerRuntimeStatus result = _impl->status;
    result.activeAllocations = _impl->allocations.size();
    result.connectedPlayers = _impl->connectionAdmissions.size();
    return result;
}

bool NetworkDedicatedSessionConnectorConfig::isValid() const {
    return localPeerId.isValid() && connectTimeoutMs != 0;
}

struct NetworkDedicatedSessionConnector::Impl {
    Impl(INetworkSubSystem& inputNetwork,
         NetworkDedicatedSessionConnectorConfig inputConfig)
        : network(inputNetwork), config(std::move(inputConfig)) {
        if (!config.nowMonotonicMilliseconds) {
            config.nowMonotonicMilliseconds = monotonicNow;
        }
    }
    INetworkSubSystem& network;
    NetworkDedicatedSessionConnectorConfig config;
    DedicatedSessionConnectionStatus status;
    uint64_t deadline = 0;
};

NetworkDedicatedSessionConnector::NetworkDedicatedSessionConnector(
    INetworkSubSystem& network,
    NetworkDedicatedSessionConnectorConfig config)
    : _impl(std::make_unique<Impl>(network, std::move(config))) {}

NetworkDedicatedSessionConnector::~NetworkDedicatedSessionConnector() {
    disconnect();
}

bool NetworkDedicatedSessionConnector::connect(
    const DedicatedAllocation& allocation) {
    if (!_impl || !_impl->config.isValid() || !allocation.isValid() ||
        _impl->status.state != DedicatedSessionConnectionState::Idle) {
        return false;
    }
    if (!allocation.players.empty() &&
        std::find(allocation.players.begin(), allocation.players.end(),
                  _impl->config.localPeerId) == allocation.players.end()) {
        return false;
    }
    DedicatedAdmission admission;
    admission.allocationId = allocation.allocationId;
    admission.peerId = _impl->config.localPeerId;
    admission.reservationToken = allocation.reservationToken;
    std::vector<uint8_t> bytes;
    if (!encodeDedicatedAdmission(admission, bytes) ||
        !_impl->network.setConnectionAdmissionToken(bytes.data(), bytes.size())) {
        return false;
    }
    _impl->network.connect(allocation.address.c_str(), allocation.port);
    _impl->status.state = DedicatedSessionConnectionState::Connecting;
    _impl->status.message.clear();
    _impl->deadline = _impl->config.nowMonotonicMilliseconds() +
        _impl->config.connectTimeoutMs;
    return true;
}

void NetworkDedicatedSessionConnector::disconnect() {
    if (!_impl) return;
    if (_impl->status.state != DedicatedSessionConnectionState::Idle) {
        _impl->network.disconnect();
    }
    (void)_impl->network.setConnectionAdmissionToken(nullptr, 0);
    _impl->status = {};
    _impl->deadline = 0;
}

void NetworkDedicatedSessionConnector::update() {
    if (!_impl || _impl->status.state == DedicatedSessionConnectionState::Idle ||
        _impl->status.state == DedicatedSessionConnectionState::Failed) return;
    _impl->network.update(0.0f);
    if (_impl->network.isConnected()) {
        _impl->status.state = DedicatedSessionConnectionState::Active;
        return;
    }
    if (_impl->config.nowMonotonicMilliseconds() >= _impl->deadline) {
        _impl->network.disconnect();
        _impl->status.state = DedicatedSessionConnectionState::Failed;
        _impl->status.message = "Dedicated connection timed out";
    }
}

DedicatedSessionConnectionStatus
NetworkDedicatedSessionConnector::getStatus() const {
    return _impl ? _impl->status : DedicatedSessionConnectionStatus{};
}

} // namespace ayt::net
