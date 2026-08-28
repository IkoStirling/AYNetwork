#include <AYNetwork/Session/P2PSessionLeaseKeeper.h>

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <utility>

namespace ayt::net
{
namespace
{

bool isPermanentLeaseError(SessionServiceError error) {
    switch (error) {
    case SessionServiceError::InvalidRequest:
    case SessionServiceError::Unauthorized:
    case SessionServiceError::SessionNotFound:
    case SessionServiceError::SessionFull:
    case SessionServiceError::SessionClosed:
    case SessionServiceError::EpochConflict:
    case SessionServiceError::HostLeaseExpired:
    case SessionServiceError::ProtocolError:
        return true;
    case SessionServiceError::None:
    case SessionServiceError::TransportError:
    case SessionServiceError::InternalError:
    case SessionServiceError::RateLimited:
        return false;
    }
    return true;
}

} // namespace

struct P2PSessionLeaseKeeper::Impl {
    std::shared_ptr<IP2PSessionService> service;
    P2PSessionMemberCredential member;
    P2PSessionLeaseKeeperConfig config;

    mutable std::mutex mutex;
    std::condition_variable wake;
    std::thread worker;
    bool stopRequested = false;
    bool running = false;
    uint32_t expectedEpoch = 0;
    SessionServiceError lastError = SessionServiceError::None;
    P2PBackendSessionInfo lastSession;

    void run() {
        std::unique_lock<std::mutex> lock(mutex);
        while (!stopRequested) {
            const uint32_t epoch = expectedEpoch;
            lock.unlock();

            P2PSessionHeartbeatRequest request;
            request.member = member;
            request.expectedEpoch = epoch;
            const auto result = service->heartbeat(request);

            lock.lock();
            if (stopRequested) break;
            lastError = result.error;
            if (result) lastSession = result.value;
            if (!result && isPermanentLeaseError(result.error)) break;

            const uint32_t delay = result
                ? config.heartbeatIntervalMs : config.transientFailureRetryMs;
            wake.wait_for(lock, std::chrono::milliseconds(delay), [this, epoch] {
                return stopRequested || expectedEpoch != epoch;
            });
        }
        running = false;
    }
};

P2PSessionLeaseKeeper::P2PSessionLeaseKeeper(
    std::shared_ptr<IP2PSessionService> service,
    P2PSessionMemberCredential member,
    P2PSessionLeaseKeeperConfig config)
    : _impl(std::make_unique<Impl>()) {
    _impl->service = std::move(service);
    _impl->member = std::move(member);
    _impl->config = config;
}

P2PSessionLeaseKeeper::~P2PSessionLeaseKeeper() {
    stop();
}

bool P2PSessionLeaseKeeper::start(uint32_t expectedEpoch) {
    if (expectedEpoch == 0 || !_impl->service || !_impl->member.isValid() ||
        !_impl->config.isValid()) {
        return false;
    }
    stop();
    {
        std::lock_guard<std::mutex> lock(_impl->mutex);
        _impl->stopRequested = false;
        _impl->running = true;
        _impl->expectedEpoch = expectedEpoch;
        _impl->lastError = SessionServiceError::None;
    }
    try {
        _impl->worker = std::thread([impl = _impl.get()] { impl->run(); });
    } catch (...) {
        std::lock_guard<std::mutex> lock(_impl->mutex);
        _impl->running = false;
        _impl->stopRequested = true;
        return false;
    }
    return true;
}

void P2PSessionLeaseKeeper::stop() {
    std::thread worker;
    {
        std::lock_guard<std::mutex> lock(_impl->mutex);
        _impl->stopRequested = true;
        _impl->wake.notify_all();
        worker = std::move(_impl->worker);
    }
    if (worker.joinable()) worker.join();
    std::lock_guard<std::mutex> lock(_impl->mutex);
    _impl->running = false;
}

bool P2PSessionLeaseKeeper::isRunning() const {
    std::lock_guard<std::mutex> lock(_impl->mutex);
    return _impl->running;
}

SessionServiceError P2PSessionLeaseKeeper::getLastError() const {
    std::lock_guard<std::mutex> lock(_impl->mutex);
    return _impl->lastError;
}

P2PBackendSessionInfo P2PSessionLeaseKeeper::getLastSession() const {
    std::lock_guard<std::mutex> lock(_impl->mutex);
    return _impl->lastSession;
}

SessionServiceResult<P2PBackendSessionInfo>
P2PSessionLeaseKeeper::claimHost(uint32_t expectedEpoch,
                                 const PeerId& newHostPeerId) {
    if (expectedEpoch == 0 || !newHostPeerId.isValid() || !_impl->service ||
        !_impl->member.isValid()) {
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::InvalidRequest,
            "invalid Host claim request");
    }

    P2PSessionClaimHostRequest request;
    request.member = _impl->member;
    request.expectedEpoch = expectedEpoch;
    request.newHostPeerId = newHostPeerId;
    auto result = _impl->service->claimHost(request);
    {
        std::lock_guard<std::mutex> lock(_impl->mutex);
        _impl->lastError = result.error;
        if (result) _impl->lastSession = result.value;
    }
    if (!result) return result;

    if (newHostPeerId == _impl->member.peerId) {
        if (!start(result.value.epoch)) {
            return SessionServiceResult<P2PBackendSessionInfo>::failure(
                SessionServiceError::InternalError,
                "Host claim committed but lease renewal could not start");
        }
    } else {
        stop();
    }
    return result;
}

} // namespace ayt::net
