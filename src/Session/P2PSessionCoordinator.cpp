#include <AYNetwork/Session/P2PSessionCoordinator.h>

#include <AYNetwork/Session/P2PSessionBackend.h>
#include <AYNetwork/Signaling/SecureUdpSignaling.h>

#include <atomic>
#include <chrono>
#include <future>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

namespace ayt::net
{
namespace
{

using Clock = std::chrono::steady_clock;

bool sameContext(const P2PMigrationContext& a,
                 const P2PMigrationContext& b) {
    return a.sessionId == b.sessionId &&
           a.currentEpoch == b.currentEpoch &&
           a.nextEpoch == b.nextEpoch &&
           a.electedHostPeerId == b.electedHostPeerId &&
           a.graceful == b.graceful;
}

bool isTransient(SessionServiceError error) {
    return error == SessionServiceError::TransportError ||
           error == SessionServiceError::InternalError ||
           error == SessionServiceError::RateLimited;
}

struct AuthorityResult {
    P2PAuthorityTransitionDecision decision =
        P2PAuthorityTransitionDecision::Rejected;
    SessionServiceError serviceError = SessionServiceError::None;
    std::string message;
    P2PBackendSessionInfo session;
    bool timedOut = false;
};

AuthorityResult runAuthorityTransition(
    const std::shared_ptr<IP2PSessionService>& service,
    const P2PSessionMemberCredential& member,
    const P2PMigrationContext& context,
    uint32_t retryMs, uint32_t timeoutMs,
    const std::shared_ptr<std::atomic<bool>>& cancelled) {
    const auto deadline = Clock::now() + std::chrono::milliseconds(timeoutMs);
    SessionServiceError lastError = SessionServiceError::None;
    std::string lastMessage;

    while (!cancelled->load() && Clock::now() < deadline) {
        const auto observed = service->getSession(context.sessionId);
        if (observed) {
            const auto& session = observed.value;
            if (session.epoch == context.nextEpoch) {
                if (session.hostPeerId == context.electedHostPeerId) {
                    return {P2PAuthorityTransitionDecision::Approved,
                            SessionServiceError::None, {}, session, false};
                }
                return {P2PAuthorityTransitionDecision::Rejected,
                        SessionServiceError::EpochConflict,
                        "backend epoch belongs to another Host", session, false};
            }
            if (session.epoch != context.currentEpoch ||
                session.hostPeerId == context.electedHostPeerId) {
                return {P2PAuthorityTransitionDecision::Rejected,
                        SessionServiceError::EpochConflict,
                        "backend authority does not match migration context",
                        session, false};
            }

            const bool localIsCandidate =
                member.peerId == context.electedHostPeerId;
            const bool localIsCurrentHost =
                member.peerId == session.hostPeerId;
            if ((context.graceful && localIsCurrentHost) ||
                (!context.graceful && localIsCandidate)) {
                P2PSessionClaimHostRequest claim;
                claim.member = member;
                claim.expectedEpoch = context.currentEpoch;
                claim.newHostPeerId = context.electedHostPeerId;
                const auto claimed = service->claimHost(claim);
                if (claimed) {
                    if (claimed.value.epoch == context.nextEpoch &&
                        claimed.value.hostPeerId == context.electedHostPeerId) {
                        return {P2PAuthorityTransitionDecision::Approved,
                                SessionServiceError::None, {}, claimed.value,
                                false};
                    }
                    return {P2PAuthorityTransitionDecision::Rejected,
                            SessionServiceError::ProtocolError,
                            "Host claim returned an unexpected authority",
                            claimed.value, false};
                }
                lastError = claimed.error;
                lastMessage = claimed.message;
                // A crash candidate receives Unauthorized while the previous
                // lease is still alive. Keep observing until it expires.
                if (context.graceful &&
                    claimed.error == SessionServiceError::Unauthorized) {
                    return {P2PAuthorityTransitionDecision::Rejected,
                            claimed.error, claimed.message, session, false};
                }
                if (claimed.error != SessionServiceError::Unauthorized &&
                    claimed.error != SessionServiceError::EpochConflict &&
                    claimed.error != SessionServiceError::HostLeaseExpired &&
                    !isTransient(claimed.error)) {
                    return {P2PAuthorityTransitionDecision::Rejected,
                            claimed.error, claimed.message, session, false};
                }
            } else if (context.graceful) {
                return {P2PAuthorityTransitionDecision::Rejected,
                        SessionServiceError::Unauthorized,
                        "graceful migration must be authorized by current Host",
                        session, false};
            }
        } else {
            lastError = observed.error;
            lastMessage = observed.message;
            if (!isTransient(observed.error)) {
                return {P2PAuthorityTransitionDecision::Rejected,
                        observed.error, observed.message, {}, false};
            }
        }

        const auto wake = Clock::now() + std::chrono::milliseconds(retryMs);
        while (!cancelled->load() && Clock::now() < wake) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    if (cancelled->load()) {
        return {P2PAuthorityTransitionDecision::Rejected,
                SessionServiceError::InternalError,
                "authority transition cancelled", {}, false};
    }
    return {P2PAuthorityTransitionDecision::Pending, lastError,
            lastMessage.empty() ? "authority transition timed out" : lastMessage,
            {}, true};
}

} // namespace

bool P2PSessionCoordinatorConfig::isValid() const {
    if (!p2p.isValid() || p2p.sessionId != 0 ||
        p2p.sessionEpoch != 0 || capacity == 0 ||
        capacity > kP2PMaxSessionMembers || authorityRetryMs == 0 ||
        authorityTimeoutMs == 0 || authorityTimeoutMs > 60000u ||
        authorityRetryMs > authorityTimeoutMs || !lease.isValid()) {
        return false;
    }
    return true;
}

struct P2PSessionCoordinator::Impl {
    enum class OperationKind : uint8_t {
        None,
        Create,
        Join,
        Assigned,
        Leave,
    };
    struct OperationResult {
        OperationKind kind = OperationKind::None;
        SessionServiceError error = SessionServiceError::None;
        std::string message;
        P2PSessionGrant grant;
    };

    Impl(INetworkSubSystem& networkRef,
         std::shared_ptr<IP2PSessionService> sessionService,
         P2PSessionCoordinatorConfig input)
        : network(networkRef), service(std::move(sessionService)),
          config(std::move(input)),
          cancelled(std::make_shared<std::atomic<bool>>(false)) {
        status.state = P2PSessionCoordinatorState::Idle;
        if (!service || !config.isValid()) {
            fail(P2PSessionCoordinatorError::InvalidConfiguration,
                 SessionServiceError::InvalidRequest,
                 "invalid P2P session coordinator configuration");
        }
    }

    ~Impl() {
        network.setP2PAuthorityTransitionGate({});
        cancelled->store(true);
        if (operation.valid()) operation.wait();
        if (authorityTask.valid()) authorityTask.wait();
        for (auto& task : cleanupTasks) task.wait();
        if (lease) lease->stop();
        network.disconnect();
    }

    void fail(P2PSessionCoordinatorError error,
              SessionServiceError serviceError,
              std::string message) {
        status.state = P2PSessionCoordinatorState::Failed;
        status.error = error;
        status.serviceError = serviceError;
        status.message = std::move(message);
    }

    void launchMembershipCleanup(
        const P2PSessionMemberCredential& member) {
        if (!member.isValid()) return;
        const auto sessionService = service;
        try {
            cleanupTasks.emplace_back(std::async(std::launch::async,
                [sessionService, member] {
                    try {
                        P2PSessionLeaveRequest request;
                        request.member = member;
                        (void)sessionService->leaveSession(request);
                    } catch (...) {
                        // Best-effort rollback. The backend's expiry policy is
                        // still the final cleanup boundary if transport is down.
                    }
                }));
        } catch (...) {
            // If no worker can be created, backend expiry remains the bounded
            // cleanup mechanism. Bootstrap failure itself must not throw.
        }
    }

    void consumeCleanupTasks() {
        auto it = cleanupTasks.begin();
        while (it != cleanupTasks.end()) {
            if (it->wait_for(std::chrono::milliseconds(0)) !=
                    std::future_status::ready) {
                ++it;
                continue;
            }
            it->get();
            it = cleanupTasks.erase(it);
        }
    }

    bool failBootstrap(const P2PSessionMemberCredential& member,
                       P2PSessionCoordinatorError error,
                       SessionServiceError serviceError,
                       std::string message) {
        if (lease) lease->stop();
        network.disconnect();
        network.setP2PAuthorityTransitionGate({});
        network.setP2PSessionJoinValidator({});
        (void)network.setP2PJoinTicket(nullptr, 0);
        lease.reset();
        signaling.reset();
        grant = {};
        launchMembershipCleanup(member);
        fail(error, serviceError, std::move(message));
        return false;
    }

    bool busy() const {
        return operation.valid() ||
               status.state == P2PSessionCoordinatorState::Creating ||
               status.state == P2PSessionCoordinatorState::Joining ||
               status.state == P2PSessionCoordinatorState::Starting ||
               status.state == P2PSessionCoordinatorState::Leaving;
    }

    bool startOperation(OperationKind kind, uint64_t joinSessionId = 0) {
        if (status.state != P2PSessionCoordinatorState::Idle || busy()) {
            status.error = P2PSessionCoordinatorError::Busy;
            status.message = "session coordinator is busy";
            return false;
        }
        status.error = P2PSessionCoordinatorError::None;
        status.serviceError = SessionServiceError::None;
        status.message.clear();
        status.state = kind == OperationKind::Create
            ? P2PSessionCoordinatorState::Creating
            : P2PSessionCoordinatorState::Joining;
        const auto sessionService = service;
        const auto localPeer = config.p2p.localPeerId;
        const uint16_t virtualPort = config.p2p.virtualPort;
        const uint16_t capacity = config.capacity;
        try {
            operation = std::async(std::launch::async,
                [sessionService, localPeer, virtualPort, capacity,
                 kind, joinSessionId]() {
                    OperationResult result;
                    result.kind = kind;
                    try {
                        if (kind == OperationKind::Create) {
                            P2PSessionCreateRequest request;
                            request.hostPeerId = localPeer;
                            request.virtualPort = virtualPort;
                            request.capacity = capacity;
                            auto serviceResult =
                                sessionService->createSession(request);
                            result.error = serviceResult.error;
                            result.message = std::move(serviceResult.message);
                            if (serviceResult) {
                                result.grant = std::move(serviceResult.value);
                            }
                        } else {
                            P2PSessionJoinRequest request;
                            request.sessionId = joinSessionId;
                            request.peerId = localPeer;
                            auto serviceResult =
                                sessionService->joinSession(request);
                            result.error = serviceResult.error;
                            result.message = std::move(serviceResult.message);
                            if (serviceResult) {
                                result.grant = std::move(serviceResult.value);
                            }
                        }
                    } catch (...) {
                        result.error = SessionServiceError::InternalError;
                        result.message = "session service operation threw";
                    }
                    return result;
                });
        } catch (...) {
            fail(P2PSessionCoordinatorError::SessionServiceRejected,
                 SessionServiceError::InternalError,
                 "failed to launch session service worker");
            return false;
        }
        return true;
    }

    bool bootstrap(OperationResult result) {
        status.state = P2PSessionCoordinatorState::Starting;
        if (result.error != SessionServiceError::None ||
            !result.grant.isValid()) {
            fail(P2PSessionCoordinatorError::SessionServiceRejected,
                 result.error == SessionServiceError::None
                    ? SessionServiceError::ProtocolError : result.error,
                 result.message.empty() ? "invalid session grant" : result.message);
            return false;
        }

        // Preserve the backend identity in failure diagnostics even if the
        // local transport rejects a later bootstrap step.
        status.backendSession = result.grant.session;
        P2PConfig p2p = config.p2p;
        std::shared_ptr<ISignalingTransport> transport;
        if (usesWebSocketSignaling(result.grant)) {
            WebSocketSignalingClientConfig signalingConfig;
            if (applyP2PSessionGrant(result.grant, p2p, signalingConfig)) {
                transport = std::make_shared<WebSocketSignalingClient>(
                    std::move(signalingConfig));
            }
        } else {
            SecureUdpSignalingClientConfig signalingConfig;
            if (applyP2PSessionGrant(result.grant, p2p, signalingConfig)) {
                transport = std::make_shared<SecureUdpSignalingClient>(
                    std::move(signalingConfig));
            }
        }
        if (!transport) {
            return failBootstrap(
                result.grant.member,
                P2PSessionCoordinatorError::NetworkConfigurationFailed,
                SessionServiceError::ProtocolError,
                "session grant could not be applied to P2P configuration");
        }
        if (!network.configureP2P(p2p, transport)) {
            return failBootstrap(
                result.grant.member,
                P2PSessionCoordinatorError::NetworkConfigurationFailed,
                SessionServiceError::None,
                "AYNetwork rejected P2P configuration");
        }

        grant = std::move(result.grant);
        signaling = std::move(transport);
        status.backendSession = grant.session;
        network.setP2PSessionJoinValidator(
            makeP2PSessionJoinValidator(grant));
        if (!network.setP2PJoinTicket(
                grant.joinTicket.data(), grant.joinTicket.size())) {
            return failBootstrap(
                grant.member,
                P2PSessionCoordinatorError::NetworkConfigurationFailed,
                SessionServiceError::None,
                "AYNetwork rejected Join Ticket");
        }
        network.setP2PHostMigrationEnabled(config.enableHostMigration);
        if (!network.setP2PAuthorityTransitionTimeoutMs(
                config.authorityTimeoutMs)) {
            return failBootstrap(
                grant.member,
                P2PSessionCoordinatorError::NetworkConfigurationFailed,
                SessionServiceError::None,
                "AYNetwork rejected authority timeout");
        }
        network.setP2PAuthorityTransitionGate(
            [this](const P2PMigrationContext& context) {
                return pollAuthority(context);
            });
        lease = std::make_unique<P2PSessionLeaseKeeper>(
            service, grant.member, config.lease);

        // Lobby and Matchmaking hand out a complete grant, so operation kind
        // cannot be used to infer the local role. The canonical backend Host
        // identity is also correct for direct create/join operations.
        const bool hosting =
            grant.session.hostPeerId == grant.member.peerId;
        const bool started = hosting
            ? network.listenP2P()
            : network.connectP2P(grant.session.hostPeerId);
        if (!started) {
            return failBootstrap(
                grant.member,
                P2PSessionCoordinatorError::NetworkStartFailed,
                SessionServiceError::None,
                hosting ? "failed to start P2P Host"
                        : "failed to connect to P2P Host");
        }
        if (hosting && !lease->start(grant.session.epoch)) {
            return failBootstrap(
                grant.member,
                P2PSessionCoordinatorError::LeaseStartFailed,
                SessionServiceError::InternalError,
                "P2P Host started but lease renewal did not");
        }
        status.state = hosting ? P2PSessionCoordinatorState::Hosting
                               : P2PSessionCoordinatorState::Connecting;
        return true;
    }

    void consumeOperation() {
        if (!operation.valid() ||
            operation.wait_for(std::chrono::milliseconds(0)) !=
                std::future_status::ready) return;
        OperationResult result = operation.get();
        if (result.kind == OperationKind::Leave) {
            if (result.error != SessionServiceError::None) {
                fail(P2PSessionCoordinatorError::SessionServiceRejected,
                     result.error, std::move(result.message));
            } else {
                grant = {};
                signaling.reset();
                lease.reset();
                status = {};
                status.state = P2PSessionCoordinatorState::Idle;
            }
            return;
        }
        (void)bootstrap(std::move(result));
    }

    P2PAuthorityTransitionDecision pollAuthority(
        const P2PMigrationContext& context) {
        if (!grant.isValid() || context.sessionId != grant.session.sessionId ||
            context.currentEpoch == 0 ||
            context.nextEpoch != context.currentEpoch + 1 ||
            !context.electedHostPeerId.isValid()) {
            status.error = P2PSessionCoordinatorError::AuthorityRejected;
            status.message = "invalid authority migration context";
            return P2PAuthorityTransitionDecision::Rejected;
        }

        if (!authorityContext || !sameContext(*authorityContext, context)) {
            if (authorityTask.valid() &&
                authorityTask.wait_for(std::chrono::milliseconds(0)) !=
                    std::future_status::ready) {
                // A prior transaction may have reached AYNetwork's deadline
                // just before its worker observed the same deadline. Keep the
                // new transaction frozen until that bounded worker exits.
                return P2PAuthorityTransitionDecision::Pending;
            }
            if (authorityTask.valid()) (void)authorityTask.get();
            authorityContext = context;
            authorityDecision.reset();
            const auto sessionService = service;
            const auto member = grant.member;
            const auto cancel = cancelled;
            const uint32_t retry = config.authorityRetryMs;
            const uint32_t timeout = config.authorityTimeoutMs;
            try {
                authorityTask = std::async(std::launch::async,
                    [sessionService, member, context, retry, timeout, cancel] {
                        return runAuthorityTransition(
                            sessionService, member, context,
                            retry, timeout, cancel);
                    });
            } catch (...) {
                status.error = P2PSessionCoordinatorError::AuthorityRejected;
                status.serviceError = SessionServiceError::InternalError;
                status.message = "failed to launch authority worker";
                authorityDecision =
                    P2PAuthorityTransitionDecision::Rejected;
                return *authorityDecision;
            }
            return P2PAuthorityTransitionDecision::Pending;
        }

        if (authorityDecision) return *authorityDecision;
        if (!authorityTask.valid() ||
            authorityTask.wait_for(std::chrono::milliseconds(0)) !=
                std::future_status::ready) {
            return P2PAuthorityTransitionDecision::Pending;
        }

        AuthorityResult result = authorityTask.get();
        status.serviceError = result.serviceError;
        status.message = result.message;
        if (result.session.isValid()) {
            status.backendSession = result.session;
            grant.session = result.session;
        }
        if (result.timedOut) {
            status.error = P2PSessionCoordinatorError::AuthorityTimeout;
            authorityDecision = P2PAuthorityTransitionDecision::Pending;
            return *authorityDecision;
        }
        if (result.decision == P2PAuthorityTransitionDecision::Approved) {
            status.error = P2PSessionCoordinatorError::None;
            status.message.clear();
            if (context.electedHostPeerId == grant.member.peerId) {
                if (!lease || !lease->start(context.nextEpoch)) {
                    status.error = P2PSessionCoordinatorError::LeaseStartFailed;
                    status.message =
                        "authority changed but Host lease could not start";
                    authorityDecision =
                        P2PAuthorityTransitionDecision::Rejected;
                    return *authorityDecision;
                }
            } else if (lease) {
                lease->stop();
            }
        } else {
            status.error = P2PSessionCoordinatorError::AuthorityRejected;
        }
        authorityDecision = result.decision;
        return *authorityDecision;
    }

    void clearCompletedAuthorityIfStable() {
        if (!authorityContext) return;
        const auto migration = status.networkSession.migration;
        if (migration != P2PHostMigrationState::Stable &&
            migration != P2PHostMigrationState::Disabled &&
            migration != P2PHostMigrationState::Failed) return;
        if (authorityTask.valid()) {
            if (authorityTask.wait_for(std::chrono::milliseconds(0)) !=
                    std::future_status::ready) return;
            (void)authorityTask.get();
        }
        authorityContext.reset();
        authorityDecision.reset();
    }

    INetworkSubSystem& network;
    std::shared_ptr<IP2PSessionService> service;
    P2PSessionCoordinatorConfig config;
    P2PSessionCoordinatorStatus status;
    P2PSessionGrant grant;
    std::shared_ptr<ISignalingTransport> signaling;
    std::unique_ptr<P2PSessionLeaseKeeper> lease;
    std::future<OperationResult> operation;
    std::future<AuthorityResult> authorityTask;
    std::vector<std::future<void>> cleanupTasks;
    std::optional<P2PMigrationContext> authorityContext;
    std::optional<P2PAuthorityTransitionDecision> authorityDecision;
    std::shared_ptr<std::atomic<bool>> cancelled;
};

P2PSessionCoordinator::P2PSessionCoordinator(
    INetworkSubSystem& network,
    std::shared_ptr<IP2PSessionService> service,
    P2PSessionCoordinatorConfig config)
    : _impl(std::make_unique<Impl>(
          network, std::move(service), std::move(config))) {}

P2PSessionCoordinator::~P2PSessionCoordinator() = default;

bool P2PSessionCoordinator::createSession() {
    if (_impl->config.p2p.virtualPort == 0) {
        _impl->fail(P2PSessionCoordinatorError::InvalidConfiguration,
                    SessionServiceError::InvalidRequest,
                    "Host virtual port must be non-zero");
        return false;
    }
    return _impl->startOperation(Impl::OperationKind::Create);
}

bool P2PSessionCoordinator::joinSession(uint64_t sessionId) {
    if (sessionId == 0) {
        _impl->status.error =
            P2PSessionCoordinatorError::InvalidConfiguration;
        _impl->status.message = "session id must be non-zero";
        return false;
    }
    return _impl->startOperation(Impl::OperationKind::Join, sessionId);
}

bool P2PSessionCoordinator::startAssignedSession(P2PSessionGrant grant) {
    if (_impl->status.state != P2PSessionCoordinatorState::Idle ||
        _impl->busy()) {
        _impl->status.error = P2PSessionCoordinatorError::Busy;
        _impl->status.message = "session coordinator is busy";
        return false;
    }
    if (!grant.isValid() ||
        grant.member.peerId != _impl->config.p2p.localPeerId) {
        _impl->fail(P2PSessionCoordinatorError::InvalidConfiguration,
                    SessionServiceError::ProtocolError,
                    "assigned session grant is invalid or belongs to another peer");
        return false;
    }
    Impl::OperationResult result;
    result.kind = Impl::OperationKind::Assigned;
    result.grant = std::move(grant);
    return _impl->bootstrap(std::move(result));
}

bool P2PSessionCoordinator::leaveSession() {
    if (_impl->busy() || !_impl->grant.member.isValid()) {
        _impl->status.error = P2PSessionCoordinatorError::Busy;
        _impl->status.message = "no idle active membership to leave";
        return false;
    }
    if (_impl->status.networkSession.migration !=
            P2PHostMigrationState::Disabled &&
        _impl->status.networkSession.migration !=
            P2PHostMigrationState::Stable) {
        _impl->status.error = P2PSessionCoordinatorError::Busy;
        _impl->status.message = "cannot leave during Host migration";
        return false;
    }
    if (_impl->lease) _impl->lease->stop();
    _impl->network.disconnect();
    _impl->network.setP2PAuthorityTransitionGate({});
    _impl->status.state = P2PSessionCoordinatorState::Leaving;
    const auto service = _impl->service;
    const auto member = _impl->grant.member;
    try {
        _impl->operation = std::async(std::launch::async,
            [service, member] {
                Impl::OperationResult result;
                result.kind = Impl::OperationKind::Leave;
                try {
                    P2PSessionLeaveRequest request;
                    request.member = member;
                    const auto serviceResult = service->leaveSession(request);
                    result.error = serviceResult.error;
                    result.message = serviceResult.message;
                } catch (...) {
                    result.error = SessionServiceError::InternalError;
                    result.message = "session leave operation threw";
                }
                return result;
            });
    } catch (...) {
        _impl->fail(P2PSessionCoordinatorError::SessionServiceRejected,
                    SessionServiceError::InternalError,
                    "failed to launch session leave worker");
        return false;
    }
    return true;
}

bool P2PSessionCoordinator::requestHostMigration() {
    if (_impl->busy() || !_impl->grant.isValid() ||
        _impl->status.networkSession.role != P2PSessionRole::Host ||
        !_impl->network.requestP2PHostMigration()) {
        _impl->status.error = P2PSessionCoordinatorError::Busy;
        _impl->status.message = "Host migration could not start";
        return false;
    }
    _impl->status.error = P2PSessionCoordinatorError::None;
    _impl->status.serviceError = SessionServiceError::None;
    _impl->status.message.clear();
    _impl->status.state = P2PSessionCoordinatorState::Migrating;
    return true;
}

void P2PSessionCoordinator::update() {
    _impl->consumeCleanupTasks();
    _impl->consumeOperation();
    _impl->status.networkSession = _impl->network.getP2PSessionInfo();
    _impl->clearCompletedAuthorityIfStable();
    if (_impl->lease) {
        const auto leaseSession = _impl->lease->getLastSession();
        if (leaseSession.isValid()) {
            _impl->status.backendSession = leaseSession;
            _impl->grant.session = leaseSession;
        }
    }
    if (_impl->status.state == P2PSessionCoordinatorState::Failed ||
        _impl->status.state == P2PSessionCoordinatorState::Idle ||
        _impl->status.state == P2PSessionCoordinatorState::Creating ||
        _impl->status.state == P2PSessionCoordinatorState::Joining ||
        _impl->status.state == P2PSessionCoordinatorState::Starting ||
        _impl->status.state == P2PSessionCoordinatorState::Leaving) {
        return;
    }

    const auto& session = _impl->status.networkSession;
    if (session.migration == P2PHostMigrationState::Failed) {
        const auto error = session.migrationFailure ==
                P2PMigrationFailureReason::AuthorityRejected
            ? P2PSessionCoordinatorError::AuthorityRejected
            : (session.migrationFailure ==
                    P2PMigrationFailureReason::AuthorityTimeout
                ? P2PSessionCoordinatorError::AuthorityTimeout
                : P2PSessionCoordinatorError::MigrationFailed);
        _impl->clearCompletedAuthorityIfStable();
        _impl->fail(error, _impl->status.serviceError,
                    "Host migration failed");
        return;
    }
    if (session.migrationFailure != P2PMigrationFailureReason::None) {
        _impl->status.error = session.migrationFailure ==
                P2PMigrationFailureReason::AuthorityRejected
            ? P2PSessionCoordinatorError::AuthorityRejected
            : (session.migrationFailure ==
                    P2PMigrationFailureReason::AuthorityTimeout
                ? P2PSessionCoordinatorError::AuthorityTimeout
                : P2PSessionCoordinatorError::MigrationFailed);
        if (_impl->status.message.empty()) {
            _impl->status.message = "Host migration was aborted";
        }
    }
    if (session.migration != P2PHostMigrationState::Disabled &&
        session.migration != P2PHostMigrationState::Stable) {
        _impl->status.state = P2PSessionCoordinatorState::Migrating;
    } else if (session.role == P2PSessionRole::Host) {
        if (_impl->lease && !_impl->lease->isRunning() && session.epoch != 0) {
            if (!_impl->lease->start(session.epoch)) {
                _impl->fail(P2PSessionCoordinatorError::LeaseStartFailed,
                            SessionServiceError::InternalError,
                            "promoted Host lease could not start");
                return;
            }
        }
        _impl->status.state = P2PSessionCoordinatorState::Hosting;
    } else if (session.state == P2PSessionState::Active) {
        if (_impl->lease && _impl->lease->isRunning()) _impl->lease->stop();
        _impl->status.state = P2PSessionCoordinatorState::Active;
    } else {
        _impl->status.state = P2PSessionCoordinatorState::Connecting;
    }
}

bool P2PSessionCoordinator::reset() {
    if (_impl->busy() || (_impl->authorityTask.valid() &&
        _impl->authorityTask.wait_for(std::chrono::milliseconds(0)) !=
            std::future_status::ready)) return false;
    _impl->network.setP2PAuthorityTransitionGate({});
    _impl->network.disconnect();
    if (_impl->lease) _impl->lease->stop();
    _impl->lease.reset();
    _impl->signaling.reset();
    _impl->grant = {};
    _impl->authorityContext.reset();
    _impl->authorityDecision.reset();
    if (_impl->authorityTask.valid()) (void)_impl->authorityTask.get();
    _impl->status = {};
    _impl->status.state = P2PSessionCoordinatorState::Idle;
    return true;
}

P2PSessionCoordinatorStatus P2PSessionCoordinator::getStatus() const {
    return _impl->status;
}

P2PSessionGrant P2PSessionCoordinator::getGrant() const {
    return _impl->grant;
}

} // namespace ayt::net
