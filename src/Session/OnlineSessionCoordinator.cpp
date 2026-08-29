#include <AYNetwork/Session/OnlineSessionCoordinator.h>

#include <algorithm>
#include <chrono>
#include <future>
#include <utility>

namespace ayt::net
{
namespace
{

uint64_t steadyMilliseconds() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
}

bool containsPeer(const std::vector<PeerId>& peers, const PeerId& peer) {
    return std::find(peers.begin(), peers.end(), peer) != peers.end();
}

bool retryable(OnlineServiceError error) {
    return error == OnlineServiceError::BackendUnavailable ||
           error == OnlineServiceError::InternalError;
}

} // namespace

bool OnlineSessionCoordinatorConfig::isValid() const {
    return localPeerId.isValid() && matchmakingPollIntervalMs != 0 &&
           matchmakingPollIntervalMs <= 60000u &&
           maxConsecutivePollFailures != 0 &&
           maxConsecutivePollFailures <= 1000u;
}

struct OnlineSessionCoordinator::Impl {
    enum class OperationKind : uint8_t {
        None,
        ListLobbies,
        CreateLobby,
        JoinLobby,
        RefreshLobby,
        UpdateLobby,
        LeaveLobby,
        LaunchLobby,
        EnqueueMatch,
        PollMatch,
        CancelMatch,
    };

    struct OperationResult {
        OperationKind kind = OperationKind::None;
        OnlineServiceError error = OnlineServiceError::None;
        std::string message;
        LobbyInfo lobby;
        std::vector<LobbyInfo> lobbies;
        LobbyLaunchResult launch;
        MatchTicketInfo ticket;
    };

    Impl(std::shared_ptr<ILobbyService> lobbyService,
         std::shared_ptr<IMatchmakingService> matchmakingService,
         P2PSessionCoordinator& p2pCoordinator,
         OnlineSessionCoordinatorConfig input,
         std::shared_ptr<IDedicatedSessionConnector> dedicatedConnector)
        : lobbies(std::move(lobbyService)),
          matchmaking(std::move(matchmakingService)),
          p2p(p2pCoordinator),
          dedicated(std::move(dedicatedConnector)),
          config(std::move(input)) {
        if (!lobbies || !matchmaking || !config.isValid()) {
            fail(OnlineSessionCoordinatorError::InvalidConfiguration,
                 OnlineServiceError::InvalidRequest,
                 "invalid Online Session coordinator configuration");
        }
    }

    ~Impl() {
        if (operation.valid()) operation.wait();
        if (dedicated && status.topology == OnlineSessionTopology::Dedicated) {
            dedicated->disconnect();
        }
    }

    uint64_t now() const {
        return config.nowMilliseconds ? config.nowMilliseconds()
                                      : steadyMilliseconds();
    }

    bool hasLobby() const {
        return status.lobby.lobbyId != 0 &&
               containsPeer(status.lobby.members, config.localPeerId);
    }

    bool hasLiveMatchTicket() const {
        return status.matchTicket.ticketId != 0 &&
            (status.matchTicket.state == MatchTicketState::Queued ||
             status.matchTicket.state == MatchTicketState::Matching);
    }

    bool operationRunning() const { return operation.valid(); }

    void clearError() {
        status.error = OnlineSessionCoordinatorError::None;
        status.serviceError = OnlineServiceError::None;
        status.p2pError = P2PSessionCoordinatorError::None;
        status.message.clear();
    }

    void fail(OnlineSessionCoordinatorError error,
              OnlineServiceError serviceError,
              std::string message) {
        status.state = OnlineSessionCoordinatorState::Failed;
        status.error = error;
        status.serviceError = serviceError;
        status.p2pError = p2p.getStatus().error;
        status.message = std::move(message);
    }

    bool rejectBusy(const char* message = "Online Session coordinator is busy") {
        status.error = OnlineSessionCoordinatorError::Busy;
        status.message = message;
        return false;
    }

    template <typename Work>
    bool startOperation(OperationKind kind,
                        OnlineSessionCoordinatorState state,
                        Work&& work) {
        if (operationRunning()) return rejectBusy();
        clearError();
        status.state = state;
        try {
            operation = std::async(
                std::launch::async, std::forward<Work>(work));
        } catch (...) {
            fail(OnlineSessionCoordinatorError::WorkerStartFailed,
                 OnlineServiceError::InternalError,
                 "failed to launch Online Services worker");
            return false;
        }
        (void)kind;
        return true;
    }

    static OperationResult exceptionResult(OperationKind kind) {
        OperationResult result;
        result.kind = kind;
        result.error = OnlineServiceError::InternalError;
        result.message = "Online Services operation threw";
        return result;
    }

    bool startList(ListLobbiesRequest request) {
        if (status.state != OnlineSessionCoordinatorState::Idle ||
            operationRunning()) return rejectBusy();
        lobbyResults.clear();
        const auto service = lobbies;
        return startOperation(OperationKind::ListLobbies,
            OnlineSessionCoordinatorState::ListingLobbies,
            [service, request = std::move(request)]() mutable {
                OperationResult result;
                result.kind = OperationKind::ListLobbies;
                try {
                    auto value = service->listLobbies(request);
                    result.error = value.error;
                    result.message = std::move(value.message);
                    if (value) result.lobbies = std::move(value.value);
                } catch (...) {
                    return exceptionResult(OperationKind::ListLobbies);
                }
                return result;
            });
    }

    bool startCreate(CreateLobbyRequest request) {
        if (status.state != OnlineSessionCoordinatorState::Idle ||
            operationRunning()) return rejectBusy();
        request.ownerPeerId = config.localPeerId;
        const auto service = lobbies;
        return startOperation(OperationKind::CreateLobby,
            OnlineSessionCoordinatorState::CreatingLobby,
            [service, request = std::move(request)]() mutable {
                OperationResult result;
                result.kind = OperationKind::CreateLobby;
                try {
                    auto value = service->createLobby(request);
                    result.error = value.error;
                    result.message = std::move(value.message);
                    if (value) result.lobby = std::move(value.value);
                } catch (...) {
                    return exceptionResult(OperationKind::CreateLobby);
                }
                return result;
            });
    }

    bool startJoin(LobbyId lobbyId) {
        if (status.state != OnlineSessionCoordinatorState::Idle ||
            operationRunning()) return rejectBusy();
        if (lobbyId == 0) {
            status.error = OnlineSessionCoordinatorError::InvalidConfiguration;
            status.message = "Lobby id must be non-zero";
            return false;
        }
        const auto service = lobbies;
        const PeerId peer = config.localPeerId;
        return startOperation(OperationKind::JoinLobby,
            OnlineSessionCoordinatorState::JoiningLobby,
            [service, lobbyId, peer] {
                OperationResult result;
                result.kind = OperationKind::JoinLobby;
                try {
                    auto value = service->joinLobby(lobbyId, peer);
                    result.error = value.error;
                    result.message = std::move(value.message);
                    if (value) result.lobby = std::move(value.value);
                } catch (...) {
                    return exceptionResult(OperationKind::JoinLobby);
                }
                return result;
            });
    }

    bool startRefresh() {
        if (status.state != OnlineSessionCoordinatorState::InLobby ||
            operationRunning() || !hasLobby()) return rejectBusy();
        const auto service = lobbies;
        const LobbyId lobbyId = status.lobby.lobbyId;
        return startOperation(OperationKind::RefreshLobby,
            OnlineSessionCoordinatorState::RefreshingLobby,
            [service, lobbyId] {
                OperationResult result;
                result.kind = OperationKind::RefreshLobby;
                try {
                    auto value = service->getLobby(lobbyId);
                    result.error = value.error;
                    result.message = std::move(value.message);
                    if (value) result.lobby = std::move(value.value);
                } catch (...) {
                    return exceptionResult(OperationKind::RefreshLobby);
                }
                return result;
            });
    }

    bool startUpdateLobby(std::string name) {
        if (status.state != OnlineSessionCoordinatorState::InLobby ||
            operationRunning() || !hasLobby()) return rejectBusy();
        UpdateLobbyRequest request;
        request.lobbyId = status.lobby.lobbyId;
        request.actorPeerId = config.localPeerId;
        request.expectedRevision = status.lobby.revision;
        request.name = std::move(name);
        const auto service = lobbies;
        return startOperation(OperationKind::UpdateLobby,
            OnlineSessionCoordinatorState::UpdatingLobby,
            [service, request = std::move(request)]() mutable {
                OperationResult result;
                result.kind = OperationKind::UpdateLobby;
                try {
                    auto value = service->updateLobby(request);
                    result.error = value.error;
                    result.message = std::move(value.message);
                    if (value) result.lobby = std::move(value.value);
                } catch (...) {
                    return exceptionResult(OperationKind::UpdateLobby);
                }
                return result;
            });
    }

    bool startLeaveLobby() {
        const bool recoverableFailure =
            status.state == OnlineSessionCoordinatorState::Failed &&
            status.topology == OnlineSessionTopology::None && !hasLiveMatchTicket();
        const bool afterSession =
            status.state == OnlineSessionCoordinatorState::LeavingSession &&
            status.topology == OnlineSessionTopology::None;
        if ((status.state != OnlineSessionCoordinatorState::InLobby &&
             !recoverableFailure && !afterSession) ||
            operationRunning() || !hasLobby()) {
            return rejectBusy("no idle Lobby membership to leave");
        }
        const auto service = lobbies;
        const LobbyId lobbyId = status.lobby.lobbyId;
        const PeerId peer = config.localPeerId;
        return startOperation(OperationKind::LeaveLobby,
            OnlineSessionCoordinatorState::LeavingLobby,
            [service, lobbyId, peer] {
                OperationResult result;
                result.kind = OperationKind::LeaveLobby;
                try {
                    auto value = service->leaveLobby(lobbyId, peer);
                    result.error = value.error;
                    result.message = std::move(value.message);
                    if (value) result.lobby = std::move(value.value);
                } catch (...) {
                    return exceptionResult(OperationKind::LeaveLobby);
                }
                return result;
            });
    }

    bool startLaunchLobby(uint16_t virtualPort) {
        if (status.state != OnlineSessionCoordinatorState::InLobby ||
            operationRunning() || !hasLobby()) return rejectBusy();
        if (virtualPort == 0) {
            status.error = OnlineSessionCoordinatorError::InvalidConfiguration;
            status.message = "P2P virtual port must be non-zero";
            return false;
        }
        LaunchLobbyRequest request;
        request.lobbyId = status.lobby.lobbyId;
        request.actorPeerId = config.localPeerId;
        request.expectedRevision = status.lobby.revision;
        request.virtualPort = virtualPort;
        const auto service = lobbies;
        return startOperation(OperationKind::LaunchLobby,
            OnlineSessionCoordinatorState::LaunchingLobby,
            [service, request] {
                OperationResult result;
                result.kind = OperationKind::LaunchLobby;
                try {
                    auto value = service->launchLobbyP2P(request);
                    result.error = value.error;
                    result.message = std::move(value.message);
                    if (value) result.launch = std::move(value.value);
                } catch (...) {
                    return exceptionResult(OperationKind::LaunchLobby);
                }
                return result;
            });
    }

    bool startEnqueue(MatchmakingRequest request) {
        if (status.state != OnlineSessionCoordinatorState::Idle ||
            operationRunning()) return rejectBusy();
        if (request.partyMembers.empty()) {
            request.partyMembers.push_back(config.localPeerId);
        } else if (!containsPeer(request.partyMembers, config.localPeerId)) {
            status.error = OnlineSessionCoordinatorError::InvalidConfiguration;
            status.message = "matchmaking party does not contain local peer";
            return false;
        }
        cancelRequested = false;
        reconcileCancellation = false;
        pollFailures = 0;
        const auto service = matchmaking;
        return startOperation(OperationKind::EnqueueMatch,
            OnlineSessionCoordinatorState::Queueing,
            [service, request = std::move(request)]() mutable {
                OperationResult result;
                result.kind = OperationKind::EnqueueMatch;
                try {
                    auto value = service->enqueueMatch(request);
                    result.error = value.error;
                    result.message = std::move(value.message);
                    if (value) result.ticket = std::move(value.value);
                } catch (...) {
                    return exceptionResult(OperationKind::EnqueueMatch);
                }
                return result;
            });
    }

    bool startPoll() {
        if (operationRunning() || status.matchTicket.ticketId == 0) return false;
        const auto service = matchmaking;
        const MatchTicketId ticketId = status.matchTicket.ticketId;
        const PeerId peer = config.localPeerId;
        return startOperation(OperationKind::PollMatch,
            cancelRequested ? OnlineSessionCoordinatorState::CancellingMatch
                            : OnlineSessionCoordinatorState::Queueing,
            [service, ticketId, peer] {
                OperationResult result;
                result.kind = OperationKind::PollMatch;
                try {
                    auto value = service->getMatch(ticketId, peer);
                    result.error = value.error;
                    result.message = std::move(value.message);
                    if (value) result.ticket = std::move(value.value);
                } catch (...) {
                    return exceptionResult(OperationKind::PollMatch);
                }
                return result;
            });
    }

    bool startCancel() {
        if (operationRunning() || status.matchTicket.ticketId == 0) return false;
        const auto service = matchmaking;
        const MatchTicketId ticketId = status.matchTicket.ticketId;
        const PeerId peer = config.localPeerId;
        return startOperation(OperationKind::CancelMatch,
            OnlineSessionCoordinatorState::CancellingMatch,
            [service, ticketId, peer] {
                OperationResult result;
                result.kind = OperationKind::CancelMatch;
                try {
                    auto value = service->cancelMatch(ticketId, peer);
                    result.error = value.error;
                    result.message = std::move(value.message);
                    if (value) result.ticket = std::move(value.value);
                } catch (...) {
                    return exceptionResult(OperationKind::CancelMatch);
                }
                return result;
            });
    }

    const P2PSessionGrant* localGrant(
        const std::vector<P2PSessionGrant>& grants) const {
        const P2PSessionGrant* selected = nullptr;
        for (const auto& grant : grants) {
            if (grant.member.peerId != config.localPeerId) continue;
            if (selected) return nullptr;
            selected = &grant;
        }
        return selected;
    }

    bool beginP2PAssignment(const std::vector<P2PSessionGrant>& grants) {
        const P2PSessionGrant* selected = localGrant(grants);
        if (!selected || !selected->isValid()) {
            fail(OnlineSessionCoordinatorError::InvalidAssignment,
                 OnlineServiceError::InternalError,
                 "P2P assignment does not contain exactly one valid local grant");
            return false;
        }
        if (!p2p.startAssignedSession(*selected)) {
            const auto p2pStatus = p2p.getStatus();
            status.p2pError = p2pStatus.error;
            fail(OnlineSessionCoordinatorError::P2PStartFailed,
                 OnlineServiceError::None,
                 p2pStatus.message.empty()
                    ? "P2P assignment could not start" : p2pStatus.message);
            return false;
        }
        status.topology = OnlineSessionTopology::P2P;
        status.state = OnlineSessionCoordinatorState::Connecting;
        return true;
    }

    bool beginAssignment(MatchAssignment assignment) {
        if (!assignment.content.isValid()) {
            fail(OnlineSessionCoordinatorError::InvalidAssignment,
                 OnlineServiceError::InternalError,
                 "match assignment omitted a valid content descriptor");
            return false;
        }
        status.assignment = std::move(assignment);
        status.state = OnlineSessionCoordinatorState::Assigned;
        if (status.assignment.topology == MatchTopology::P2P) {
            if (status.assignment.dedicated.isValid()) {
                fail(OnlineSessionCoordinatorError::InvalidAssignment,
                     OnlineServiceError::InternalError,
                     "P2P assignment also contains a Dedicated reservation");
                return false;
            }
            return beginP2PAssignment(status.assignment.p2pGrants);
        }
        if (status.assignment.topology != MatchTopology::Dedicated ||
            !status.assignment.p2pGrants.empty() ||
            !status.assignment.dedicated.isValid()) {
            fail(OnlineSessionCoordinatorError::InvalidAssignment,
                 OnlineServiceError::InternalError,
                 "match assignment topology or credentials are invalid");
            return false;
        }
        if (!dedicated) {
            fail(OnlineSessionCoordinatorError::DedicatedUnavailable,
                 OnlineServiceError::None,
                 "Dedicated assignment received without a session connector");
            return false;
        }
        if (!dedicated->connect(status.assignment.dedicated)) {
            fail(OnlineSessionCoordinatorError::DedicatedStartFailed,
                 OnlineServiceError::None,
                 "Dedicated connector rejected the reservation");
            return false;
        }
        status.topology = OnlineSessionTopology::Dedicated;
        status.dedicated = dedicated->getStatus();
        status.state = OnlineSessionCoordinatorState::Connecting;
        return true;
    }

    void clearMatchAndAssignment() {
        status.matchTicket = {};
        status.assignment = {};
        status.topology = OnlineSessionTopology::None;
        status.dedicated = {};
        cancelRequested = false;
        reconcileCancellation = false;
        pollFailures = 0;
    }

    void returnToLobbyOrIdle() {
        clearError();
        status.state = hasLobby() ? OnlineSessionCoordinatorState::InLobby
                                  : OnlineSessionCoordinatorState::Idle;
    }

    void consumeMatchTicket(MatchTicketInfo ticket) {
        status.matchTicket = std::move(ticket);
        pollFailures = 0;
        if (status.matchTicket.state == MatchTicketState::Queued ||
            status.matchTicket.state == MatchTicketState::Matching) {
            status.state = cancelRequested
                ? OnlineSessionCoordinatorState::CancellingMatch
                : OnlineSessionCoordinatorState::Queueing;
            reconcileCancellation = false;
            nextPollAtMs = now() + config.matchmakingPollIntervalMs;
            return;
        }
        if (status.matchTicket.state == MatchTicketState::Cancelled) {
            clearMatchAndAssignment();
            returnToLobbyOrIdle();
            return;
        }
        if (status.matchTicket.state == MatchTicketState::Failed) {
            fail(OnlineSessionCoordinatorError::OnlineServiceRejected,
                 OnlineServiceError::InternalError,
                 status.matchTicket.failure.empty()
                    ? "matchmaking failed" : status.matchTicket.failure);
            return;
        }
        if (status.matchTicket.state != MatchTicketState::Matched) {
            fail(OnlineSessionCoordinatorError::InvalidAssignment,
                 OnlineServiceError::InternalError,
                 "matchmaking returned an unknown ticket state");
            return;
        }
        if (cancelRequested) {
            status.message =
                "match assignment committed before cancellation completed";
        }
        cancelRequested = false;
        reconcileCancellation = false;
        (void)beginAssignment(status.matchTicket.assignment);
    }

    void handlePollFailure(OperationResult result) {
        ++pollFailures;
        status.serviceError = result.error;
        status.message = std::move(result.message);
        if (retryable(status.serviceError) &&
            pollFailures <= config.maxConsecutivePollFailures) {
            status.state = cancelRequested
                ? OnlineSessionCoordinatorState::CancellingMatch
                : OnlineSessionCoordinatorState::Queueing;
            nextPollAtMs = now() + config.matchmakingPollIntervalMs;
            return;
        }
        fail(OnlineSessionCoordinatorError::OnlineServiceRejected,
             status.serviceError,
             status.message.empty() ? "match polling failed" : status.message);
    }

    void consumeOperation() {
        if (!operation.valid() ||
            operation.wait_for(std::chrono::milliseconds(0)) !=
                std::future_status::ready) return;
        OperationResult result = operation.get();
        if (result.error != OnlineServiceError::None) {
            if (result.kind == OperationKind::LeaveLobby &&
                result.error == OnlineServiceError::NotFound) {
                status.lobby = {};
                clearMatchAndAssignment();
                status.state = OnlineSessionCoordinatorState::Idle;
                status.serviceError = result.error;
                status.message = result.message.empty()
                    ? "Lobby membership was already absent"
                    : std::move(result.message);
                return;
            }
            if (result.kind == OperationKind::RefreshLobby &&
                result.error == OnlineServiceError::NotFound) {
                status.lobby = {};
                status.state = OnlineSessionCoordinatorState::Idle;
                status.serviceError = result.error;
                status.message = result.message.empty()
                    ? "Lobby no longer exists" : std::move(result.message);
                return;
            }
            if (result.kind == OperationKind::CancelMatch &&
                result.error == OnlineServiceError::NotFound) {
                clearMatchAndAssignment();
                status.state = hasLobby()
                    ? OnlineSessionCoordinatorState::InLobby
                    : OnlineSessionCoordinatorState::Idle;
                status.serviceError = result.error;
                status.message = result.message.empty()
                    ? "match ticket was already absent"
                    : std::move(result.message);
                return;
            }
            if (result.kind == OperationKind::PollMatch) {
                handlePollFailure(std::move(result));
                return;
            }
            if (result.kind == OperationKind::CancelMatch &&
                result.error == OnlineServiceError::Conflict) {
                status.serviceError = result.error;
                status.message = result.message.empty()
                    ? "match cancellation raced with assignment"
                    : std::move(result.message);
                cancelRequested = true;
                reconcileCancellation = true;
                status.state = OnlineSessionCoordinatorState::CancellingMatch;
                nextPollAtMs = now();
                return;
            }
            if (result.kind == OperationKind::CancelMatch &&
                retryable(result.error) &&
                ++pollFailures <= config.maxConsecutivePollFailures) {
                status.serviceError = result.error;
                status.message = std::move(result.message);
                status.state = OnlineSessionCoordinatorState::CancellingMatch;
                nextPollAtMs = now() + config.matchmakingPollIntervalMs;
                return;
            }
            fail(OnlineSessionCoordinatorError::OnlineServiceRejected,
                 result.error,
                 result.message.empty()
                    ? "Online Services operation was rejected"
                    : std::move(result.message));
            return;
        }

        clearError();
        switch (result.kind) {
        case OperationKind::ListLobbies:
            lobbyResults = std::move(result.lobbies);
            status.state = OnlineSessionCoordinatorState::Idle;
            break;
        case OperationKind::CreateLobby:
        case OperationKind::JoinLobby:
        case OperationKind::RefreshLobby:
        case OperationKind::UpdateLobby:
            if (!result.lobby.isValid() ||
                !containsPeer(result.lobby.members, config.localPeerId)) {
                fail(OnlineSessionCoordinatorError::OnlineServiceRejected,
                     OnlineServiceError::InternalError,
                     "Lobby response omitted the local membership");
                break;
            }
            status.lobby = std::move(result.lobby);
            status.state = OnlineSessionCoordinatorState::InLobby;
            break;
        case OperationKind::LeaveLobby:
            status.lobby = {};
            clearMatchAndAssignment();
            status.state = OnlineSessionCoordinatorState::Idle;
            break;
        case OperationKind::LaunchLobby: {
            if (!result.launch.lobby.isValid() ||
                !containsPeer(result.launch.lobby.members, config.localPeerId)) {
                fail(OnlineSessionCoordinatorError::InvalidAssignment,
                     OnlineServiceError::InternalError,
                     "Lobby launch response omitted the local membership");
                break;
            }
            status.lobby = std::move(result.launch.lobby);
            MatchAssignment assignment;
            assignment.topology = MatchTopology::P2P;
            assignment.content = status.lobby.content;
            assignment.p2pGrants = std::move(result.launch.memberGrants);
            (void)beginAssignment(std::move(assignment));
            break;
        }
        case OperationKind::EnqueueMatch:
        case OperationKind::PollMatch:
        case OperationKind::CancelMatch:
            consumeMatchTicket(std::move(result.ticket));
            break;
        default:
            fail(OnlineSessionCoordinatorError::OnlineServiceRejected,
                 OnlineServiceError::InternalError,
                 "Online Services worker returned an unknown operation");
            break;
        }
    }

    void updateMatchmaking() {
        if (operationRunning() || now() < nextPollAtMs) return;
        if (cancelRequested && !reconcileCancellation) {
            (void)startCancel();
        } else {
            (void)startPoll();
        }
    }

    void syncP2P() {
        const auto p2pStatus = p2p.getStatus();
        status.p2pError = p2pStatus.error;
        if (p2pStatus.state == P2PSessionCoordinatorState::Failed) {
            fail(OnlineSessionCoordinatorError::P2PFailed,
                 OnlineServiceError::None,
                 p2pStatus.message.empty()
                    ? "P2P session failed" : p2pStatus.message);
            return;
        }
        if (p2pStatus.state == P2PSessionCoordinatorState::Hosting ||
            p2pStatus.state == P2PSessionCoordinatorState::Active) {
            status.state = OnlineSessionCoordinatorState::InSession;
            return;
        }
        if (p2pStatus.state == P2PSessionCoordinatorState::Idle) {
            fail(OnlineSessionCoordinatorError::P2PFailed,
                 OnlineServiceError::None,
                 "P2P session returned to Idle before becoming active");
            return;
        }
        status.state = OnlineSessionCoordinatorState::Connecting;
    }

    void syncDedicated() {
        if (!dedicated) {
            fail(OnlineSessionCoordinatorError::DedicatedUnavailable,
                 OnlineServiceError::None,
                 "Dedicated connector is unavailable");
            return;
        }
        dedicated->update();
        status.dedicated = dedicated->getStatus();
        if (status.dedicated.state == DedicatedSessionConnectionState::Active) {
            status.state = OnlineSessionCoordinatorState::InSession;
        } else if (status.dedicated.state ==
                       DedicatedSessionConnectionState::Connecting) {
            status.state = OnlineSessionCoordinatorState::Connecting;
        } else if (status.dedicated.state ==
                       DedicatedSessionConnectionState::Failed) {
            fail(OnlineSessionCoordinatorError::DedicatedFailed,
                 OnlineServiceError::None,
                 status.dedicated.message.empty()
                    ? "Dedicated connection failed" : status.dedicated.message);
        } else {
            fail(OnlineSessionCoordinatorError::DedicatedFailed,
                 OnlineServiceError::None,
                 "Dedicated connection returned to Idle before becoming active");
        }
    }

    void finishSessionLeave() {
        clearMatchAndAssignment();
        if (hasLobby()) {
            if (!startLeaveLobby()) {
                fail(OnlineSessionCoordinatorError::OnlineServiceRejected,
                     status.serviceError,
                     status.message.empty()
                        ? "failed to leave Lobby after session"
                        : status.message);
            }
        } else {
            status.lobby = {};
            returnToLobbyOrIdle();
        }
    }

    void syncLeaving() {
        if (status.topology == OnlineSessionTopology::P2P) {
            const auto p2pStatus = p2p.getStatus();
            status.p2pError = p2pStatus.error;
            if (p2pStatus.state == P2PSessionCoordinatorState::Idle) {
                finishSessionLeave();
            } else if (p2pStatus.state == P2PSessionCoordinatorState::Failed) {
                fail(OnlineSessionCoordinatorError::P2PFailed,
                     OnlineServiceError::None,
                     p2pStatus.message.empty()
                        ? "P2P session leave failed" : p2pStatus.message);
            }
            return;
        }
        if (status.topology == OnlineSessionTopology::Dedicated && dedicated) {
            dedicated->update();
            status.dedicated = dedicated->getStatus();
            if (status.dedicated.state ==
                DedicatedSessionConnectionState::Idle) {
                finishSessionLeave();
            } else if (status.dedicated.state ==
                       DedicatedSessionConnectionState::Failed) {
                fail(OnlineSessionCoordinatorError::DedicatedFailed,
                     OnlineServiceError::None,
                     status.dedicated.message.empty()
                        ? "Dedicated disconnect failed"
                        : status.dedicated.message);
            }
        }
    }

    std::shared_ptr<ILobbyService> lobbies;
    std::shared_ptr<IMatchmakingService> matchmaking;
    P2PSessionCoordinator& p2p;
    std::shared_ptr<IDedicatedSessionConnector> dedicated;
    OnlineSessionCoordinatorConfig config;
    OnlineSessionCoordinatorStatus status;
    std::vector<LobbyInfo> lobbyResults;
    std::future<OperationResult> operation;
    uint64_t nextPollAtMs = 0;
    uint32_t pollFailures = 0;
    bool cancelRequested = false;
    bool reconcileCancellation = false;
};

OnlineSessionCoordinator::OnlineSessionCoordinator(
    std::shared_ptr<ILobbyService> lobbies,
    std::shared_ptr<IMatchmakingService> matchmaking,
    P2PSessionCoordinator& p2p,
    OnlineSessionCoordinatorConfig config,
    std::shared_ptr<IDedicatedSessionConnector> dedicated)
    : _impl(std::make_unique<Impl>(
          std::move(lobbies), std::move(matchmaking), p2p,
          std::move(config), std::move(dedicated))) {}

OnlineSessionCoordinator::~OnlineSessionCoordinator() = default;

bool OnlineSessionCoordinator::listLobbies(ListLobbiesRequest request) {
    return _impl->startList(std::move(request));
}

bool OnlineSessionCoordinator::createLobby(CreateLobbyRequest request) {
    return _impl->startCreate(std::move(request));
}

bool OnlineSessionCoordinator::joinLobby(LobbyId lobbyId) {
    return _impl->startJoin(lobbyId);
}

bool OnlineSessionCoordinator::refreshLobby() {
    return _impl->startRefresh();
}

bool OnlineSessionCoordinator::updateLobbyName(std::string name) {
    return _impl->startUpdateLobby(std::move(name));
}

bool OnlineSessionCoordinator::leaveLobby() {
    return _impl->startLeaveLobby();
}

bool OnlineSessionCoordinator::launchLobbyP2P(uint16_t virtualPort) {
    return _impl->startLaunchLobby(virtualPort);
}

bool OnlineSessionCoordinator::startMatchmaking(MatchmakingRequest request) {
    return _impl->startEnqueue(std::move(request));
}

bool OnlineSessionCoordinator::cancelMatchmaking() {
    const bool failedWithTicket =
        _impl->status.state == OnlineSessionCoordinatorState::Failed &&
        _impl->hasLiveMatchTicket();
    if (_impl->status.state != OnlineSessionCoordinatorState::Queueing &&
        _impl->status.state != OnlineSessionCoordinatorState::CancellingMatch &&
        !failedWithTicket) {
        return _impl->rejectBusy("no active matchmaking ticket to cancel");
    }
    // A new explicit cleanup attempt after terminal polling/cancellation
    // failure receives a fresh bounded retry budget. Calls made while an
    // existing cancellation is still progressing must not reset that budget.
    if (failedWithTicket) _impl->pollFailures = 0;
    _impl->cancelRequested = true;
    _impl->reconcileCancellation = false;
    _impl->status.state = OnlineSessionCoordinatorState::CancellingMatch;
    _impl->nextPollAtMs = _impl->now();
    // enqueueMatch may still be in flight. Preserve the intent and issue the
    // backend cancellation as soon as its ticket id arrives.
    if (_impl->status.matchTicket.ticketId == 0) return true;
    if (!_impl->operationRunning()) return _impl->startCancel();
    return true;
}

bool OnlineSessionCoordinator::leaveSession() {
    const bool active =
        _impl->status.state == OnlineSessionCoordinatorState::Connecting ||
        _impl->status.state == OnlineSessionCoordinatorState::InSession ||
        _impl->status.state == OnlineSessionCoordinatorState::Failed;
    if (!active || _impl->operationRunning()) {
        return _impl->rejectBusy("no active session to leave");
    }
    if (_impl->status.topology == OnlineSessionTopology::P2P) {
        if (!_impl->p2p.leaveSession()) {
            const auto p2pStatus = _impl->p2p.getStatus();
            _impl->status.p2pError = p2pStatus.error;
            _impl->fail(OnlineSessionCoordinatorError::P2PFailed,
                        OnlineServiceError::None,
                        p2pStatus.message.empty()
                            ? "P2P session could not leave"
                            : p2pStatus.message);
            return false;
        }
    } else if (_impl->status.topology == OnlineSessionTopology::Dedicated &&
               _impl->dedicated) {
        _impl->dedicated->disconnect();
        _impl->status.dedicated = _impl->dedicated->getStatus();
    } else {
        return _impl->rejectBusy("session assignment has no active topology");
    }
    _impl->clearError();
    _impl->status.state = OnlineSessionCoordinatorState::LeavingSession;
    return true;
}

void OnlineSessionCoordinator::update() {
    _impl->p2p.update();
    _impl->consumeOperation();
    if (_impl->operationRunning()) return;

    if (_impl->status.state == OnlineSessionCoordinatorState::Queueing ||
        _impl->status.state == OnlineSessionCoordinatorState::CancellingMatch) {
        _impl->updateMatchmaking();
    } else if (_impl->status.state == OnlineSessionCoordinatorState::Connecting ||
               _impl->status.state == OnlineSessionCoordinatorState::InSession) {
        if (_impl->status.topology == OnlineSessionTopology::P2P) {
            _impl->syncP2P();
        } else if (_impl->status.topology == OnlineSessionTopology::Dedicated) {
            _impl->syncDedicated();
        }
    } else if (_impl->status.state ==
               OnlineSessionCoordinatorState::LeavingSession) {
        _impl->syncLeaving();
    }
}

bool OnlineSessionCoordinator::reset() {
    if (_impl->status.state != OnlineSessionCoordinatorState::Failed ||
        _impl->operationRunning() || _impl->hasLobby() ||
        _impl->hasLiveMatchTicket() ||
        _impl->status.topology != OnlineSessionTopology::None ||
        _impl->p2p.getGrant().member.isValid()) {
        return false;
    }
    if (_impl->p2p.getStatus().state == P2PSessionCoordinatorState::Failed &&
        !_impl->p2p.reset()) return false;
    _impl->status = {};
    _impl->lobbyResults.clear();
    _impl->cancelRequested = false;
    _impl->reconcileCancellation = false;
    _impl->pollFailures = 0;
    _impl->nextPollAtMs = 0;
    return true;
}

OnlineSessionCoordinatorStatus OnlineSessionCoordinator::getStatus() const {
    return _impl->status;
}

std::vector<LobbyInfo> OnlineSessionCoordinator::getLobbyResults() const {
    return _impl->lobbyResults;
}

} // namespace ayt::net
