#include <AYNetwork/Session/InMemoryOnlineServices.h>

#include <sodium.h>

#include <algorithm>
#include <chrono>
#include <limits>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace ayt::net
{
namespace
{

constexpr size_t kMaxNameBytes = 128;
constexpr size_t kMaxKeyBytes = 64;
constexpr char kHex[] = "0123456789abcdef";

uint64_t systemNow() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}

bool sodiumReady() {
    static const bool ready = sodium_init() >= 0;
    return ready;
}

std::string randomToken() {
    if (!sodiumReady()) return {};
    std::array<uint8_t, 32> bytes{};
    randombytes_buf(bytes.data(), bytes.size());
    std::string token(bytes.size() * 2, '0');
    for (size_t i = 0; i < bytes.size(); ++i) {
        token[i * 2] = kHex[bytes[i] >> 4];
        token[i * 2 + 1] = kHex[bytes[i] & 0x0f];
    }
    sodium_memzero(bytes.data(), bytes.size());
    return token;
}

bool tokenEqual(const std::string& left, const std::string& right) {
    return left.size() == right.size() && left.size() == 64 && sodiumReady() &&
           sodium_memcmp(left.data(), right.data(), left.size()) == 0;
}

bool validKey(const std::string& value) {
    return !value.empty() && value.size() <= kMaxKeyBytes;
}

bool containsPeer(const std::vector<PeerId>& peers, const PeerId& peer) {
    return std::find(peers.begin(), peers.end(), peer) != peers.end();
}

bool validParty(const std::vector<PeerId>& party, uint16_t target) {
    if (party.empty() || party.size() > target) return false;
    std::unordered_set<std::string> unique;
    for (const PeerId& peer : party) {
        if (!peer.isValid() || !unique.insert(peer.value).second) return false;
    }
    return true;
}

OnlineServiceError mapSessionError(SessionServiceError error) {
    switch (error) {
    case SessionServiceError::InvalidRequest: return OnlineServiceError::InvalidRequest;
    case SessionServiceError::Unauthorized: return OnlineServiceError::Unauthorized;
    case SessionServiceError::SessionFull: return OnlineServiceError::Full;
    case SessionServiceError::SessionClosed: return OnlineServiceError::Closed;
    case SessionServiceError::TransportError:
    case SessionServiceError::RateLimited: return OnlineServiceError::BackendUnavailable;
    case SessionServiceError::None: return OnlineServiceError::None;
    case SessionServiceError::SessionNotFound:
    case SessionServiceError::EpochConflict:
    case SessionServiceError::HostLeaseExpired:
    case SessionServiceError::ProtocolError:
    case SessionServiceError::InternalError: return OnlineServiceError::InternalError;
    }
    return OnlineServiceError::InternalError;
}

} // namespace

bool LobbyInfo::isValid() const {
    if (lobbyId == 0 || revision == 0 || !ownerPeerId.isValid() ||
        name.empty() || !validKey(region) || !validKey(buildId) ||
        !content.isValid() || capacity == 0 || members.size() > capacity) {
        return false;
    }
    // The final member leaving returns a closed tombstone so remote clients
    // can distinguish a successful leave from a malformed response. There is
    // deliberately no current owner/member in that terminal representation.
    if (state == LobbyState::Closed) {
        return members.empty() && sessionId == 0;
    }
    return !members.empty() && containsPeer(members, ownerPeerId) &&
           (state == LobbyState::InSession ? sessionId != 0 : sessionId == 0);
}

struct InMemoryOnlineServices::Impl {
    struct LobbyRecord { LobbyInfo info; };
    struct ServerRecord {
        DedicatedServerInfo info;
        std::string token;
    };
    struct AllocationRecord { DedicatedAllocation allocation; };
    struct TicketRecord { MatchTicketInfo info; };

    Impl(InMemoryOnlineServicesConfig input,
         std::shared_ptr<IP2PSessionService> sessions)
        : config(std::move(input)), p2pSessions(std::move(sessions)) {
        if (!config.nowUnixSeconds) config.nowUnixSeconds = systemNow;
    }

    uint64_t now() const { return config.nowUnixSeconds(); }

    template <typename Map>
    uint64_t allocateId(const Map& map) const {
        if (!sodiumReady()) return 0;
        for (unsigned attempt = 0; attempt < 32; ++attempt) {
            uint64_t id = 0;
            randombytes_buf(&id, sizeof(id));
            id &= (std::numeric_limits<uint64_t>::max)() >> 1;
            if (id != 0 && map.find(id) == map.end()) return id;
        }
        return 0;
    }

    void expireDedicatedLocked() {
        const uint64_t current = now();
        for (auto it = allocations.begin(); it != allocations.end();) {
            if (current < it->second.allocation.expiresAtUnixSeconds) {
                ++it;
                continue;
            }
            auto server = servers.find(it->second.allocation.serverId);
            if (server != servers.end()) {
                server->second.info.reservedPlayers = static_cast<uint16_t>(
                    server->second.info.reservedPlayers -
                    it->second.allocation.playerCount);
            }
            it = allocations.erase(it);
        }
        for (auto it = servers.begin(); it != servers.end();) {
            if (current < it->second.info.leaseExpiresAtUnixSeconds) {
                ++it;
                continue;
            }
            const DedicatedServerId expiredId = it->first;
            for (auto allocation = allocations.begin();
                 allocation != allocations.end();) {
                allocation = allocation->second.allocation.serverId == expiredId
                    ? allocations.erase(allocation) : std::next(allocation);
            }
            it = servers.erase(it);
        }
    }

    bool matchCompatible(const MatchmakingRequest& left,
                         const MatchmakingRequest& right) const {
        return left.queue == right.queue && left.region == right.region &&
               left.buildId == right.buildId &&
               left.content == right.content &&
               left.topology == right.topology &&
               left.targetPlayers == right.targetPlayers &&
               left.virtualPort == right.virtualPort;
    }

    bool peerHasActiveTicketLocked(const PeerId& peer) const {
        for (const auto& [id, record] : tickets) {
            (void)id;
            if ((record.info.state == MatchTicketState::Queued ||
                 record.info.state == MatchTicketState::Matching) &&
                containsPeer(record.info.request.partyMembers, peer)) return true;
        }
        return false;
    }

    InMemoryOnlineServicesConfig config;
    std::shared_ptr<IP2PSessionService> p2pSessions;
    mutable std::mutex mutex;
    std::unordered_map<LobbyId, LobbyRecord> lobbies;
    std::unordered_map<DedicatedServerId, ServerRecord> servers;
    std::unordered_map<DedicatedAllocationId, AllocationRecord> allocations;
    std::unordered_map<MatchTicketId, TicketRecord> tickets;
    std::vector<MatchTicketId> ticketOrder;
};

InMemoryOnlineServices::InMemoryOnlineServices(
    InMemoryOnlineServicesConfig config,
    std::shared_ptr<IP2PSessionService> p2pSessions)
    : _impl(std::make_unique<Impl>(std::move(config), std::move(p2pSessions))) {}

InMemoryOnlineServices::~InMemoryOnlineServices() = default;

OnlineServiceResult<LobbyInfo> InMemoryOnlineServices::createLobby(
    const CreateLobbyRequest& request) {
    if (!request.ownerPeerId.isValid() || request.name.empty() ||
        request.name.size() > kMaxNameBytes || !validKey(request.region) ||
        !validKey(request.buildId) || !request.content.isValid() ||
        request.capacity == 0 ||
        request.capacity > _impl->config.maxLobbyCapacity) {
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::InvalidRequest, "invalid lobby request");
    }
    std::lock_guard<std::mutex> lock(_impl->mutex);
    if (_impl->lobbies.size() >= _impl->config.maxLobbies) {
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::Full, "lobby directory is full");
    }
    const LobbyId id = _impl->allocateId(_impl->lobbies);
    if (id == 0) {
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::InternalError, "failed to allocate lobby id");
    }
    LobbyInfo info;
    info.lobbyId = id;
    info.revision = 1;
    info.ownerPeerId = request.ownerPeerId;
    info.name = request.name;
    info.region = request.region;
    info.buildId = request.buildId;
    info.content = request.content;
    info.capacity = request.capacity;
    info.state = LobbyState::Open;
    info.members.push_back(request.ownerPeerId);
    _impl->lobbies.emplace(id, Impl::LobbyRecord{info});
    return OnlineServiceResult<LobbyInfo>::success(std::move(info));
}

OnlineServiceResult<std::vector<LobbyInfo>>
InMemoryOnlineServices::listLobbies(const ListLobbiesRequest& request) {
    if (request.limit == 0 || request.limit > 1000 ||
        request.region.size() > kMaxKeyBytes ||
        request.buildId.size() > kMaxKeyBytes) {
        return OnlineServiceResult<std::vector<LobbyInfo>>::failure(
            OnlineServiceError::InvalidRequest, "invalid lobby filter");
    }
    std::vector<LobbyInfo> result;
    std::lock_guard<std::mutex> lock(_impl->mutex);
    for (const auto& [id, record] : _impl->lobbies) {
        (void)id;
        const LobbyInfo& info = record.info;
        if (info.state != LobbyState::Open ||
            (!request.region.empty() && info.region != request.region) ||
            (!request.buildId.empty() && info.buildId != request.buildId) ||
            info.capacity - info.members.size() < request.minimumOpenSlots) {
            continue;
        }
        result.push_back(info);
    }
    std::sort(result.begin(), result.end(), [](const auto& left, const auto& right) {
        return left.lobbyId < right.lobbyId;
    });
    if (result.size() > request.limit) result.resize(request.limit);
    return OnlineServiceResult<std::vector<LobbyInfo>>::success(std::move(result));
}

OnlineServiceResult<LobbyInfo> InMemoryOnlineServices::joinLobby(
    LobbyId lobbyId, const PeerId& authenticatedPeer) {
    if (lobbyId == 0 || !authenticatedPeer.isValid()) {
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::InvalidRequest, "invalid lobby join");
    }
    std::lock_guard<std::mutex> lock(_impl->mutex);
    auto found = _impl->lobbies.find(lobbyId);
    if (found == _impl->lobbies.end()) {
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::NotFound, "lobby not found");
    }
    LobbyInfo& info = found->second.info;
    if (containsPeer(info.members, authenticatedPeer)) {
        return OnlineServiceResult<LobbyInfo>::success(info);
    }
    if (info.state != LobbyState::Open) {
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::Closed, "lobby is not joinable");
    }
    if (info.members.size() >= info.capacity) {
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::Full, "lobby is full");
    }
    info.members.push_back(authenticatedPeer);
    ++info.revision;
    return OnlineServiceResult<LobbyInfo>::success(info);
}

OnlineServiceResult<LobbyInfo> InMemoryOnlineServices::leaveLobby(
    LobbyId lobbyId, const PeerId& authenticatedPeer) {
    if (lobbyId == 0 || !authenticatedPeer.isValid()) {
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::InvalidRequest, "invalid lobby leave");
    }
    std::lock_guard<std::mutex> lock(_impl->mutex);
    auto found = _impl->lobbies.find(lobbyId);
    if (found == _impl->lobbies.end()) {
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::NotFound, "lobby not found");
    }
    LobbyInfo& info = found->second.info;
    if (info.state == LobbyState::Launching) {
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::Conflict, "lobby launch is in progress");
    }
    auto member = std::find(info.members.begin(), info.members.end(), authenticatedPeer);
    if (member == info.members.end()) {
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::Unauthorized, "peer is not a lobby member");
    }
    info.members.erase(member);
    if (info.members.empty()) {
        LobbyInfo closed = info;
        closed.state = LobbyState::Closed;
        closed.sessionId = 0;
        ++closed.revision;
        _impl->lobbies.erase(found);
        return OnlineServiceResult<LobbyInfo>::success(std::move(closed));
    }
    if (info.ownerPeerId == authenticatedPeer) info.ownerPeerId = info.members.front();
    ++info.revision;
    return OnlineServiceResult<LobbyInfo>::success(info);
}

OnlineServiceResult<LobbyInfo> InMemoryOnlineServices::updateLobby(
    const UpdateLobbyRequest& request) {
    if (request.lobbyId == 0 || !request.actorPeerId.isValid() ||
        request.expectedRevision == 0 || request.name.empty() ||
        request.name.size() > kMaxNameBytes) {
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::InvalidRequest, "invalid lobby update");
    }
    std::lock_guard<std::mutex> lock(_impl->mutex);
    auto found = _impl->lobbies.find(request.lobbyId);
    if (found == _impl->lobbies.end()) {
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::NotFound, "lobby not found");
    }
    LobbyInfo& info = found->second.info;
    if (info.ownerPeerId != request.actorPeerId) {
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::Unauthorized, "only the lobby owner may update it");
    }
    if (info.revision != request.expectedRevision) {
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::Conflict, "lobby revision changed");
    }
    if (info.state != LobbyState::Open) {
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::Closed, "lobby is not open");
    }
    info.name = request.name;
    ++info.revision;
    return OnlineServiceResult<LobbyInfo>::success(info);
}

OnlineServiceResult<LobbyInfo> InMemoryOnlineServices::getLobby(LobbyId lobbyId) {
    std::lock_guard<std::mutex> lock(_impl->mutex);
    auto found = _impl->lobbies.find(lobbyId);
    if (found == _impl->lobbies.end()) {
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::NotFound, "lobby not found");
    }
    return OnlineServiceResult<LobbyInfo>::success(found->second.info);
}

OnlineServiceResult<LobbyLaunchResult> InMemoryOnlineServices::launchLobbyP2P(
    const LaunchLobbyRequest& request) {
    LobbyInfo snapshot;
    uint64_t launchRevision = 0;
    {
        std::lock_guard<std::mutex> lock(_impl->mutex);
        auto found = _impl->lobbies.find(request.lobbyId);
        if (found == _impl->lobbies.end()) {
            return OnlineServiceResult<LobbyLaunchResult>::failure(
                OnlineServiceError::NotFound, "lobby not found");
        }
        LobbyInfo& info = found->second.info;
        if (!request.actorPeerId.isValid() || request.virtualPort == 0 ||
            request.expectedRevision == 0) {
            return OnlineServiceResult<LobbyLaunchResult>::failure(
                OnlineServiceError::InvalidRequest, "invalid lobby launch");
        }
        if (info.ownerPeerId != request.actorPeerId) {
            return OnlineServiceResult<LobbyLaunchResult>::failure(
                OnlineServiceError::Unauthorized, "only the lobby owner may launch");
        }
        if (info.revision != request.expectedRevision ||
            info.state != LobbyState::Open) {
            return OnlineServiceResult<LobbyLaunchResult>::failure(
                OnlineServiceError::Conflict, "lobby revision or state changed");
        }
        info.state = LobbyState::Launching;
        launchRevision = ++info.revision;
        snapshot = info;
    }

    auto rollback = [&] {
        std::lock_guard<std::mutex> lock(_impl->mutex);
        auto found = _impl->lobbies.find(request.lobbyId);
        if (found != _impl->lobbies.end() &&
            found->second.info.state == LobbyState::Launching &&
            found->second.info.revision == launchRevision) {
            found->second.info.state = LobbyState::Open;
            ++found->second.info.revision;
        }
    };
    if (!_impl->p2pSessions) {
        rollback();
        return OnlineServiceResult<LobbyLaunchResult>::failure(
            OnlineServiceError::BackendUnavailable,
            "P2P session backend is not configured");
    }

    P2PSessionCreateRequest create;
    create.hostPeerId = snapshot.ownerPeerId;
    create.virtualPort = request.virtualPort;
    create.capacity = snapshot.capacity;
    auto host = _impl->p2pSessions->createSession(create);
    if (!host) {
        rollback();
        return OnlineServiceResult<LobbyLaunchResult>::failure(
            mapSessionError(host.error), host.message);
    }
    std::vector<P2PSessionGrant> grants;
    grants.push_back(host.value);
    for (const PeerId& peer : snapshot.members) {
        if (peer == snapshot.ownerPeerId) continue;
        auto joined = _impl->p2pSessions->joinSession(
            {host.value.session.sessionId, peer});
        if (!joined) {
            (void)_impl->p2pSessions->leaveSession({host.value.member});
            rollback();
            return OnlineServiceResult<LobbyLaunchResult>::failure(
                mapSessionError(joined.error), joined.message);
        }
        grants.push_back(std::move(joined.value));
    }

    LobbyLaunchResult result;
    {
        std::lock_guard<std::mutex> lock(_impl->mutex);
        auto found = _impl->lobbies.find(request.lobbyId);
        if (found == _impl->lobbies.end() ||
            found->second.info.state != LobbyState::Launching ||
            found->second.info.revision != launchRevision) {
            (void)_impl->p2pSessions->leaveSession({host.value.member});
            return OnlineServiceResult<LobbyLaunchResult>::failure(
                OnlineServiceError::Conflict, "lobby changed during launch");
        }
        found->second.info.state = LobbyState::InSession;
        found->second.info.sessionId = host.value.session.sessionId;
        ++found->second.info.revision;
        result.lobby = found->second.info;
    }
    result.memberGrants = std::move(grants);
    return OnlineServiceResult<LobbyLaunchResult>::success(std::move(result));
}

OnlineServiceResult<DedicatedServerGrant> InMemoryOnlineServices::registerServer(
    const DedicatedServerRegistration& request) {
    if (request.instanceName.empty() || request.instanceName.size() > kMaxNameBytes ||
        !validKey(request.region) || !validKey(request.buildId) ||
        request.address.empty() || request.address.size() > 255 ||
        request.port == 0 || request.capacity == 0 ||
        request.capacity > _impl->config.maxDedicatedServerCapacity) {
        return OnlineServiceResult<DedicatedServerGrant>::failure(
            OnlineServiceError::InvalidRequest, "invalid server registration");
    }
    std::lock_guard<std::mutex> lock(_impl->mutex);
    _impl->expireDedicatedLocked();
    if (_impl->servers.size() >= _impl->config.maxDedicatedServers) {
        return OnlineServiceResult<DedicatedServerGrant>::failure(
            OnlineServiceError::Full, "server directory is full");
    }
    for (const auto& [id, record] : _impl->servers) {
        (void)id;
        if (record.info.instanceName == request.instanceName) {
            return OnlineServiceResult<DedicatedServerGrant>::failure(
                OnlineServiceError::Conflict, "server instance already registered");
        }
    }
    const DedicatedServerId id = _impl->allocateId(_impl->servers);
    const std::string token = randomToken();
    if (id == 0 || token.empty()) {
        return OnlineServiceResult<DedicatedServerGrant>::failure(
            OnlineServiceError::InternalError, "failed to create server credential");
    }
    DedicatedServerInfo info;
    info.serverId = id;
    info.instanceName = request.instanceName;
    info.region = request.region;
    info.buildId = request.buildId;
    info.address = request.address;
    info.port = request.port;
    info.capacity = request.capacity;
    info.leaseExpiresAtUnixSeconds =
        _impl->now() + _impl->config.dedicatedLeaseSeconds;
    _impl->servers.emplace(id, Impl::ServerRecord{info, token});
    return OnlineServiceResult<DedicatedServerGrant>::success(
        {info, DedicatedServerCredential{id, token}});
}

OnlineServiceResult<DedicatedServerInfo> InMemoryOnlineServices::heartbeatServer(
    const DedicatedServerCredential& credential) {
    if (!credential.isValid()) {
        return OnlineServiceResult<DedicatedServerInfo>::failure(
            OnlineServiceError::InvalidRequest, "invalid server credential");
    }
    std::lock_guard<std::mutex> lock(_impl->mutex);
    _impl->expireDedicatedLocked();
    auto found = _impl->servers.find(credential.serverId);
    if (found == _impl->servers.end()) {
        return OnlineServiceResult<DedicatedServerInfo>::failure(
            OnlineServiceError::NotFound, "server lease not found");
    }
    if (!tokenEqual(found->second.token, credential.token)) {
        return OnlineServiceResult<DedicatedServerInfo>::failure(
            OnlineServiceError::Unauthorized, "invalid server credential");
    }
    found->second.info.leaseExpiresAtUnixSeconds =
        _impl->now() + _impl->config.dedicatedLeaseSeconds;
    return OnlineServiceResult<DedicatedServerInfo>::success(found->second.info);
}

OnlineServiceResult<DedicatedServerInfo>
InMemoryOnlineServices::setServerDraining(
    const DedicatedServerCredential& credential, bool draining) {
    if (!credential.isValid()) {
        return OnlineServiceResult<DedicatedServerInfo>::failure(
            OnlineServiceError::InvalidRequest, "invalid server credential");
    }
    std::lock_guard<std::mutex> lock(_impl->mutex);
    _impl->expireDedicatedLocked();
    auto found = _impl->servers.find(credential.serverId);
    if (found == _impl->servers.end()) {
        return OnlineServiceResult<DedicatedServerInfo>::failure(
            OnlineServiceError::NotFound, "server not found");
    }
    if (!tokenEqual(found->second.token, credential.token)) {
        return OnlineServiceResult<DedicatedServerInfo>::failure(
            OnlineServiceError::Unauthorized, "invalid server credential");
    }
    found->second.info.draining = draining;
    return OnlineServiceResult<DedicatedServerInfo>::success(found->second.info);
}

OnlineServiceResult<SessionServiceEmpty>
InMemoryOnlineServices::unregisterServer(
    const DedicatedServerCredential& credential) {
    if (!credential.isValid()) {
        return OnlineServiceResult<SessionServiceEmpty>::failure(
            OnlineServiceError::InvalidRequest, "invalid server credential");
    }
    std::lock_guard<std::mutex> lock(_impl->mutex);
    _impl->expireDedicatedLocked();
    auto found = _impl->servers.find(credential.serverId);
    if (found == _impl->servers.end()) {
        return OnlineServiceResult<SessionServiceEmpty>::failure(
            OnlineServiceError::NotFound, "server not found");
    }
    if (!tokenEqual(found->second.token, credential.token)) {
        return OnlineServiceResult<SessionServiceEmpty>::failure(
            OnlineServiceError::Unauthorized, "invalid server credential");
    }
    if (found->second.info.reservedPlayers != 0) {
        found->second.info.draining = true;
        return OnlineServiceResult<SessionServiceEmpty>::failure(
            OnlineServiceError::Conflict,
            "server is draining until allocations are released");
    }
    _impl->servers.erase(found);
    return OnlineServiceResult<SessionServiceEmpty>::success({});
}

OnlineServiceResult<DedicatedAllocation>
InMemoryOnlineServices::allocateServer(
    const DedicatedAllocationRequest& request) {
    if (!validKey(request.region) || !validKey(request.buildId) ||
        request.playerCount == 0 ||
        request.playerCount > _impl->config.maxMatchPlayers) {
        return OnlineServiceResult<DedicatedAllocation>::failure(
            OnlineServiceError::InvalidRequest, "invalid allocation request");
    }
    std::lock_guard<std::mutex> lock(_impl->mutex);
    _impl->expireDedicatedLocked();
    Impl::ServerRecord* selected = nullptr;
    for (auto& [id, record] : _impl->servers) {
        (void)id;
        const auto& info = record.info;
        if (info.draining || info.region != request.region ||
            info.buildId != request.buildId ||
            static_cast<uint32_t>(info.reservedPlayers) + request.playerCount >
                info.capacity) continue;
        if (!selected ||
            static_cast<uint32_t>(info.reservedPlayers) * selected->info.capacity <
                static_cast<uint32_t>(selected->info.reservedPlayers) * info.capacity ||
            (static_cast<uint32_t>(info.reservedPlayers) * selected->info.capacity ==
                 static_cast<uint32_t>(selected->info.reservedPlayers) * info.capacity &&
             info.serverId < selected->info.serverId)) {
            selected = &record;
        }
    }
    if (!selected) {
        return OnlineServiceResult<DedicatedAllocation>::failure(
            OnlineServiceError::NoCapacity, "no eligible dedicated server");
    }
    const DedicatedAllocationId id = _impl->allocateId(_impl->allocations);
    const std::string token = randomToken();
    if (id == 0 || token.empty()) {
        return OnlineServiceResult<DedicatedAllocation>::failure(
            OnlineServiceError::InternalError, "failed to create allocation");
    }
    DedicatedAllocation allocation;
    allocation.allocationId = id;
    allocation.serverId = selected->info.serverId;
    allocation.address = selected->info.address;
    allocation.port = selected->info.port;
    allocation.playerCount = request.playerCount;
    allocation.reservationToken = token;
    allocation.expiresAtUnixSeconds =
        _impl->now() + _impl->config.allocationLifetimeSeconds;
    selected->info.reservedPlayers = static_cast<uint16_t>(
        selected->info.reservedPlayers + request.playerCount);
    _impl->allocations.emplace(id, Impl::AllocationRecord{allocation});
    return OnlineServiceResult<DedicatedAllocation>::success(
        std::move(allocation));
}

OnlineServiceResult<SessionServiceEmpty>
InMemoryOnlineServices::releaseAllocation(
    DedicatedAllocationId allocationId,
    const std::string& reservationToken) {
    if (allocationId == 0 || reservationToken.size() != 64) {
        return OnlineServiceResult<SessionServiceEmpty>::failure(
            OnlineServiceError::InvalidRequest, "invalid allocation credential");
    }
    std::lock_guard<std::mutex> lock(_impl->mutex);
    _impl->expireDedicatedLocked();
    auto found = _impl->allocations.find(allocationId);
    if (found == _impl->allocations.end()) {
        return OnlineServiceResult<SessionServiceEmpty>::failure(
            OnlineServiceError::NotFound, "allocation not found");
    }
    if (!tokenEqual(found->second.allocation.reservationToken,
                    reservationToken)) {
        return OnlineServiceResult<SessionServiceEmpty>::failure(
            OnlineServiceError::Unauthorized, "invalid allocation credential");
    }
    auto server = _impl->servers.find(found->second.allocation.serverId);
    if (server != _impl->servers.end()) {
        server->second.info.reservedPlayers = static_cast<uint16_t>(
            server->second.info.reservedPlayers -
            found->second.allocation.playerCount);
    }
    _impl->allocations.erase(found);
    return OnlineServiceResult<SessionServiceEmpty>::success({});
}

OnlineServiceResult<std::vector<DedicatedServerInfo>>
InMemoryOnlineServices::listServers() {
    std::vector<DedicatedServerInfo> result;
    std::lock_guard<std::mutex> lock(_impl->mutex);
    _impl->expireDedicatedLocked();
    result.reserve(_impl->servers.size());
    for (const auto& [id, record] : _impl->servers) {
        (void)id;
        result.push_back(record.info);
    }
    std::sort(result.begin(), result.end(), [](const auto& left, const auto& right) {
        return left.serverId < right.serverId;
    });
    return OnlineServiceResult<std::vector<DedicatedServerInfo>>::success(
        std::move(result));
}

OnlineServiceResult<MatchTicketInfo> InMemoryOnlineServices::enqueueMatch(
    const MatchmakingRequest& request) {
    if (!validParty(request.partyMembers, request.targetPlayers) ||
        !validKey(request.queue) || !validKey(request.region) ||
        !validKey(request.buildId) || !request.content.isValid() ||
        request.targetPlayers < 2 ||
        request.virtualPort == 0 ||
        request.targetPlayers > _impl->config.maxMatchPlayers) {
        return OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::InvalidRequest, "invalid matchmaking request");
    }
    std::lock_guard<std::mutex> lock(_impl->mutex);
    if (_impl->tickets.size() >= _impl->config.maxMatchTickets) {
        return OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::Full, "matchmaking queue is full");
    }
    for (const PeerId& peer : request.partyMembers) {
        if (_impl->peerHasActiveTicketLocked(peer)) {
            return OnlineServiceResult<MatchTicketInfo>::failure(
                OnlineServiceError::Conflict,
                "a party member already has an active match ticket");
        }
    }
    const MatchTicketId id = _impl->allocateId(_impl->tickets);
    if (id == 0) {
        return OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::InternalError, "failed to allocate match ticket");
    }
    MatchTicketInfo info;
    info.ticketId = id;
    info.request = request;
    _impl->tickets.emplace(id, Impl::TicketRecord{info});
    _impl->ticketOrder.push_back(id);
    return OnlineServiceResult<MatchTicketInfo>::success(std::move(info));
}

OnlineServiceResult<MatchTicketInfo> InMemoryOnlineServices::getMatch(
    MatchTicketId ticketId, const PeerId& authenticatedPeer) {
    if (ticketId == 0 || !authenticatedPeer.isValid()) {
        return OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::InvalidRequest, "invalid match query");
    }
    std::lock_guard<std::mutex> lock(_impl->mutex);
    auto found = _impl->tickets.find(ticketId);
    if (found == _impl->tickets.end()) {
        return OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::NotFound, "match ticket not found");
    }
    if (!containsPeer(found->second.info.request.partyMembers,
                      authenticatedPeer)) {
        return OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::Unauthorized, "peer does not own this ticket");
    }
    return OnlineServiceResult<MatchTicketInfo>::success(found->second.info);
}

OnlineServiceResult<MatchTicketInfo> InMemoryOnlineServices::cancelMatch(
    MatchTicketId ticketId, const PeerId& authenticatedPeer) {
    std::lock_guard<std::mutex> lock(_impl->mutex);
    auto found = _impl->tickets.find(ticketId);
    if (found == _impl->tickets.end()) {
        return OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::NotFound, "match ticket not found");
    }
    if (!authenticatedPeer.isValid() ||
        !containsPeer(found->second.info.request.partyMembers,
                      authenticatedPeer)) {
        return OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::Unauthorized, "peer does not own this ticket");
    }
    if (found->second.info.state != MatchTicketState::Queued) {
        return OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::Conflict, "match ticket is no longer queued");
    }
    found->second.info.state = MatchTicketState::Cancelled;
    return OnlineServiceResult<MatchTicketInfo>::success(found->second.info);
}

size_t InMemoryOnlineServices::runMatchmaking(size_t maxMatches) {
    size_t processed = 0;
    while (processed < maxMatches) {
        std::vector<MatchTicketId> batch;
        MatchmakingRequest matchRequest;
        {
            std::lock_guard<std::mutex> lock(_impl->mutex);
            for (MatchTicketId seedId : _impl->ticketOrder) {
                auto seed = _impl->tickets.find(seedId);
                if (seed == _impl->tickets.end() ||
                    seed->second.info.state != MatchTicketState::Queued) continue;
                std::vector<MatchTicketId> candidate{seedId};
                size_t players = seed->second.info.request.partyMembers.size();
                for (MatchTicketId candidateId : _impl->ticketOrder) {
                    if (candidateId == seedId) continue;
                    auto entry = _impl->tickets.find(candidateId);
                    if (entry == _impl->tickets.end() ||
                        entry->second.info.state != MatchTicketState::Queued ||
                        !_impl->matchCompatible(seed->second.info.request,
                                                entry->second.info.request)) continue;
                    const size_t party = entry->second.info.request.partyMembers.size();
                    if (players + party > seed->second.info.request.targetPlayers) continue;
                    candidate.push_back(candidateId);
                    players += party;
                    if (players == seed->second.info.request.targetPlayers) break;
                }
                if (players != seed->second.info.request.targetPlayers) continue;
                batch = std::move(candidate);
                matchRequest = seed->second.info.request;
                for (MatchTicketId id : batch) {
                    _impl->tickets.at(id).info.state = MatchTicketState::Matching;
                }
                break;
            }
        }
        if (batch.empty()) break;

        std::vector<PeerId> peers;
        {
            std::lock_guard<std::mutex> lock(_impl->mutex);
            for (MatchTicketId id : batch) {
                const auto& party = _impl->tickets.at(id).info.request.partyMembers;
                peers.insert(peers.end(), party.begin(), party.end());
            }
        }

        MatchAssignment assignment;
        assignment.content = matchRequest.content;
        OnlineServiceError failure = OnlineServiceError::None;
        std::string failureMessage;
        if (matchRequest.topology == MatchTopology::Dedicated ||
            matchRequest.topology == MatchTopology::Any) {
            auto allocated = allocateServer({
                matchRequest.region, matchRequest.buildId,
                static_cast<uint16_t>(peers.size())});
            if (allocated) {
                assignment.topology = MatchTopology::Dedicated;
                assignment.dedicated = std::move(allocated.value);
            } else if (matchRequest.topology == MatchTopology::Dedicated) {
                failure = allocated.error;
                failureMessage = allocated.message;
            }
        }

        if (!assignment.dedicated.isValid() &&
            matchRequest.topology != MatchTopology::Dedicated) {
            if (!_impl->p2pSessions) {
                failure = OnlineServiceError::BackendUnavailable;
                failureMessage = "P2P session backend is not configured";
            } else {
                P2PSessionCreateRequest create;
                create.hostPeerId = peers.front();
                create.virtualPort = matchRequest.virtualPort;
                create.capacity = matchRequest.targetPlayers;
                auto host = _impl->p2pSessions->createSession(create);
                if (!host) {
                    failure = mapSessionError(host.error);
                    failureMessage = host.message;
                } else {
                    assignment.topology = MatchTopology::P2P;
                    assignment.p2pGrants.push_back(host.value);
                    for (size_t i = 1; i < peers.size(); ++i) {
                        auto joined = _impl->p2pSessions->joinSession(
                            {host.value.session.sessionId, peers[i]});
                        if (!joined) {
                            failure = mapSessionError(joined.error);
                            failureMessage = joined.message;
                            (void)_impl->p2pSessions->leaveSession(
                                {host.value.member});
                            assignment.p2pGrants.clear();
                            break;
                        }
                        assignment.p2pGrants.push_back(std::move(joined.value));
                    }
                }
            }
        }

        if (failure == OnlineServiceError::NoCapacity &&
            matchRequest.topology == MatchTopology::Dedicated) {
            std::lock_guard<std::mutex> lock(_impl->mutex);
            for (MatchTicketId id : batch) {
                _impl->tickets.at(id).info.state = MatchTicketState::Queued;
            }
            break;
        }

        std::lock_guard<std::mutex> lock(_impl->mutex);
        for (MatchTicketId id : batch) {
            MatchTicketInfo& info = _impl->tickets.at(id).info;
            if (failure != OnlineServiceError::None) {
                info.state = MatchTicketState::Failed;
                info.failure = failureMessage.empty()
                    ? "match backend failed" : failureMessage;
                continue;
            }
            info.state = MatchTicketState::Matched;
            info.assignment.topology = assignment.topology;
            info.assignment.content = assignment.content;
            if (assignment.topology == MatchTopology::Dedicated) {
                info.assignment.dedicated = assignment.dedicated;
            } else {
                for (const auto& grant : assignment.p2pGrants) {
                    if (containsPeer(info.request.partyMembers,
                                     grant.member.peerId)) {
                        info.assignment.p2pGrants.push_back(grant);
                    }
                }
            }
        }
        ++processed;
    }
    return processed;
}

} // namespace ayt::net
