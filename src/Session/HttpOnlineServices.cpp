#include <AYNetwork/Session/HttpOnlineServices.h>

#include "HttpOnlineServicesRoutes.h"

#include <httplib.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <charconv>
#include <limits>
#include <stdexcept>
#include <type_traits>

namespace ayt::net
{
namespace
{

using json = nlohmann::json;
// A full legal lobby page can include 1,000 lobbies with up to 64 member IDs,
// while the dedicated directory can contain 4,096 entries. Keep responses
// bounded, but large enough for every response produced by the reference
// backend's configured limits.
constexpr size_t kMaxResponseBytes = 8u * 1024u * 1024u;
constexpr char kHex[] = "0123456789abcdef";

const char* errorName(OnlineServiceError error) {
    switch (error) {
    case OnlineServiceError::None: return "none";
    case OnlineServiceError::InvalidRequest: return "invalid_request";
    case OnlineServiceError::Unauthorized: return "unauthorized";
    case OnlineServiceError::NotFound: return "not_found";
    case OnlineServiceError::Conflict: return "conflict";
    case OnlineServiceError::Full: return "full";
    case OnlineServiceError::Closed: return "closed";
    case OnlineServiceError::BackendUnavailable: return "backend_unavailable";
    case OnlineServiceError::NoCapacity: return "no_capacity";
    case OnlineServiceError::InternalError: return "internal_error";
    }
    return "internal_error";
}

OnlineServiceError parseError(const std::string& name) {
    if (name == "none") return OnlineServiceError::None;
    if (name == "invalid_request") return OnlineServiceError::InvalidRequest;
    if (name == "unauthorized") return OnlineServiceError::Unauthorized;
    if (name == "not_found" || name == "session_not_found") {
        return OnlineServiceError::NotFound;
    }
    if (name == "conflict" || name == "epoch_conflict") {
        return OnlineServiceError::Conflict;
    }
    if (name == "full" || name == "session_full") return OnlineServiceError::Full;
    if (name == "closed" || name == "session_closed") {
        return OnlineServiceError::Closed;
    }
    if (name == "backend_unavailable" || name == "transport_error" ||
        name == "rate_limited") return OnlineServiceError::BackendUnavailable;
    if (name == "no_capacity") return OnlineServiceError::NoCapacity;
    return OnlineServiceError::InternalError;
}

int statusFor(OnlineServiceError error) {
    switch (error) {
    case OnlineServiceError::None: return 200;
    case OnlineServiceError::InvalidRequest: return 400;
    case OnlineServiceError::Unauthorized: return 401;
    case OnlineServiceError::NotFound: return 404;
    case OnlineServiceError::Conflict:
    case OnlineServiceError::Full: return 409;
    case OnlineServiceError::Closed: return 410;
    case OnlineServiceError::BackendUnavailable:
    case OnlineServiceError::NoCapacity: return 503;
    case OnlineServiceError::InternalError: return 500;
    }
    return 500;
}

template <typename T>
void writeFailure(httplib::Response& response,
                  const OnlineServiceResult<T>& result) {
    response.status = statusFor(result.error);
    response.set_content(json{{"ok", false}, {"error", errorName(result.error)},
                              {"message", result.message}}.dump(),
                         "application/json");
}

void writeSuccess(httplib::Response& response, const json& value) {
    response.status = 200;
    response.set_content(json{{"ok", true}, {"value", value}}.dump(),
                         "application/json");
}

void badRequest(httplib::Response& response, const char* message) {
    writeFailure(response, OnlineServiceResult<SessionServiceEmpty>::failure(
        OnlineServiceError::InvalidRequest, message));
}

template <typename T>
bool readUnsigned(const json& object, const char* key, T& out) {
    static_assert(std::is_unsigned_v<T>);
    if (!object.is_object() || !object.contains(key)) return false;
    uint64_t value = 0;
    try {
        const json& field = object[key];
        if (field.is_number_unsigned()) value = field.get<uint64_t>();
        else if (field.is_number_integer()) {
            const int64_t signedValue = field.get<int64_t>();
            if (signedValue < 0) return false;
            value = static_cast<uint64_t>(signedValue);
        } else return false;
    } catch (...) { return false; }
    if (value > static_cast<uint64_t>((std::numeric_limits<T>::max)())) return false;
    out = static_cast<T>(value);
    return true;
}

bool parseId(const httplib::Request& request, size_t matchIndex, uint64_t& id) {
    if (request.matches.size() <= matchIndex) return false;
    const std::string text = request.matches[matchIndex].str();
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), id);
    return parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size() &&
           id != 0;
}

json parseBody(const httplib::Request& request) {
    return json::parse(request.body, nullptr, false);
}

bool readBearer(const httplib::Request& request, std::string& token) {
    const std::string header = request.get_header_value("Authorization");
    constexpr std::string_view prefix = "Bearer ";
    if (header.rfind(prefix.data(), 0) != 0) return false;
    token = header.substr(prefix.size());
    return !token.empty();
}

bool containsPeer(const std::vector<PeerId>& peers, const PeerId& peer) {
    return std::find(peers.begin(), peers.end(), peer) != peers.end();
}

std::string toHex(const uint8_t* bytes, size_t size) {
    std::string out(size * 2, '0');
    for (size_t i = 0; i < size; ++i) {
        out[i * 2] = kHex[bytes[i] >> 4];
        out[i * 2 + 1] = kHex[bytes[i] & 0x0f];
    }
    return out;
}

int hexValue(char value) {
    if (value >= '0' && value <= '9') return value - '0';
    if (value >= 'a' && value <= 'f') return value - 'a' + 10;
    if (value >= 'A' && value <= 'F') return value - 'A' + 10;
    return -1;
}

bool fromHex(const std::string& text, uint8_t* bytes, size_t size) {
    if (!bytes || text.size() != size * 2) return false;
    for (size_t i = 0; i < size; ++i) {
        const int high = hexValue(text[i * 2]);
        const int low = hexValue(text[i * 2 + 1]);
        if (high < 0 || low < 0) return false;
        bytes[i] = static_cast<uint8_t>((high << 4) | low);
    }
    return true;
}

json sessionToJson(const P2PBackendSessionInfo& value) {
    return {{"session_id", value.sessionId}, {"epoch", value.epoch},
            {"host_peer_id", value.hostPeerId.value},
            {"virtual_port", value.virtualPort}, {"capacity", value.capacity},
            {"member_count", value.memberCount}, {"open", value.open},
            {"host_lease_expires_at", value.hostLeaseExpiresAtUnixSeconds},
            {"signaling_address", value.signalingAddress},
            {"signaling_port", value.signalingPort},
            {"signaling_room", value.signalingRoom}};
}

bool sessionFromJson(const json& value, P2PBackendSessionInfo& out) {
    try {
        P2PBackendSessionInfo parsed;
        if (!readUnsigned(value, "session_id", parsed.sessionId) ||
            !readUnsigned(value, "epoch", parsed.epoch) ||
            !readUnsigned(value, "virtual_port", parsed.virtualPort) ||
            !readUnsigned(value, "capacity", parsed.capacity) ||
            !readUnsigned(value, "member_count", parsed.memberCount) ||
            !readUnsigned(value, "host_lease_expires_at",
                          parsed.hostLeaseExpiresAtUnixSeconds) ||
            !readUnsigned(value, "signaling_port", parsed.signalingPort)) return false;
        parsed.hostPeerId = PeerId{value.at("host_peer_id").get<std::string>()};
        parsed.open = value.at("open").get<bool>();
        parsed.signalingAddress = value.at("signaling_address").get<std::string>();
        parsed.signalingRoom = value.at("signaling_room").get<std::string>();
        if (!parsed.isValid()) return false;
        out = std::move(parsed);
        return true;
    } catch (...) { out = {}; return false; }
}

json grantToJson(const P2PSessionGrant& value) {
    return {{"session", sessionToJson(value.session)},
            {"peer_id", value.member.peerId.value},
            {"member_token", value.member.token},
            {"signaling_token", value.signalingToken},
            {"join_ticket", toHex(value.joinTicket.data(), value.joinTicket.size())},
            {"ticket_public_key", toHex(value.ticketPublicKey.data(),
                                        value.ticketPublicKey.size())}};
}

bool grantFromJson(const json& value, P2PSessionGrant& out) {
    try {
        P2PSessionGrant parsed;
        if (!sessionFromJson(value.at("session"), parsed.session)) return false;
        parsed.member.sessionId = parsed.session.sessionId;
        parsed.member.peerId = PeerId{value.at("peer_id").get<std::string>()};
        parsed.member.token = value.at("member_token").get<std::string>();
        parsed.signalingToken = value.at("signaling_token").get<std::string>();
        const std::string ticket = value.at("join_ticket").get<std::string>();
        const std::string key = value.at("ticket_public_key").get<std::string>();
        if (ticket.empty() || (ticket.size() & 1u) != 0) return false;
        parsed.joinTicket.resize(ticket.size() / 2);
        if (!fromHex(ticket, parsed.joinTicket.data(), parsed.joinTicket.size()) ||
            !fromHex(key, parsed.ticketPublicKey.data(), parsed.ticketPublicKey.size()) ||
            !parsed.isValid()) return false;
        out = std::move(parsed);
        return true;
    } catch (...) { out = {}; return false; }
}

json contentToJson(const OnlineContentDescriptor& value) {
    return {{"content_id", value.contentId},
            {"content_version", value.contentVersion},
            {"content_seed", value.contentSeed}};
}

bool contentFromJson(const json& value, OnlineContentDescriptor& out) {
    try {
        OnlineContentDescriptor parsed;
        parsed.contentId = value.at("content_id").get<std::string>();
        parsed.contentVersion = value.at("content_version").get<std::string>();
        if (!readUnsigned(value, "content_seed", parsed.contentSeed) ||
            !parsed.isValid()) return false;
        out = std::move(parsed);
        return true;
    } catch (...) { out = {}; return false; }
}

json metadataToJson(const OnlineMetadata& metadata) {
    json value = json::object();
    for (const auto& [key, entry] : metadata) value[key] = entry;
    return value;
}

bool metadataFromJson(const json& value, OnlineMetadata& out) {
    if (!value.is_object()) return false;
    OnlineMetadata parsed;
    try {
        for (auto it = value.begin(); it != value.end(); ++it) {
            if (!it.value().is_string()) return false;
            parsed.emplace(it.key(), it.value().get<std::string>());
        }
    } catch (...) { return false; }
    out = std::move(parsed);
    return true;
}

json lobbyToJson(const LobbyInfo& value) {
    json members = json::array();
    for (const PeerId& peer : value.members) members.push_back(peer.value);
    return {{"lobby_id", value.lobbyId}, {"revision", value.revision},
            {"owner_peer_id", value.ownerPeerId.value}, {"name", value.name},
            {"region", value.region}, {"build_id", value.buildId},
            {"content", contentToJson(value.content)},
            {"capacity", value.capacity},
            {"state", static_cast<uint8_t>(value.state)},
            {"visibility", static_cast<uint8_t>(value.visibility)},
            {"metadata", metadataToJson(value.metadata)},
            {"password_protected", value.passwordProtected},
            {"members", std::move(members)}, {"session_id", value.sessionId}};
}

bool lobbyFromJson(const json& value, LobbyInfo& out) {
    try {
        LobbyInfo parsed;
        uint8_t state = 0;
        uint8_t visibility = 0;
        if (!readUnsigned(value, "lobby_id", parsed.lobbyId) ||
            !readUnsigned(value, "revision", parsed.revision) ||
            !readUnsigned(value, "capacity", parsed.capacity) ||
            !readUnsigned(value, "state", state) || state > 3 ||
            !readUnsigned(value, "session_id", parsed.sessionId) ||
            !readUnsigned(value, "visibility", visibility) || visibility > 2 ||
            !value.at("password_protected").is_boolean()) return false;
        parsed.ownerPeerId = PeerId{value.at("owner_peer_id").get<std::string>()};
        parsed.name = value.at("name").get<std::string>();
        parsed.region = value.at("region").get<std::string>();
        parsed.buildId = value.at("build_id").get<std::string>();
        if (!contentFromJson(value.at("content"), parsed.content)) return false;
        parsed.state = static_cast<LobbyState>(state);
        parsed.visibility = static_cast<LobbyVisibility>(visibility);
        parsed.passwordProtected = value.at("password_protected").get<bool>();
        if (!metadataFromJson(value.at("metadata"), parsed.metadata)) return false;
        for (const auto& peer : value.at("members")) {
            parsed.members.push_back(PeerId{peer.get<std::string>()});
        }
        if (!parsed.isValid()) return false;
        out = std::move(parsed);
        return true;
    } catch (...) { out = {}; return false; }
}

json dedicatedInfoToJson(const DedicatedServerInfo& value) {
    return {{"server_id", value.serverId}, {"instance_name", value.instanceName},
            {"region", value.region}, {"build_id", value.buildId},
            {"address", value.address}, {"port", value.port},
            {"capacity", value.capacity},
            {"reserved_players", value.reservedPlayers},
            {"draining", value.draining},
            {"lease_expires_at", value.leaseExpiresAtUnixSeconds}};
}

bool dedicatedInfoFromJson(const json& value, DedicatedServerInfo& out) {
    try {
        DedicatedServerInfo parsed;
        if (!readUnsigned(value, "server_id", parsed.serverId) ||
            !readUnsigned(value, "port", parsed.port) ||
            !readUnsigned(value, "capacity", parsed.capacity) ||
            !readUnsigned(value, "reserved_players", parsed.reservedPlayers) ||
            !readUnsigned(value, "lease_expires_at",
                          parsed.leaseExpiresAtUnixSeconds)) return false;
        parsed.instanceName = value.at("instance_name").get<std::string>();
        parsed.region = value.at("region").get<std::string>();
        parsed.buildId = value.at("build_id").get<std::string>();
        parsed.address = value.at("address").get<std::string>();
        parsed.draining = value.at("draining").get<bool>();
        if (parsed.serverId == 0 || parsed.address.empty() || parsed.port == 0 ||
            parsed.capacity == 0 || parsed.reservedPlayers > parsed.capacity) return false;
        out = std::move(parsed);
        return true;
    } catch (...) { out = {}; return false; }
}

json allocationToJson(const DedicatedAllocation& value) {
    return {{"allocation_id", value.allocationId}, {"server_id", value.serverId},
            {"address", value.address}, {"port", value.port},
            {"player_count", value.playerCount},
            {"reservation_token", value.reservationToken},
            {"expires_at", value.expiresAtUnixSeconds}};
}

bool allocationFromJson(const json& value, DedicatedAllocation& out) {
    try {
        DedicatedAllocation parsed;
        if (!readUnsigned(value, "allocation_id", parsed.allocationId) ||
            !readUnsigned(value, "server_id", parsed.serverId) ||
            !readUnsigned(value, "port", parsed.port) ||
            !readUnsigned(value, "player_count", parsed.playerCount) ||
            !readUnsigned(value, "expires_at", parsed.expiresAtUnixSeconds)) return false;
        parsed.address = value.at("address").get<std::string>();
        parsed.reservationToken = value.at("reservation_token").get<std::string>();
        if (!parsed.isValid()) return false;
        out = std::move(parsed);
        return true;
    } catch (...) { out = {}; return false; }
}

json matchRequestToJson(const MatchmakingRequest& value) {
    json members = json::array();
    for (const PeerId& peer : value.partyMembers) members.push_back(peer.value);
    return {{"party_members", std::move(members)},
            {"source_lobby_id", value.sourceLobbyId},
            {"source_lobby_revision", value.sourceLobbyRevision},
            {"party_leader_peer_id", value.partyLeaderPeerId.value},
            {"queue", value.queue},
            {"region", value.region}, {"build_id", value.buildId},
            {"content", contentToJson(value.content)},
            {"topology", static_cast<uint8_t>(value.topology)},
            {"target_players", value.targetPlayers},
            {"minimum_players", value.minimumPlayers},
            {"virtual_port", value.virtualPort},
            {"estimated_ping_ms", value.estimatedPingMs},
            {"max_ping_ms", value.maxPingMs},
            {"skill_rating", value.skillRating},
            {"skill_tolerance", value.skillTolerance},
            {"team_count", value.teamCount},
            {"allow_backfill", value.allowBackfill},
            {"require_acceptance", value.requireAcceptance}};
}

bool matchRequestFromJson(const json& value, MatchmakingRequest& out) {
    try {
        MatchmakingRequest parsed;
        uint8_t topology = 0;
        if (!readUnsigned(value, "topology", topology) || topology > 2 ||
            !readUnsigned(value, "source_lobby_id", parsed.sourceLobbyId) ||
            !readUnsigned(value, "source_lobby_revision",
                          parsed.sourceLobbyRevision) ||
            !readUnsigned(value, "target_players", parsed.targetPlayers) ||
            !readUnsigned(value, "minimum_players", parsed.minimumPlayers) ||
            !readUnsigned(value, "virtual_port", parsed.virtualPort) ||
            !readUnsigned(value, "estimated_ping_ms", parsed.estimatedPingMs) ||
            !readUnsigned(value, "max_ping_ms", parsed.maxPingMs) ||
            !readUnsigned(value, "skill_rating", parsed.skillRating) ||
            !readUnsigned(value, "skill_tolerance", parsed.skillTolerance) ||
            !readUnsigned(value, "team_count", parsed.teamCount) ||
            !value.at("allow_backfill").is_boolean() ||
            !value.at("require_acceptance").is_boolean()) return false;
        parsed.partyLeaderPeerId = PeerId{
            value.at("party_leader_peer_id").get<std::string>()};
        parsed.queue = value.at("queue").get<std::string>();
        parsed.region = value.at("region").get<std::string>();
        parsed.buildId = value.at("build_id").get<std::string>();
        if (!contentFromJson(value.at("content"), parsed.content)) return false;
        parsed.topology = static_cast<MatchTopology>(topology);
        parsed.allowBackfill = value.at("allow_backfill").get<bool>();
        parsed.requireAcceptance = value.at("require_acceptance").get<bool>();
        for (const auto& peer : value.at("party_members")) {
            parsed.partyMembers.push_back(PeerId{peer.get<std::string>()});
        }
        out = std::move(parsed);
        return true;
    } catch (...) { out = {}; return false; }
}

json matchTicketToJson(const MatchTicketInfo& value) {
    json grants = json::array();
    for (const auto& grant : value.assignment.p2pGrants) {
        grants.push_back(grantToJson(grant));
    }
    json placements = json::array();
    for (const auto& placement : value.assignment.placements) {
        placements.push_back({{"peer_id", placement.peerId.value},
                              {"team_index", placement.teamIndex}});
    }
    json accepted = json::array();
    for (const auto& peer : value.acceptedMembers) {
        accepted.push_back(peer.value);
    }
    const auto& assignmentContent = value.assignment.content.isValid()
        ? value.assignment.content : value.request.content;
    return {{"ticket_id", value.ticketId},
            {"state", static_cast<uint8_t>(value.state)},
            {"request", matchRequestToJson(value.request)},
            {"assignment", {{"match_id", value.assignment.matchId},
                            {"topology", static_cast<uint8_t>(
                                value.assignment.topology)},
                            {"content", contentToJson(assignmentContent)},
                            {"placements", std::move(placements)},
                            {"p2p_grants", std::move(grants)},
                            {"dedicated", allocationToJson(
                                value.assignment.dedicated)}}},
            {"accepted_members", std::move(accepted)},
            {"acceptance_expires_at", value.acceptanceExpiresAtUnixSeconds},
            {"failure", value.failure}};
}

bool matchTicketFromJson(const json& value, MatchTicketInfo& out) {
    try {
        MatchTicketInfo parsed;
        uint8_t state = 0;
        uint8_t topology = 0;
        if (!readUnsigned(value, "ticket_id", parsed.ticketId) ||
            !readUnsigned(value, "state", state) || state > 5 ||
            !matchRequestFromJson(value.at("request"), parsed.request) ||
            !readUnsigned(value.at("assignment"), "match_id",
                          parsed.assignment.matchId) ||
            !readUnsigned(value.at("assignment"), "topology", topology) ||
            topology > 2 ||
            !readUnsigned(value, "acceptance_expires_at",
                          parsed.acceptanceExpiresAtUnixSeconds)) return false;
        parsed.state = static_cast<MatchTicketState>(state);
        parsed.assignment.topology = static_cast<MatchTopology>(topology);
        if (!contentFromJson(value.at("assignment").at("content"),
                             parsed.assignment.content)) return false;
        parsed.failure = value.at("failure").get<std::string>();
        for (const auto& grant : value.at("assignment").at("p2p_grants")) {
            P2PSessionGrant parsedGrant;
            if (!grantFromJson(grant, parsedGrant)) return false;
            parsed.assignment.p2pGrants.push_back(std::move(parsedGrant));
        }
        for (const auto& placement :
             value.at("assignment").at("placements")) {
            MatchPlayerPlacement parsedPlacement;
            parsedPlacement.peerId = PeerId{
                placement.at("peer_id").get<std::string>()};
            if (!readUnsigned(placement, "team_index",
                              parsedPlacement.teamIndex) ||
                !parsedPlacement.peerId.isValid()) return false;
            parsed.assignment.placements.push_back(
                std::move(parsedPlacement));
        }
        for (const auto& peer : value.at("accepted_members")) {
            parsed.acceptedMembers.push_back(
                PeerId{peer.get<std::string>()});
        }
        const json& dedicated = value.at("assignment").at("dedicated");
        if (dedicated.value("allocation_id", uint64_t{0}) != 0 &&
            !allocationFromJson(dedicated, parsed.assignment.dedicated)) return false;
        out = std::move(parsed);
        return true;
    } catch (...) { out = {}; return false; }
}

json launchToJson(const LobbyLaunchResult& value) {
    json grants = json::array();
    for (const auto& grant : value.memberGrants) grants.push_back(grantToJson(grant));
    return {{"lobby", lobbyToJson(value.lobby)}, {"member_grants", std::move(grants)}};
}

bool launchFromJson(const json& value, LobbyLaunchResult& out) {
    try {
        LobbyLaunchResult parsed;
        if (!lobbyFromJson(value.at("lobby"), parsed.lobby)) return false;
        for (const auto& grant : value.at("member_grants")) {
            P2PSessionGrant parsedGrant;
            if (!grantFromJson(grant, parsedGrant)) return false;
            parsed.memberGrants.push_back(std::move(parsedGrant));
        }
        out = std::move(parsed);
        return true;
    } catch (...) { out = {}; return false; }
}

struct TransportResponse {
    OnlineServiceError error = OnlineServiceError::None;
    std::string message;
    json value;
};

TransportResponse decodeResponse(const httplib::Result& response) {
    if (!response) return {OnlineServiceError::BackendUnavailable,
                           "online service request failed", {}};
    const json body = json::parse(response->body, nullptr, false);
    if (!body.is_object() || !body.contains("ok") || !body["ok"].is_boolean()) {
        return {OnlineServiceError::InternalError,
                "malformed online service response", {}};
    }
    if (!body["ok"].get<bool>()) {
        return {parseError(body.value("error", "internal_error")),
                body.value("message", "online service rejected request"), {}};
    }
    if (response->status < 200 || response->status >= 300 ||
        !body.contains("value")) {
        return {OnlineServiceError::InternalError,
                "inconsistent online service response", {}};
    }
    return {OnlineServiceError::None, {}, body["value"]};
}

std::string urlEncode(const std::string& input) {
    std::string result;
    constexpr char allowed[] =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.~";
    for (unsigned char value : input) {
        if (std::char_traits<char>::find(allowed, sizeof(allowed) - 1,
                                         static_cast<char>(value))) {
            result.push_back(static_cast<char>(value));
        } else {
            result.push_back('%');
            result.push_back(kHex[value >> 4]);
            result.push_back(kHex[value & 0x0f]);
        }
    }
    return result;
}

} // namespace

struct HttpOnlineServices::Impl {
    explicit Impl(HttpOnlineServicesClientConfig input) : config(std::move(input)) {}

    template <typename Operation>
    TransportResponse invoke(Operation&& operation) const {
        if (config.serverAddress.empty() || config.serverPort == 0) {
            return {OnlineServiceError::InvalidRequest,
                    "invalid online HTTP client configuration", {}};
        }
        httplib::Client client(config.serverAddress, config.serverPort);
        auto setTimeout = [](uint32_t milliseconds, time_t& seconds,
                             time_t& microseconds) {
            seconds = static_cast<time_t>(milliseconds / 1000u);
            microseconds = static_cast<time_t>((milliseconds % 1000u) * 1000u);
        };
        time_t seconds = 0;
        time_t microseconds = 0;
        setTimeout(config.connectTimeoutMs, seconds, microseconds);
        client.set_connection_timeout(seconds, microseconds);
        setTimeout(config.requestTimeoutMs, seconds, microseconds);
        client.set_read_timeout(seconds, microseconds);
        client.set_write_timeout(seconds, microseconds);
        client.set_payload_max_length(kMaxResponseBytes);
        return decodeResponse(operation(client));
    }

    httplib::Headers playerHeaders(const std::string& token) const {
        return {{"Authorization", "Bearer " + token}};
    }
    httplib::Headers controlHeaders() const {
        return {{"X-AY-Server-Token", config.dedicatedControlToken}};
    }
    bool actorMatches(const PeerId& actor, const std::string& token) const {
        return config.localPeerId.isValid() && actor == config.localPeerId &&
               !token.empty();
    }
    std::string playerToken() const {
        if (!config.playerAccessTokenProvider) return config.playerAccessToken;
        try {
            return config.playerAccessTokenProvider();
        } catch (...) {
            return {};
        }
    }

    HttpOnlineServicesClientConfig config;
};

HttpOnlineServices::HttpOnlineServices(HttpOnlineServicesClientConfig config)
    : _impl(std::make_unique<Impl>(std::move(config))) {}
HttpOnlineServices::~HttpOnlineServices() = default;

OnlineServiceResult<LobbyInfo> HttpOnlineServices::createLobby(
    const CreateLobbyRequest& request) {
    const std::string token = _impl->playerToken();
    if (!_impl->actorMatches(request.ownerPeerId, token)) {
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::Unauthorized, "local lobby actor does not match token");
    }
    const json body{{"name", request.name}, {"region", request.region},
                    {"build_id", request.buildId},
                    {"content", contentToJson(request.content)},
                    {"capacity", request.capacity},
                    {"visibility", static_cast<uint8_t>(request.visibility)},
                    {"metadata", metadataToJson(request.metadata)},
                    {"password", request.password}};
    const auto response = _impl->invoke([&](httplib::Client& client) {
        return client.Post("/v1/lobbies", _impl->playerHeaders(token), body.dump(),
                           "application/json");
    });
    if (response.error != OnlineServiceError::None) {
        return OnlineServiceResult<LobbyInfo>::failure(response.error, response.message);
    }
    LobbyInfo info;
    if (!lobbyFromJson(response.value, info)) {
        return OnlineServiceResult<LobbyInfo>::failure(
            OnlineServiceError::InternalError, "invalid lobby response");
    }
    return OnlineServiceResult<LobbyInfo>::success(std::move(info));
}

OnlineServiceResult<std::vector<LobbyInfo>> HttpOnlineServices::listLobbies(
    const ListLobbiesRequest& request) {
    const std::string token = _impl->playerToken();
    if (!_impl->config.localPeerId.isValid() ||
        token.empty()) {
        return OnlineServiceResult<std::vector<LobbyInfo>>::failure(
            OnlineServiceError::Unauthorized, "player authentication is missing");
    }
    const std::string path = "/v1/lobbies?region=" + urlEncode(request.region) +
        "&build_id=" + urlEncode(request.buildId) +
        "&content_id=" + urlEncode(request.contentId) +
        "&metadata=" + urlEncode(metadataToJson(request.metadata).dump()) +
        "&minimum_open_slots=" + std::to_string(request.minimumOpenSlots) +
        "&limit=" + std::to_string(request.limit);
    const auto response = _impl->invoke([&](httplib::Client& client) {
        return client.Get(path, _impl->playerHeaders(token));
    });
    if (response.error != OnlineServiceError::None) {
        return OnlineServiceResult<std::vector<LobbyInfo>>::failure(
            response.error, response.message);
    }
    try {
        std::vector<LobbyInfo> result;
        for (const auto& entry : response.value) {
            LobbyInfo info;
            if (!lobbyFromJson(entry, info)) throw std::runtime_error("invalid");
            result.push_back(std::move(info));
        }
        return OnlineServiceResult<std::vector<LobbyInfo>>::success(std::move(result));
    } catch (...) {
        return OnlineServiceResult<std::vector<LobbyInfo>>::failure(
            OnlineServiceError::InternalError, "invalid lobby-list response");
    }
}

OnlineServiceResult<LobbyInfo> HttpOnlineServices::joinLobby(
    LobbyId lobbyId, const PeerId& actor) {
    return joinLobby(JoinLobbyRequest{lobbyId, actor});
}

OnlineServiceResult<LobbyInfo> HttpOnlineServices::joinLobby(
    const JoinLobbyRequest& request) {
    const std::string token = _impl->playerToken();
    if (!_impl->actorMatches(request.authenticatedPeer, token)) return
        OnlineServiceResult<LobbyInfo>::failure(
        OnlineServiceError::Unauthorized, "local lobby actor does not match token");
    const std::string path = "/v1/lobbies/" +
        std::to_string(request.lobbyId) + "/join";
    const json body{{"password", request.password},
                    {"invitation_token", request.invitationToken}};
    const auto response = _impl->invoke([&](httplib::Client& client) {
        return client.Post(path, _impl->playerHeaders(token), body.dump(),
                           "application/json");
    });
    if (response.error != OnlineServiceError::None) return
        OnlineServiceResult<LobbyInfo>::failure(response.error, response.message);
    LobbyInfo info;
    if (!lobbyFromJson(response.value, info)) return
        OnlineServiceResult<LobbyInfo>::failure(OnlineServiceError::InternalError,
                                                 "invalid lobby response");
    return OnlineServiceResult<LobbyInfo>::success(std::move(info));
}

OnlineServiceResult<LobbyInfo> HttpOnlineServices::leaveLobby(
    LobbyId lobbyId, const PeerId& actor) {
    const std::string token = _impl->playerToken();
    if (!_impl->actorMatches(actor, token)) return OnlineServiceResult<LobbyInfo>::failure(
        OnlineServiceError::Unauthorized, "local lobby actor does not match token");
    const std::string path = "/v1/lobbies/" + std::to_string(lobbyId) + "/leave";
    const auto response = _impl->invoke([&](httplib::Client& client) {
        return client.Post(path, _impl->playerHeaders(token), "{}", "application/json");
    });
    if (response.error != OnlineServiceError::None) return
        OnlineServiceResult<LobbyInfo>::failure(response.error, response.message);
    LobbyInfo info;
    if (!lobbyFromJson(response.value, info)) return
        OnlineServiceResult<LobbyInfo>::failure(OnlineServiceError::InternalError,
                                                 "invalid lobby response");
    return OnlineServiceResult<LobbyInfo>::success(std::move(info));
}

OnlineServiceResult<LobbyInfo> HttpOnlineServices::updateLobby(
    const UpdateLobbyRequest& request) {
    const std::string token = _impl->playerToken();
    if (!_impl->actorMatches(request.actorPeerId, token)) return
        OnlineServiceResult<LobbyInfo>::failure(OnlineServiceError::Unauthorized,
            "local lobby actor does not match token");
    const json body{{"expected_revision", request.expectedRevision},
                    {"name", request.name},
                    {"replace_metadata", request.replaceMetadata},
                    {"metadata", metadataToJson(request.metadata)},
                    {"set_visibility", request.setVisibility},
                    {"visibility", static_cast<uint8_t>(request.visibility)},
                    {"set_password", request.setPassword},
                    {"password", request.password}};
    const std::string path = "/v1/lobbies/" + std::to_string(request.lobbyId) +
        "/update";
    const auto response = _impl->invoke([&](httplib::Client& client) {
        return client.Post(path, _impl->playerHeaders(token), body.dump(),
                           "application/json");
    });
    if (response.error != OnlineServiceError::None) return
        OnlineServiceResult<LobbyInfo>::failure(response.error, response.message);
    LobbyInfo info;
    if (!lobbyFromJson(response.value, info)) return
        OnlineServiceResult<LobbyInfo>::failure(OnlineServiceError::InternalError,
                                                 "invalid lobby response");
    return OnlineServiceResult<LobbyInfo>::success(std::move(info));
}

OnlineServiceResult<LobbyInfo> HttpOnlineServices::getLobby(LobbyId lobbyId) {
    const std::string token = _impl->playerToken();
    if (!_impl->config.localPeerId.isValid() ||
        token.empty()) return
        OnlineServiceResult<LobbyInfo>::failure(OnlineServiceError::Unauthorized,
                                                 "player authentication is missing");
    const std::string path = "/v1/lobbies/" + std::to_string(lobbyId);
    const auto response = _impl->invoke([&](httplib::Client& client) {
        return client.Get(path, _impl->playerHeaders(token));
    });
    if (response.error != OnlineServiceError::None) return
        OnlineServiceResult<LobbyInfo>::failure(response.error, response.message);
    LobbyInfo info;
    if (!lobbyFromJson(response.value, info)) return
        OnlineServiceResult<LobbyInfo>::failure(OnlineServiceError::InternalError,
                                                 "invalid lobby response");
    return OnlineServiceResult<LobbyInfo>::success(std::move(info));
}

OnlineServiceResult<LobbyInvitation>
HttpOnlineServices::createLobbyInvitation(
    const CreateLobbyInvitationRequest& request) {
    const std::string token = _impl->playerToken();
    if (!_impl->actorMatches(request.actorPeerId, token)) {
        return OnlineServiceResult<LobbyInvitation>::failure(
            OnlineServiceError::Unauthorized,
            "local lobby actor does not match token");
    }
    const json body{{"expected_revision", request.expectedRevision},
                    {"lifetime_seconds", request.lifetimeSeconds},
                    {"max_uses", request.maxUses}};
    const std::string path = "/v1/lobbies/" +
        std::to_string(request.lobbyId) + "/invitations";
    const auto response = _impl->invoke([&](httplib::Client& client) {
        return client.Post(path, _impl->playerHeaders(token), body.dump(),
                           "application/json");
    });
    if (response.error != OnlineServiceError::None) {
        return OnlineServiceResult<LobbyInvitation>::failure(
            response.error, response.message);
    }
    try {
        LobbyInvitation invitation;
        if (!readUnsigned(response.value, "lobby_id", invitation.lobbyId) ||
            !readUnsigned(response.value, "expires_at",
                          invitation.expiresAtUnixSeconds) ||
            !readUnsigned(response.value, "remaining_uses",
                          invitation.remainingUses)) {
            throw std::runtime_error("invalid");
        }
        invitation.token = response.value.at("token").get<std::string>();
        if (!invitation.isValid()) throw std::runtime_error("invalid");
        return OnlineServiceResult<LobbyInvitation>::success(
            std::move(invitation));
    } catch (...) {
        return OnlineServiceResult<LobbyInvitation>::failure(
            OnlineServiceError::InternalError,
            "invalid lobby-invitation response");
    }
}

OnlineServiceResult<LobbyLaunchResult> HttpOnlineServices::launchLobbyP2P(
    const LaunchLobbyRequest& request) {
    const std::string token = _impl->playerToken();
    if (!_impl->actorMatches(request.actorPeerId, token)) return
        OnlineServiceResult<LobbyLaunchResult>::failure(
            OnlineServiceError::Unauthorized,
            "local lobby actor does not match token");
    const json body{{"expected_revision", request.expectedRevision},
                    {"virtual_port", request.virtualPort}};
    const std::string path = "/v1/lobbies/" + std::to_string(request.lobbyId) +
        "/launch-p2p";
    const auto response = _impl->invoke([&](httplib::Client& client) {
        return client.Post(path, _impl->playerHeaders(token), body.dump(),
                           "application/json");
    });
    if (response.error != OnlineServiceError::None) return
        OnlineServiceResult<LobbyLaunchResult>::failure(response.error,
                                                         response.message);
    LobbyLaunchResult result;
    if (!launchFromJson(response.value, result)) return
        OnlineServiceResult<LobbyLaunchResult>::failure(
            OnlineServiceError::InternalError, "invalid lobby-launch response");
    return OnlineServiceResult<LobbyLaunchResult>::success(std::move(result));
}

OnlineServiceResult<MatchTicketInfo> HttpOnlineServices::enqueueMatch(
    const MatchmakingRequest& request) {
    const std::string token = _impl->playerToken();
    if (!_impl->config.localPeerId.isValid() ||
        token.empty() ||
        (request.sourceLobbyId == 0 &&
         !containsPeer(request.partyMembers, _impl->config.localPeerId)) ||
        (request.sourceLobbyId != 0 &&
         request.partyLeaderPeerId != _impl->config.localPeerId)) return
        OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::Unauthorized, "local peer is not in match party");
    const auto response = _impl->invoke([&](httplib::Client& client) {
        return client.Post("/v1/matches", _impl->playerHeaders(token),
                           matchRequestToJson(request).dump(), "application/json");
    });
    if (response.error != OnlineServiceError::None) return
        OnlineServiceResult<MatchTicketInfo>::failure(response.error,
                                                       response.message);
    MatchTicketInfo info;
    if (!matchTicketFromJson(response.value, info)) return
        OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::InternalError, "invalid match response");
    return OnlineServiceResult<MatchTicketInfo>::success(std::move(info));
}

OnlineServiceResult<MatchTicketInfo> HttpOnlineServices::getMatch(
    MatchTicketId ticketId, const PeerId& actor) {
    const std::string token = _impl->playerToken();
    if (!_impl->actorMatches(actor, token)) return OnlineServiceResult<MatchTicketInfo>::failure(
        OnlineServiceError::Unauthorized, "local match actor does not match token");
    const std::string path = "/v1/matches/" + std::to_string(ticketId);
    const auto response = _impl->invoke([&](httplib::Client& client) {
        return client.Get(path, _impl->playerHeaders(token));
    });
    if (response.error != OnlineServiceError::None) return
        OnlineServiceResult<MatchTicketInfo>::failure(response.error,
                                                       response.message);
    MatchTicketInfo info;
    if (!matchTicketFromJson(response.value, info)) return
        OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::InternalError, "invalid match response");
    return OnlineServiceResult<MatchTicketInfo>::success(std::move(info));
}

OnlineServiceResult<MatchTicketInfo> HttpOnlineServices::cancelMatch(
    MatchTicketId ticketId, const PeerId& actor) {
    const std::string token = _impl->playerToken();
    if (!_impl->actorMatches(actor, token)) return OnlineServiceResult<MatchTicketInfo>::failure(
        OnlineServiceError::Unauthorized, "local match actor does not match token");
    const std::string path = "/v1/matches/" + std::to_string(ticketId) + "/cancel";
    const auto response = _impl->invoke([&](httplib::Client& client) {
        return client.Post(path, _impl->playerHeaders(token), "{}", "application/json");
    });
    if (response.error != OnlineServiceError::None) return
        OnlineServiceResult<MatchTicketInfo>::failure(response.error,
                                                       response.message);
    MatchTicketInfo info;
    if (!matchTicketFromJson(response.value, info)) return
        OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::InternalError, "invalid match response");
    return OnlineServiceResult<MatchTicketInfo>::success(std::move(info));
}

OnlineServiceResult<MatchTicketInfo> HttpOnlineServices::respondToMatch(
    const MatchAcceptanceRequest& request) {
    const std::string token = _impl->playerToken();
    if (!_impl->actorMatches(request.authenticatedPeer, token)) {
        return OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::Unauthorized,
            "local match actor does not match token");
    }
    const std::string path = "/v1/matches/" +
        std::to_string(request.ticketId) + "/respond";
    const json body{{"accept", request.accept}};
    const auto response = _impl->invoke([&](httplib::Client& client) {
        return client.Post(path, _impl->playerHeaders(token), body.dump(),
                           "application/json");
    });
    if (response.error != OnlineServiceError::None) {
        return OnlineServiceResult<MatchTicketInfo>::failure(
            response.error, response.message);
    }
    MatchTicketInfo info;
    if (!matchTicketFromJson(response.value, info)) {
        return OnlineServiceResult<MatchTicketInfo>::failure(
            OnlineServiceError::InternalError, "invalid match response");
    }
    return OnlineServiceResult<MatchTicketInfo>::success(std::move(info));
}

size_t HttpOnlineServices::runMatchmaking(size_t maxMatches) {
    const json body{{"max_matches", maxMatches}};
    const auto response = _impl->invoke([&](httplib::Client& client) {
        return client.Post("/v1/matches/run", _impl->controlHeaders(), body.dump(),
                           "application/json");
    });
    size_t processed = 0;
    if (response.error != OnlineServiceError::None ||
        !readUnsigned(response.value, "processed", processed)) return 0;
    return processed;
}

OnlineServiceResult<DedicatedServerGrant> HttpOnlineServices::registerServer(
    const DedicatedServerRegistration& request) {
    const json body{{"instance_name", request.instanceName}, {"region", request.region},
                    {"build_id", request.buildId}, {"address", request.address},
                    {"port", request.port}, {"capacity", request.capacity}};
    const auto response = _impl->invoke([&](httplib::Client& client) {
        return client.Post("/v1/dedicated/servers", _impl->controlHeaders(),
                           body.dump(), "application/json");
    });
    if (response.error != OnlineServiceError::None) return
        OnlineServiceResult<DedicatedServerGrant>::failure(response.error,
                                                            response.message);
    try {
        DedicatedServerGrant grant;
        if (!dedicatedInfoFromJson(response.value.at("server"), grant.server) ||
            !readUnsigned(response.value, "server_id", grant.credential.serverId)) {
            throw std::runtime_error("invalid");
        }
        grant.credential.token = response.value.at("server_token").get<std::string>();
        if (!grant.credential.isValid()) throw std::runtime_error("invalid");
        return OnlineServiceResult<DedicatedServerGrant>::success(std::move(grant));
    } catch (...) {
        return OnlineServiceResult<DedicatedServerGrant>::failure(
            OnlineServiceError::InternalError, "invalid server grant response");
    }
}

OnlineServiceResult<DedicatedServerInfo> HttpOnlineServices::heartbeatServer(
    const DedicatedServerCredential& credential) {
    const std::string path = "/v1/dedicated/servers/" +
        std::to_string(credential.serverId) + "/heartbeat";
    const httplib::Headers headers{{"Authorization", "Bearer " + credential.token}};
    const auto response = _impl->invoke([&](httplib::Client& client) {
        return client.Post(path, headers, "{}", "application/json");
    });
    if (response.error != OnlineServiceError::None) return
        OnlineServiceResult<DedicatedServerInfo>::failure(response.error,
                                                           response.message);
    DedicatedServerInfo info;
    if (!dedicatedInfoFromJson(response.value, info)) return
        OnlineServiceResult<DedicatedServerInfo>::failure(
            OnlineServiceError::InternalError, "invalid server response");
    return OnlineServiceResult<DedicatedServerInfo>::success(std::move(info));
}

OnlineServiceResult<DedicatedServerInfo> HttpOnlineServices::setServerDraining(
    const DedicatedServerCredential& credential, bool draining) {
    const std::string path = "/v1/dedicated/servers/" +
        std::to_string(credential.serverId) + "/drain";
    const httplib::Headers headers{{"Authorization", "Bearer " + credential.token}};
    const json body{{"draining", draining}};
    const auto response = _impl->invoke([&](httplib::Client& client) {
        return client.Post(path, headers, body.dump(), "application/json");
    });
    if (response.error != OnlineServiceError::None) return
        OnlineServiceResult<DedicatedServerInfo>::failure(response.error,
                                                           response.message);
    DedicatedServerInfo info;
    if (!dedicatedInfoFromJson(response.value, info)) return
        OnlineServiceResult<DedicatedServerInfo>::failure(
            OnlineServiceError::InternalError, "invalid server response");
    return OnlineServiceResult<DedicatedServerInfo>::success(std::move(info));
}

OnlineServiceResult<SessionServiceEmpty> HttpOnlineServices::unregisterServer(
    const DedicatedServerCredential& credential) {
    const std::string path = "/v1/dedicated/servers/" +
        std::to_string(credential.serverId) + "/unregister";
    const httplib::Headers headers{{"Authorization", "Bearer " + credential.token}};
    const auto response = _impl->invoke([&](httplib::Client& client) {
        return client.Post(path, headers, "{}", "application/json");
    });
    if (response.error != OnlineServiceError::None) return
        OnlineServiceResult<SessionServiceEmpty>::failure(response.error,
                                                           response.message);
    return OnlineServiceResult<SessionServiceEmpty>::success({});
}

OnlineServiceResult<DedicatedAllocation> HttpOnlineServices::allocateServer(
    const DedicatedAllocationRequest& request) {
    const json body{{"region", request.region}, {"build_id", request.buildId},
                    {"player_count", request.playerCount}};
    const auto response = _impl->invoke([&](httplib::Client& client) {
        return client.Post("/v1/dedicated/allocations", _impl->controlHeaders(),
                           body.dump(), "application/json");
    });
    if (response.error != OnlineServiceError::None) return
        OnlineServiceResult<DedicatedAllocation>::failure(response.error,
                                                           response.message);
    DedicatedAllocation allocation;
    if (!allocationFromJson(response.value, allocation)) return
        OnlineServiceResult<DedicatedAllocation>::failure(
            OnlineServiceError::InternalError, "invalid allocation response");
    return OnlineServiceResult<DedicatedAllocation>::success(std::move(allocation));
}

OnlineServiceResult<SessionServiceEmpty> HttpOnlineServices::releaseAllocation(
    DedicatedAllocationId allocationId, const std::string& token) {
    const std::string path = "/v1/dedicated/allocations/" +
        std::to_string(allocationId) + "/release";
    const httplib::Headers headers{{"Authorization", "Bearer " + token}};
    const auto response = _impl->invoke([&](httplib::Client& client) {
        return client.Post(path, headers, "{}", "application/json");
    });
    if (response.error != OnlineServiceError::None) return
        OnlineServiceResult<SessionServiceEmpty>::failure(response.error,
                                                           response.message);
    return OnlineServiceResult<SessionServiceEmpty>::success({});
}

OnlineServiceResult<std::vector<DedicatedServerInfo>>
HttpOnlineServices::listServers() {
    const auto response = _impl->invoke([&](httplib::Client& client) {
        return client.Get("/v1/dedicated/servers", _impl->controlHeaders());
    });
    if (response.error != OnlineServiceError::None) return
        OnlineServiceResult<std::vector<DedicatedServerInfo>>::failure(
            response.error, response.message);
    try {
        std::vector<DedicatedServerInfo> servers;
        for (const auto& entry : response.value) {
            DedicatedServerInfo info;
            if (!dedicatedInfoFromJson(entry, info)) throw std::runtime_error("invalid");
            servers.push_back(std::move(info));
        }
        return OnlineServiceResult<std::vector<DedicatedServerInfo>>::success(
            std::move(servers));
    } catch (...) {
        return OnlineServiceResult<std::vector<DedicatedServerInfo>>::failure(
            OnlineServiceError::InternalError, "invalid server-list response");
    }
}

void installHttpOnlineServicesRoutes(
    httplib::Server& server, HttpOnlineServicesRouteConfig config) {
    auto permit = [config](const httplib::Request& request,
                           httplib::Response& response) {
        return !config.permit || config.permit(request, response);
    };
    auto player = [config](const httplib::Request& request,
                           httplib::Response& response, PeerId& peer) {
        std::string token;
        if (readBearer(request, token) && config.playerAuthenticator &&
            config.playerAuthenticator(token, peer) && peer.isValid()) return true;
        writeFailure(response, OnlineServiceResult<SessionServiceEmpty>::failure(
            OnlineServiceError::Unauthorized, "player authentication failed"));
        return false;
    };
    auto control = [config](const httplib::Request& request,
                            httplib::Response& response) {
        const std::string token = request.get_header_value("X-AY-Server-Token");
        if (config.dedicatedControlAuthenticator &&
            config.dedicatedControlAuthenticator(token)) return true;
        writeFailure(response, OnlineServiceResult<SessionServiceEmpty>::failure(
            OnlineServiceError::Unauthorized, "dedicated control authentication failed"));
        return false;
    };

    if (config.lobbies) {
        server.Post("/v1/lobbies", [config, permit, player](
            const httplib::Request& request, httplib::Response& response) {
            if (!permit(request, response)) return;
            PeerId actor;
            if (!player(request, response, actor)) return;
            const json body = parseBody(request);
            CreateLobbyRequest input;
            uint8_t visibility = 0;
            input.ownerPeerId = actor;
            try {
                input.name = body.at("name").get<std::string>();
                input.region = body.at("region").get<std::string>();
                input.buildId = body.at("build_id").get<std::string>();
                input.password = body.at("password").get<std::string>();
            } catch (...) { badRequest(response, "invalid lobby body"); return; }
            if (!contentFromJson(body.value("content", json{}), input.content)) {
                badRequest(response, "invalid lobby content"); return;
            }
            if (!readUnsigned(body, "capacity", input.capacity) ||
                !readUnsigned(body, "visibility", visibility) ||
                visibility > 2 ||
                !metadataFromJson(body.value("metadata", json{}),
                                  input.metadata)) {
                badRequest(response, "invalid lobby capacity"); return;
            }
            input.visibility = static_cast<LobbyVisibility>(visibility);
            const auto result = config.lobbies->createLobby(input);
            if (!result) writeFailure(response, result);
            else writeSuccess(response, lobbyToJson(result.value));
        });

        server.Get("/v1/lobbies", [config, permit, player](
            const httplib::Request& request, httplib::Response& response) {
            if (!permit(request, response)) return;
            PeerId actor;
            if (!player(request, response, actor)) return;
            (void)actor;
            ListLobbiesRequest input;
            input.region = request.get_param_value("region");
            input.buildId = request.get_param_value("build_id");
            input.contentId = request.get_param_value("content_id");
            const json metadata = json::parse(
                request.get_param_value("metadata"), nullptr, false);
            if (!metadataFromJson(metadata, input.metadata)) {
                badRequest(response, "invalid lobby metadata filter"); return;
            }
            auto parseParam = [&](const char* key, auto& value) {
                const std::string text = request.get_param_value(key);
                if (text.empty()) return true;
                const auto parsed = std::from_chars(
                    text.data(), text.data() + text.size(), value);
                return parsed.ec == std::errc{} &&
                       parsed.ptr == text.data() + text.size();
            };
            if (!parseParam("minimum_open_slots", input.minimumOpenSlots) ||
                !parseParam("limit", input.limit)) {
                badRequest(response, "invalid lobby query"); return;
            }
            const auto result = config.lobbies->listLobbies(input);
            if (!result) { writeFailure(response, result); return; }
            json values = json::array();
            for (const auto& entry : result.value) values.push_back(lobbyToJson(entry));
            writeSuccess(response, values);
        });

        server.Get(R"(/v1/lobbies/(\d+))", [config, permit, player](
            const httplib::Request& request, httplib::Response& response) {
            if (!permit(request, response)) return;
            PeerId actor;
            uint64_t id = 0;
            if (!player(request, response, actor)) return;
            (void)actor;
            if (!parseId(request, 1, id)) { badRequest(response, "invalid lobby id"); return; }
            const auto result = config.lobbies->getLobby(id);
            if (!result) writeFailure(response, result);
            else writeSuccess(response, lobbyToJson(result.value));
        });

        auto membershipRoute = [&](const char* pattern, bool joining) {
            server.Post(pattern, [config, permit, player, joining](
                const httplib::Request& request, httplib::Response& response) {
                if (!permit(request, response)) return;
                PeerId actor;
                uint64_t id = 0;
                if (!player(request, response, actor)) return;
                if (!parseId(request, 1, id)) { badRequest(response, "invalid lobby id"); return; }
                OnlineServiceResult<LobbyInfo> result;
                if (joining) {
                    const json body = parseBody(request);
                    JoinLobbyRequest input;
                    input.lobbyId = id;
                    input.authenticatedPeer = actor;
                    try {
                        input.password = body.at("password").get<std::string>();
                        input.invitationToken =
                            body.at("invitation_token").get<std::string>();
                    } catch (...) {
                        badRequest(response, "invalid lobby credentials");
                        return;
                    }
                    result = config.lobbies->joinLobby(input);
                } else {
                    result = config.lobbies->leaveLobby(id, actor);
                }
                if (!result) writeFailure(response, result);
                else writeSuccess(response, lobbyToJson(result.value));
            });
        };
        membershipRoute(R"(/v1/lobbies/(\d+)/join)", true);
        membershipRoute(R"(/v1/lobbies/(\d+)/leave)", false);

        server.Post(R"(/v1/lobbies/(\d+)/update)", [config, permit, player](
            const httplib::Request& request, httplib::Response& response) {
            if (!permit(request, response)) return;
            UpdateLobbyRequest input;
            uint8_t visibility = 0;
            if (!player(request, response, input.actorPeerId)) return;
            const json body = parseBody(request);
            try {
                input.name = body.at("name").get<std::string>();
                input.replaceMetadata =
                    body.at("replace_metadata").get<bool>();
                input.setVisibility = body.at("set_visibility").get<bool>();
                input.setPassword = body.at("set_password").get<bool>();
                input.password = body.at("password").get<std::string>();
            }
            catch (...) { badRequest(response, "invalid lobby update"); return; }
            if (!parseId(request, 1, input.lobbyId) ||
                !readUnsigned(body, "expected_revision", input.expectedRevision) ||
                !readUnsigned(body, "visibility", visibility) || visibility > 2 ||
                !metadataFromJson(body.value("metadata", json{}),
                                  input.metadata)) {
                badRequest(response, "invalid lobby update"); return;
            }
            input.visibility = static_cast<LobbyVisibility>(visibility);
            const auto result = config.lobbies->updateLobby(input);
            if (!result) writeFailure(response, result);
            else writeSuccess(response, lobbyToJson(result.value));
        });

        server.Post(R"(/v1/lobbies/(\d+)/invitations)",
            [config, permit, player](const httplib::Request& request,
                                     httplib::Response& response) {
            if (!permit(request, response)) return;
            CreateLobbyInvitationRequest input;
            if (!player(request, response, input.actorPeerId)) return;
            const json body = parseBody(request);
            if (!parseId(request, 1, input.lobbyId) ||
                !readUnsigned(body, "expected_revision", input.expectedRevision) ||
                !readUnsigned(body, "lifetime_seconds", input.lifetimeSeconds) ||
                !readUnsigned(body, "max_uses", input.maxUses)) {
                badRequest(response, "invalid lobby invitation"); return;
            }
            const auto result = config.lobbies->createLobbyInvitation(input);
            if (!result) { writeFailure(response, result); return; }
            writeSuccess(response,
                json{{"lobby_id", result.value.lobbyId},
                     {"token", result.value.token},
                     {"expires_at", result.value.expiresAtUnixSeconds},
                     {"remaining_uses", result.value.remainingUses}});
        });

        server.Post(R"(/v1/lobbies/(\d+)/launch-p2p)",
            [config, permit, player](const httplib::Request& request,
                                     httplib::Response& response) {
            if (!permit(request, response)) return;
            LaunchLobbyRequest input;
            if (!player(request, response, input.actorPeerId)) return;
            const json body = parseBody(request);
            if (!parseId(request, 1, input.lobbyId) ||
                !readUnsigned(body, "expected_revision", input.expectedRevision) ||
                !readUnsigned(body, "virtual_port", input.virtualPort)) {
                badRequest(response, "invalid lobby launch"); return;
            }
            const auto result = config.lobbies->launchLobbyP2P(input);
            if (!result) writeFailure(response, result);
            else writeSuccess(response, launchToJson(result.value));
        });
    }

    if (config.matchmaking) {
        server.Post("/v1/matches", [config, permit, player](
            const httplib::Request& request, httplib::Response& response) {
            if (!permit(request, response)) return;
            PeerId actor;
            if (!player(request, response, actor)) return;
            MatchmakingRequest input;
            if (!matchRequestFromJson(parseBody(request), input)) {
                badRequest(response, "invalid matchmaking request");
                return;
            }
            bool authorized = false;
            if (input.sourceLobbyId != 0 && config.lobbies) {
                const auto lobby = config.lobbies->getLobby(input.sourceLobbyId);
                authorized = lobby && lobby.value.ownerPeerId == actor &&
                    lobby.value.revision == input.sourceLobbyRevision &&
                    lobby.value.state == LobbyState::Open;
                if (authorized) {
                    input.partyLeaderPeerId = actor;
                    input.partyMembers = lobby.value.members;
                }
            } else if (input.sourceLobbyId == 0 &&
                       containsPeer(input.partyMembers, actor)) {
                authorized = config.partyAuthorizer
                    ? config.partyAuthorizer(actor, input.partyMembers)
                    : (input.partyMembers.size() == 1 &&
                       input.partyMembers.front() == actor);
            }
            if (!authorized) {
                writeFailure(response,
                    OnlineServiceResult<SessionServiceEmpty>::failure(
                        OnlineServiceError::Unauthorized,
                        "party membership is not authorized"));
                return;
            }
            const auto result = config.matchmaking->enqueueMatch(input);
            if (!result) writeFailure(response, result);
            else writeSuccess(response, matchTicketToJson(result.value));
        });

        server.Get(R"(/v1/matches/(\d+))", [config, permit, player](
            const httplib::Request& request, httplib::Response& response) {
            if (!permit(request, response)) return;
            PeerId actor;
            uint64_t id = 0;
            if (!player(request, response, actor)) return;
            if (!parseId(request, 1, id)) { badRequest(response, "invalid match id"); return; }
            const auto result = config.matchmaking->getMatch(id, actor);
            if (!result) writeFailure(response, result);
            else writeSuccess(response, matchTicketToJson(result.value));
        });

        server.Post(R"(/v1/matches/(\d+)/cancel)", [config, permit, player](
            const httplib::Request& request, httplib::Response& response) {
            if (!permit(request, response)) return;
            PeerId actor;
            uint64_t id = 0;
            if (!player(request, response, actor)) return;
            if (!parseId(request, 1, id)) { badRequest(response, "invalid match id"); return; }
            const auto result = config.matchmaking->cancelMatch(id, actor);
            if (!result) writeFailure(response, result);
            else writeSuccess(response, matchTicketToJson(result.value));
        });

        server.Post(R"(/v1/matches/(\d+)/respond)", [config, permit, player](
            const httplib::Request& request, httplib::Response& response) {
            if (!permit(request, response)) return;
            MatchAcceptanceRequest input;
            if (!player(request, response, input.authenticatedPeer)) return;
            const json body = parseBody(request);
            if (!parseId(request, 1, input.ticketId) ||
                !body.contains("accept") || !body.at("accept").is_boolean()) {
                badRequest(response, "invalid match response"); return;
            }
            input.accept = body.at("accept").get<bool>();
            const auto result = config.matchmaking->respondToMatch(input);
            if (!result) writeFailure(response, result);
            else writeSuccess(response, matchTicketToJson(result.value));
        });

        server.Post("/v1/matches/run", [config, permit, control](
            const httplib::Request& request, httplib::Response& response) {
            if (!permit(request, response) || !control(request, response)) return;
            size_t maximum = 0;
            if (!readUnsigned(parseBody(request), "max_matches", maximum) ||
                maximum == 0 || maximum > 1000) {
                badRequest(response, "invalid matchmaking pump limit"); return;
            }
            writeSuccess(response,
                json{{"processed", config.matchmaking->runMatchmaking(maximum)}});
        });
    }

    if (config.dedicated) {
        server.Post("/v1/dedicated/servers", [config, permit, control](
            const httplib::Request& request, httplib::Response& response) {
            if (!permit(request, response) || !control(request, response)) return;
            const json body = parseBody(request);
            DedicatedServerRegistration input;
            try {
                input.instanceName = body.at("instance_name").get<std::string>();
                input.region = body.at("region").get<std::string>();
                input.buildId = body.at("build_id").get<std::string>();
                input.address = body.at("address").get<std::string>();
            } catch (...) { badRequest(response, "invalid server registration"); return; }
            if (!readUnsigned(body, "port", input.port) ||
                !readUnsigned(body, "capacity", input.capacity)) {
                badRequest(response, "invalid server registration"); return;
            }
            const auto result = config.dedicated->registerServer(input);
            if (!result) { writeFailure(response, result); return; }
            writeSuccess(response, json{{"server", dedicatedInfoToJson(result.value.server)},
                                        {"server_id", result.value.credential.serverId},
                                        {"server_token", result.value.credential.token}});
        });

        server.Get("/v1/dedicated/servers", [config, permit, control](
            const httplib::Request& request, httplib::Response& response) {
            if (!permit(request, response) || !control(request, response)) return;
            const auto result = config.dedicated->listServers();
            if (!result) { writeFailure(response, result); return; }
            json values = json::array();
            for (const auto& entry : result.value) {
                values.push_back(dedicatedInfoToJson(entry));
            }
            writeSuccess(response, values);
        });

        auto credentialRoute = [&](const char* pattern, auto operation) {
            server.Post(pattern, [config, permit, operation](
                const httplib::Request& request, httplib::Response& response) {
                if (!permit(request, response)) return;
                DedicatedServerCredential credential;
                if (!parseId(request, 1, credential.serverId) ||
                    !readBearer(request, credential.token)) {
                    badRequest(response, "invalid server credential"); return;
                }
                operation(config, request, response, credential);
            });
        };
        credentialRoute(R"(/v1/dedicated/servers/(\d+)/heartbeat)",
            [](const auto& config, const auto&, auto& response, const auto& credential) {
                const auto result = config.dedicated->heartbeatServer(credential);
                if (!result) writeFailure(response, result);
                else writeSuccess(response, dedicatedInfoToJson(result.value));
            });
        credentialRoute(R"(/v1/dedicated/servers/(\d+)/drain)",
            [](const auto& config, const auto& request, auto& response,
               const auto& credential) {
                const json body = parseBody(request);
                if (!body.is_object() || !body.contains("draining") ||
                    !body["draining"].is_boolean()) {
                    badRequest(response, "invalid drain request"); return;
                }
                const auto result = config.dedicated->setServerDraining(
                    credential, body["draining"].get<bool>());
                if (!result) writeFailure(response, result);
                else writeSuccess(response, dedicatedInfoToJson(result.value));
            });
        credentialRoute(R"(/v1/dedicated/servers/(\d+)/unregister)",
            [](const auto& config, const auto&, auto& response, const auto& credential) {
                const auto result = config.dedicated->unregisterServer(credential);
                if (!result) writeFailure(response, result);
                else writeSuccess(response, json::object());
            });

        server.Post("/v1/dedicated/allocations", [config, permit, control](
            const httplib::Request& request, httplib::Response& response) {
            if (!permit(request, response) || !control(request, response)) return;
            const json body = parseBody(request);
            DedicatedAllocationRequest input;
            try {
                input.region = body.at("region").get<std::string>();
                input.buildId = body.at("build_id").get<std::string>();
            } catch (...) { badRequest(response, "invalid allocation request"); return; }
            if (!readUnsigned(body, "player_count", input.playerCount)) {
                badRequest(response, "invalid allocation request"); return;
            }
            const auto result = config.dedicated->allocateServer(input);
            if (!result) writeFailure(response, result);
            else writeSuccess(response, allocationToJson(result.value));
        });

        server.Post(R"(/v1/dedicated/allocations/(\d+)/release)",
            [config, permit](const httplib::Request& request,
                             httplib::Response& response) {
            if (!permit(request, response)) return;
            uint64_t id = 0;
            std::string token;
            if (!parseId(request, 1, id) || !readBearer(request, token)) {
                badRequest(response, "invalid allocation credential"); return;
            }
            const auto result = config.dedicated->releaseAllocation(id, token);
            if (!result) writeFailure(response, result);
            else writeSuccess(response, json::object());
        });
    }
}

} // namespace ayt::net
