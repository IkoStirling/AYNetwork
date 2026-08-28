#include <AYNetwork/Session/HttpSessionService.h>

#include <httplib.h>
#include <nlohmann/json.hpp>

#include <atomic>
#include <charconv>
#include <chrono>
#include <functional>
#include <limits>
#include <mutex>
#include <thread>
#include <type_traits>
#include <unordered_map>

namespace ayt::net
{
namespace
{

using json = nlohmann::json;
constexpr size_t kMaxRequestBytes = 16u * 1024u;
constexpr size_t kMaxResponseBytes = 64u * 1024u;

constexpr char kHex[] = "0123456789abcdef";

std::string toHex(const uint8_t* bytes, size_t size) {
    std::string out(size * 2, '0');
    for (size_t i = 0; i < size; ++i) {
        out[i * 2] = kHex[bytes[i] >> 4];
        out[i * 2 + 1] = kHex[bytes[i] & 0x0f];
    }
    return out;
}

int hexValue(char ch) {
    if (ch >= '0' && ch <= '9') return ch - '0';
    if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
    if (ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
    return -1;
}

bool fromHex(const std::string& text, uint8_t* bytes, size_t size) {
    if (!bytes || text.size() != size * 2) return false;
    for (size_t i = 0; i < size; ++i) {
        const int hi = hexValue(text[i * 2]);
        const int lo = hexValue(text[i * 2 + 1]);
        if (hi < 0 || lo < 0) return false;
        bytes[i] = static_cast<uint8_t>((hi << 4) | lo);
    }
    return true;
}

const char* errorName(SessionServiceError error) {
    switch (error) {
    case SessionServiceError::None: return "none";
    case SessionServiceError::InvalidRequest: return "invalid_request";
    case SessionServiceError::Unauthorized: return "unauthorized";
    case SessionServiceError::SessionNotFound: return "session_not_found";
    case SessionServiceError::SessionFull: return "session_full";
    case SessionServiceError::SessionClosed: return "session_closed";
    case SessionServiceError::EpochConflict: return "epoch_conflict";
    case SessionServiceError::HostLeaseExpired: return "host_lease_expired";
    case SessionServiceError::TransportError: return "transport_error";
    case SessionServiceError::ProtocolError: return "protocol_error";
    case SessionServiceError::InternalError: return "internal_error";
    case SessionServiceError::RateLimited: return "rate_limited";
    }
    return "internal_error";
}

SessionServiceError parseError(const std::string& name) {
    if (name == "none") return SessionServiceError::None;
    if (name == "invalid_request") return SessionServiceError::InvalidRequest;
    if (name == "unauthorized") return SessionServiceError::Unauthorized;
    if (name == "session_not_found") return SessionServiceError::SessionNotFound;
    if (name == "session_full") return SessionServiceError::SessionFull;
    if (name == "session_closed") return SessionServiceError::SessionClosed;
    if (name == "epoch_conflict") return SessionServiceError::EpochConflict;
    if (name == "host_lease_expired") return SessionServiceError::HostLeaseExpired;
    if (name == "transport_error") return SessionServiceError::TransportError;
    if (name == "protocol_error") return SessionServiceError::ProtocolError;
    if (name == "rate_limited") return SessionServiceError::RateLimited;
    return SessionServiceError::InternalError;
}

int statusFor(SessionServiceError error) {
    switch (error) {
    case SessionServiceError::None: return 200;
    case SessionServiceError::InvalidRequest:
    case SessionServiceError::ProtocolError: return 400;
    case SessionServiceError::Unauthorized: return 401;
    case SessionServiceError::SessionNotFound: return 404;
    case SessionServiceError::SessionFull:
    case SessionServiceError::SessionClosed:
    case SessionServiceError::EpochConflict: return 409;
    case SessionServiceError::HostLeaseExpired: return 410;
    case SessionServiceError::TransportError: return 502;
    case SessionServiceError::InternalError: return 500;
    case SessionServiceError::RateLimited: return 429;
    }
    return 500;
}

json sessionToJson(const P2PBackendSessionInfo& value) {
    return {
        {"session_id", value.sessionId},
        {"epoch", value.epoch},
        {"host_peer_id", value.hostPeerId.value},
        {"virtual_port", value.virtualPort},
        {"capacity", value.capacity},
        {"member_count", value.memberCount},
        {"open", value.open},
        {"host_lease_expires_at", value.hostLeaseExpiresAtUnixSeconds},
        {"signaling_address", value.signalingAddress},
        {"signaling_port", value.signalingPort},
        {"signaling_room", value.signalingRoom},
    };
}

template <typename T>
bool readUnsigned(const json& object, const char* key, T& out) {
    static_assert(std::is_unsigned_v<T>);
    if (!object.is_object() || !object.contains(key)) return false;
    const json& field = object[key];
    uint64_t value = 0;
    try {
        if (field.is_number_unsigned()) {
            value = field.get<uint64_t>();
        } else if (field.is_number_integer()) {
            const int64_t signedValue = field.get<int64_t>();
            if (signedValue < 0) return false;
            value = static_cast<uint64_t>(signedValue);
        } else {
            return false;
        }
    } catch (...) {
        return false;
    }
    if (value > static_cast<uint64_t>((std::numeric_limits<T>::max)())) {
        return false;
    }
    out = static_cast<T>(value);
    return true;
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
            !readUnsigned(value, "signaling_port", parsed.signalingPort)) {
            return false;
        }
        parsed.hostPeerId = PeerId{
            value.at("host_peer_id").get<std::string>()};
        parsed.open = value.at("open").get<bool>();
        parsed.signalingAddress =
            value.at("signaling_address").get<std::string>();
        parsed.signalingRoom =
            value.at("signaling_room").get<std::string>();
        if (!parsed.isValid()) return false;
        out = std::move(parsed);
        return true;
    } catch (...) {
        out = {};
        return false;
    }
}

json grantToJson(const P2PSessionGrant& value) {
    return {
        {"session", sessionToJson(value.session)},
        {"peer_id", value.member.peerId.value},
        {"member_token", value.member.token},
        {"signaling_token", value.signalingToken},
        {"join_ticket", toHex(value.joinTicket.data(), value.joinTicket.size())},
        {"ticket_public_key", toHex(
            value.ticketPublicKey.data(), value.ticketPublicKey.size())},
    };
}

bool grantFromJson(const json& value, P2PSessionGrant& out) {
    try {
        if (!sessionFromJson(value.at("session"), out.session)) return false;
        out.member.sessionId = out.session.sessionId;
        out.member.peerId = PeerId{value.at("peer_id").get<std::string>()};
        out.member.token = value.at("member_token").get<std::string>();
        out.signalingToken = value.at("signaling_token").get<std::string>();
        const std::string ticketHex = value.at("join_ticket").get<std::string>();
        const std::string publicKeyHex =
            value.at("ticket_public_key").get<std::string>();
        if ((ticketHex.size() & 1u) != 0 || ticketHex.empty()) return false;
        out.joinTicket.resize(ticketHex.size() / 2);
        if (!fromHex(ticketHex, out.joinTicket.data(), out.joinTicket.size()) ||
            !fromHex(publicKeyHex, out.ticketPublicKey.data(),
                     out.ticketPublicKey.size())) {
            out = {};
            return false;
        }
        return out.isValid();
    } catch (...) {
        out = {};
        return false;
    }
}

template <typename T>
void writeFailure(httplib::Response& response,
                  const SessionServiceResult<T>& result) {
    response.status = statusFor(result.error);
    response.set_content(json{
        {"ok", false},
        {"error", errorName(result.error)},
        {"message", result.message},
    }.dump(), "application/json");
}

void writeSuccess(httplib::Response& response, const json& value) {
    response.status = 200;
    response.set_content(json{{"ok", true}, {"value", value}}.dump(),
                         "application/json");
}

bool parseSessionId(const httplib::Request& request, uint64_t& id) {
    if (request.matches.size() < 2) return false;
    const std::string text = request.matches[1].str();
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), id);
    return parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size() && id != 0;
}

json parseBody(const httplib::Request& request) {
    if (request.body.size() > kMaxRequestBytes) return json::value_t::discarded;
    return json::parse(request.body, nullptr, false);
}

bool readPeer(const json& body, const char* key, PeerId& peer) {
    if (!body.is_object() || !body.contains(key) || !body[key].is_string()) return false;
    peer = PeerId{body[key].get<std::string>()};
    return peer.isValid();
}

bool readBearer(const httplib::Request& request, std::string& token) {
    constexpr const char* prefix = "Bearer ";
    const std::string header = request.get_header_value("Authorization");
    if (header.rfind(prefix, 0) != 0) return false;
    token = header.substr(7);
    return token.size() == 64;
}

struct TransportResponse {
    SessionServiceError error = SessionServiceError::None;
    std::string message;
    json value;
};

TransportResponse decodeResponse(const httplib::Result& response) {
    if (!response) {
        return {SessionServiceError::TransportError,
                "session service request failed", {}};
    }
    const json body = json::parse(response->body, nullptr, false);
    if (!body.is_object() || !body.contains("ok") || !body["ok"].is_boolean()) {
        return {SessionServiceError::ProtocolError,
                "malformed session service response", {}};
    }
    if (!body["ok"].get<bool>()) {
        return {parseError(body.value("error", "internal_error")),
                body.value("message", "session service rejected request"), {}};
    }
    if (response->status < 200 || response->status >= 300 ||
        !body.contains("value")) {
        return {SessionServiceError::ProtocolError,
                "inconsistent session service response", {}};
    }
    return {SessionServiceError::None, {}, body["value"]};
}

} // namespace

struct HttpP2PSessionService::Impl {
    explicit Impl(HttpP2PSessionClientConfig input) : config(std::move(input)) {}

    template <typename Operation>
    TransportResponse invoke(Operation&& operation) const {
        if (config.serverAddress.empty() || config.serverPort == 0) {
            return {SessionServiceError::InvalidRequest,
                    "invalid HTTP session client configuration", {}};
        }
        httplib::Client client(config.serverAddress, config.serverPort);
        const auto timeout = [](uint32_t millis, time_t& seconds,
                                time_t& microseconds) {
            seconds = static_cast<time_t>(millis / 1000u);
            microseconds = static_cast<time_t>((millis % 1000u) * 1000u);
        };
        time_t seconds = 0;
        time_t microseconds = 0;
        timeout(config.connectTimeoutMs, seconds, microseconds);
        client.set_connection_timeout(seconds, microseconds);
        timeout(config.requestTimeoutMs, seconds, microseconds);
        client.set_read_timeout(seconds, microseconds);
        client.set_write_timeout(seconds, microseconds);
        client.set_payload_max_length(kMaxResponseBytes);
        return decodeResponse(operation(client));
    }

    HttpP2PSessionClientConfig config;
};

HttpP2PSessionService::HttpP2PSessionService(HttpP2PSessionClientConfig config)
    : _impl(std::make_unique<Impl>(std::move(config))) {}

HttpP2PSessionService::~HttpP2PSessionService() = default;

SessionServiceResult<P2PSessionGrant>
HttpP2PSessionService::createSession(const P2PSessionCreateRequest& request) {
    const json body{
        {"host_peer_id", request.hostPeerId.value},
        {"virtual_port", request.virtualPort},
        {"capacity", request.capacity},
    };
    httplib::Headers headers;
    if (!_impl->config.admissionToken.empty()) {
        headers.emplace("X-AY-Admission-Token", _impl->config.admissionToken);
    }
    const auto response = _impl->invoke([&](httplib::Client& client) {
        return client.Post("/v1/sessions", headers, body.dump(),
                           "application/json");
    });
    if (response.error != SessionServiceError::None) {
        return SessionServiceResult<P2PSessionGrant>::failure(
            response.error, response.message);
    }
    P2PSessionGrant grant;
    if (!grantFromJson(response.value, grant)) {
        return SessionServiceResult<P2PSessionGrant>::failure(
            SessionServiceError::ProtocolError, "invalid create-session grant");
    }
    return SessionServiceResult<P2PSessionGrant>::success(std::move(grant));
}

SessionServiceResult<P2PSessionGrant>
HttpP2PSessionService::joinSession(const P2PSessionJoinRequest& request) {
    const json body{{"peer_id", request.peerId.value}};
    const std::string path = "/v1/sessions/" +
        std::to_string(request.sessionId) + "/join";
    httplib::Headers headers;
    if (!_impl->config.admissionToken.empty()) {
        headers.emplace("X-AY-Admission-Token", _impl->config.admissionToken);
    }
    const auto response = _impl->invoke([&](httplib::Client& client) {
        return client.Post(path, headers, body.dump(), "application/json");
    });
    if (response.error != SessionServiceError::None) {
        return SessionServiceResult<P2PSessionGrant>::failure(
            response.error, response.message);
    }
    P2PSessionGrant grant;
    if (!grantFromJson(response.value, grant)) {
        return SessionServiceResult<P2PSessionGrant>::failure(
            SessionServiceError::ProtocolError, "invalid join-session grant");
    }
    return SessionServiceResult<P2PSessionGrant>::success(std::move(grant));
}

SessionServiceResult<P2PBackendSessionInfo>
HttpP2PSessionService::heartbeat(const P2PSessionHeartbeatRequest& request) {
    const json body{
        {"peer_id", request.member.peerId.value},
        {"expected_epoch", request.expectedEpoch},
    };
    const std::string path = "/v1/sessions/" +
        std::to_string(request.member.sessionId) + "/heartbeat";
    const httplib::Headers headers{{"Authorization",
        "Bearer " + request.member.token}};
    const auto response = _impl->invoke([&](httplib::Client& client) {
        return client.Post(path, headers, body.dump(), "application/json");
    });
    if (response.error != SessionServiceError::None) {
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            response.error, response.message);
    }
    P2PBackendSessionInfo info;
    if (!sessionFromJson(response.value, info)) {
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::ProtocolError, "invalid heartbeat response");
    }
    return SessionServiceResult<P2PBackendSessionInfo>::success(std::move(info));
}

SessionServiceResult<P2PBackendSessionInfo>
HttpP2PSessionService::claimHost(const P2PSessionClaimHostRequest& request) {
    const json body{
        {"peer_id", request.member.peerId.value},
        {"expected_epoch", request.expectedEpoch},
        {"new_host_peer_id", request.newHostPeerId.value},
    };
    const std::string path = "/v1/sessions/" +
        std::to_string(request.member.sessionId) + "/claim-host";
    const httplib::Headers headers{{"Authorization",
        "Bearer " + request.member.token}};
    const auto response = _impl->invoke([&](httplib::Client& client) {
        return client.Post(path, headers, body.dump(), "application/json");
    });
    if (response.error != SessionServiceError::None) {
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            response.error, response.message);
    }
    P2PBackendSessionInfo info;
    if (!sessionFromJson(response.value, info)) {
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::ProtocolError, "invalid Host-claim response");
    }
    return SessionServiceResult<P2PBackendSessionInfo>::success(std::move(info));
}

SessionServiceResult<SessionServiceEmpty>
HttpP2PSessionService::leaveSession(const P2PSessionLeaveRequest& request) {
    const json body{{"peer_id", request.member.peerId.value}};
    const std::string path = "/v1/sessions/" +
        std::to_string(request.member.sessionId) + "/leave";
    const httplib::Headers headers{{"Authorization",
        "Bearer " + request.member.token}};
    const auto response = _impl->invoke([&](httplib::Client& client) {
        return client.Post(path, headers, body.dump(), "application/json");
    });
    if (response.error != SessionServiceError::None) {
        return SessionServiceResult<SessionServiceEmpty>::failure(
            response.error, response.message);
    }
    return SessionServiceResult<SessionServiceEmpty>::success({});
}

SessionServiceResult<P2PBackendSessionInfo>
HttpP2PSessionService::getSession(uint64_t sessionId) {
    const std::string path = "/v1/sessions/" +
        std::to_string(sessionId);
    const auto response = _impl->invoke([&](httplib::Client& client) {
        return client.Get(path);
    });
    if (response.error != SessionServiceError::None) {
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            response.error, response.message);
    }
    P2PBackendSessionInfo info;
    if (!sessionFromJson(response.value, info)) {
        return SessionServiceResult<P2PBackendSessionInfo>::failure(
            SessionServiceError::ProtocolError, "invalid session response");
    }
    return SessionServiceResult<P2PBackendSessionInfo>::success(std::move(info));
}

struct HttpP2PSessionServer::Impl {
    Impl(HttpP2PSessionServerConfig input,
         std::shared_ptr<IP2PSessionService> sessionService)
        : config(std::move(input)), service(std::move(sessionService)) {
        server.set_payload_max_length(kMaxRequestBytes);
        installRoutes();
    }

    void badRequest(httplib::Response& response, const char* message) {
        const auto result = SessionServiceResult<SessionServiceEmpty>::failure(
            SessionServiceError::InvalidRequest, message);
        writeFailure(response, result);
    }

    bool credentialFrom(const httplib::Request& request,
                        const json& body, uint64_t sessionId,
                        P2PSessionMemberCredential& credential) {
        credential.sessionId = sessionId;
        return readPeer(body, "peer_id", credential.peerId) &&
               readBearer(request, credential.token);
    }

    bool permit(const httplib::Request& request,
                httplib::Response& response) {
        if (config.rateLimitRequestsPerMinute == 0) return true;
        const auto now = std::chrono::steady_clock::now();
        const std::string source = request.remote_addr.empty()
            ? std::string{"unknown"} : request.remote_addr;
        bool allowed = false;
        {
            std::lock_guard<std::mutex> lock(rateMutex);
            auto [it, inserted] = rateBuckets.try_emplace(source);
            RateBucket& bucket = it->second;
            const double burst = static_cast<double>(config.rateLimitBurst);
            if (inserted) {
                bucket.tokens = burst;
                bucket.updated = now;
            } else {
                const double elapsedMinutes =
                    std::chrono::duration<double, std::ratio<60>>(
                        now - bucket.updated).count();
                bucket.tokens = (std::min)(
                    burst, bucket.tokens + elapsedMinutes *
                        static_cast<double>(config.rateLimitRequestsPerMinute));
                bucket.updated = now;
            }
            if (bucket.tokens >= 1.0) {
                bucket.tokens -= 1.0;
                allowed = true;
            }
        }
        if (allowed) return true;
        writeFailure(response,
            SessionServiceResult<SessionServiceEmpty>::failure(
                SessionServiceError::RateLimited,
                "session service rate limit exceeded"));
        response.set_header("Retry-After", "1");
        return false;
    }

    bool authorizeAdmission(const httplib::Request& request,
                            const PeerId& peer,
                            httplib::Response& response) const {
        if (!config.requireAdmissionAuthentication) return true;
        const std::string token =
            request.get_header_value("X-AY-Admission-Token");
        bool accepted = false;
        try {
            accepted = config.admissionAuthenticator &&
                config.admissionAuthenticator(token, peer);
        } catch (...) {
            accepted = false;
        }
        if (accepted) return true;
        writeFailure(response,
            SessionServiceResult<SessionServiceEmpty>::failure(
                SessionServiceError::Unauthorized,
                "session admission denied"));
        return false;
    }

    void installRoutes() {
        server.Post("/v1/sessions", [this](const httplib::Request& request,
                                           httplib::Response& response) {
            if (!permit(request, response)) return;
            const json body = parseBody(request);
            P2PSessionCreateRequest input;
            if (!readPeer(body, "host_peer_id", input.hostPeerId)) {
                badRequest(response, "invalid host_peer_id");
                return;
            }
            if (!authorizeAdmission(request, input.hostPeerId, response)) return;
            if (!readUnsigned(body, "virtual_port", input.virtualPort) ||
                !readUnsigned(body, "capacity", input.capacity)) {
                badRequest(response, "invalid create-session body");
                return;
            }
            const auto result = service->createSession(input);
            if (!result) writeFailure(response, result);
            else writeSuccess(response, grantToJson(result.value));
        });

        server.Post(R"(/v1/sessions/(\d+)/join)",
            [this](const httplib::Request& request, httplib::Response& response) {
                if (!permit(request, response)) return;
                P2PSessionJoinRequest input;
                const json body = parseBody(request);
                if (!parseSessionId(request, input.sessionId) ||
                    !readPeer(body, "peer_id", input.peerId)) {
                    badRequest(response, "invalid join request");
                    return;
                }
                if (!authorizeAdmission(request, input.peerId, response)) return;
                const auto result = service->joinSession(input);
                if (!result) writeFailure(response, result);
                else writeSuccess(response, grantToJson(result.value));
            });

        server.Post(R"(/v1/sessions/(\d+)/heartbeat)",
            [this](const httplib::Request& request, httplib::Response& response) {
                if (!permit(request, response)) return;
                uint64_t sessionId = 0;
                const json body = parseBody(request);
                P2PSessionHeartbeatRequest input;
                if (!parseSessionId(request, sessionId) ||
                    !credentialFrom(request, body, sessionId, input.member)) {
                    badRequest(response, "invalid heartbeat credential");
                    return;
                }
                if (!readUnsigned(body, "expected_epoch",
                                  input.expectedEpoch) ||
                    input.expectedEpoch == 0) {
                    badRequest(response, "invalid heartbeat epoch");
                    return;
                }
                const auto result = service->heartbeat(input);
                if (!result) writeFailure(response, result);
                else writeSuccess(response, sessionToJson(result.value));
            });

        server.Post(R"(/v1/sessions/(\d+)/claim-host)",
            [this](const httplib::Request& request, httplib::Response& response) {
                if (!permit(request, response)) return;
                uint64_t sessionId = 0;
                const json body = parseBody(request);
                P2PSessionClaimHostRequest input;
                if (!parseSessionId(request, sessionId) ||
                    !credentialFrom(request, body, sessionId, input.member) ||
                    !readPeer(body, "new_host_peer_id", input.newHostPeerId)) {
                    badRequest(response, "invalid Host claim credential");
                    return;
                }
                if (!readUnsigned(body, "expected_epoch",
                                  input.expectedEpoch) ||
                    input.expectedEpoch == 0) {
                    badRequest(response, "invalid Host claim epoch");
                    return;
                }
                const auto result = service->claimHost(input);
                if (!result) writeFailure(response, result);
                else writeSuccess(response, sessionToJson(result.value));
            });

        server.Post(R"(/v1/sessions/(\d+)/leave)",
            [this](const httplib::Request& request, httplib::Response& response) {
                if (!permit(request, response)) return;
                uint64_t sessionId = 0;
                const json body = parseBody(request);
                P2PSessionLeaveRequest input;
                if (!parseSessionId(request, sessionId) ||
                    !credentialFrom(request, body, sessionId, input.member)) {
                    badRequest(response, "invalid leave credential");
                    return;
                }
                const auto result = service->leaveSession(input);
                if (!result) writeFailure(response, result);
                else writeSuccess(response, json::object());
            });

        server.Get(R"(/v1/sessions/(\d+))",
            [this](const httplib::Request& request, httplib::Response& response) {
                if (!permit(request, response)) return;
                uint64_t sessionId = 0;
                if (!parseSessionId(request, sessionId)) {
                    badRequest(response, "invalid session id");
                    return;
                }
                const auto result = service->getSession(sessionId);
                if (!result) writeFailure(response, result);
                else writeSuccess(response, sessionToJson(result.value));
            });

        server.set_error_handler([](const httplib::Request&,
                                    httplib::Response& response) {
            if (response.status == 404) {
                response.set_content(json{
                    {"ok", false}, {"error", "session_not_found"},
                    {"message", "route not found"},
                }.dump(), "application/json");
            }
        });
        server.set_logger([this](const httplib::Request& request,
                                 const httplib::Response& response) {
            if (!config.auditSink) return;
            HttpP2PSessionServerConfig::AuditEvent event;
            event.method = request.method;
            event.path = request.path;
            event.remoteAddress = request.remote_addr;
            event.status = response.status;
            event.occurredAtUnixSeconds = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count());
            try {
                config.auditSink(event);
            } catch (...) {
                // Audit I/O must not tear down the HTTP worker thread.
            }
        });
    }

    struct RateBucket {
        double tokens = 0.0;
        std::chrono::steady_clock::time_point updated{};
    };

    HttpP2PSessionServerConfig config;
    std::shared_ptr<IP2PSessionService> service;
    httplib::Server server;
    std::thread thread;
    std::atomic<bool> running{false};
    uint16_t boundPort = 0;
    std::mutex rateMutex;
    std::unordered_map<std::string, RateBucket> rateBuckets;
};

HttpP2PSessionServer::HttpP2PSessionServer(
    HttpP2PSessionServerConfig config,
    std::shared_ptr<IP2PSessionService> service)
    : _impl(std::make_unique<Impl>(std::move(config), std::move(service))) {}

HttpP2PSessionServer::~HttpP2PSessionServer() { stop(); }

bool HttpP2PSessionServer::start() {
    if (_impl->running.load() || !_impl->service ||
        _impl->config.bindAddress.empty() ||
        (_impl->config.requireAdmissionAuthentication &&
         !_impl->config.admissionAuthenticator) ||
        ((_impl->config.rateLimitRequestsPerMinute == 0) !=
         (_impl->config.rateLimitBurst == 0))) return false;
    const int port = _impl->config.port == 0
        ? _impl->server.bind_to_any_port(_impl->config.bindAddress)
        : (_impl->server.bind_to_port(
               _impl->config.bindAddress, _impl->config.port)
               ? static_cast<int>(_impl->config.port) : -1);
    if (port <= 0 || port > 65535) return false;
    _impl->boundPort = static_cast<uint16_t>(port);
    _impl->running.store(true);
    _impl->thread = std::thread([impl = _impl.get()] {
        (void)impl->server.listen_after_bind();
        impl->running.store(false);
    });
    return true;
}

void HttpP2PSessionServer::stop() {
    if (!_impl) return;
    _impl->server.stop();
    if (_impl->thread.joinable()) _impl->thread.join();
    _impl->running.store(false);
}

bool HttpP2PSessionServer::isRunning() const {
    return _impl && _impl->running.load();
}

uint16_t HttpP2PSessionServer::getBoundPort() const {
    return _impl ? _impl->boundPort : 0;
}

} // namespace ayt::net
