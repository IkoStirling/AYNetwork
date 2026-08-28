#pragma once
// cpp-httplib transport for the backend-neutral P2P session contract.

#include <AYNetwork/SessionService.h>
#include <AYNetwork/OnlineServices.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

namespace ayt::net
{

struct HttpP2PSessionClientConfig {
    std::string serverAddress = "127.0.0.1";
    uint16_t serverPort = 0;
    uint32_t connectTimeoutMs = 2000;
    uint32_t requestTimeoutMs = 3000;
    // Optional deployment admission credential. It is sent only on create and
    // join; per-member bearer credentials continue to protect mutations.
    std::string admissionToken;
    // Optional thread-safe source for rotating admission credentials. When
    // present it takes precedence over admissionToken on each create/join.
    std::function<std::string()> admissionTokenProvider;
};

class HttpP2PSessionService final : public IP2PSessionService {
public:
    explicit HttpP2PSessionService(HttpP2PSessionClientConfig config);
    ~HttpP2PSessionService() override;

    HttpP2PSessionService(const HttpP2PSessionService&) = delete;
    HttpP2PSessionService& operator=(const HttpP2PSessionService&) = delete;

    SessionServiceResult<P2PSessionGrant> createSession(
        const P2PSessionCreateRequest& request) override;
    SessionServiceResult<P2PSessionGrant> joinSession(
        const P2PSessionJoinRequest& request) override;
    SessionServiceResult<P2PBackendSessionInfo> heartbeat(
        const P2PSessionHeartbeatRequest& request) override;
    SessionServiceResult<P2PBackendSessionInfo> claimHost(
        const P2PSessionClaimHostRequest& request) override;
    SessionServiceResult<SessionServiceEmpty> leaveSession(
        const P2PSessionLeaveRequest& request) override;
    SessionServiceResult<P2PBackendSessionInfo> getSession(
        uint64_t sessionId) override;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

struct HttpP2PSessionServerConfig {
    std::string bindAddress = "0.0.0.0";
    uint16_t port = 0;
    bool requireAdmissionAuthentication = false;
    std::function<bool(std::string_view, const PeerId&)>
        admissionAuthenticator;

    // Per-source token bucket. A zero rate disables limiting in development.
    uint32_t rateLimitRequestsPerMinute = 0;
    uint32_t rateLimitBurst = 0;
    size_t rateLimitTrackedSources = 16384;

    struct AuditEvent {
        std::string method;
        std::string path;
        std::string remoteAddress;
        int status = 0;
        uint64_t occurredAtUnixSeconds = 0;
    };
    std::function<void(const AuditEvent&)> auditSink;

    // Online-service routes derive the caller identity from this callback.
    // The request body is never allowed to choose actorPeerId.
    std::function<bool(std::string_view, PeerId&)> playerAuthenticator;
    std::function<bool(const PeerId&, const std::vector<PeerId>&)>
        partyAuthorizer;
    std::function<bool(std::string_view)> dedicatedControlAuthenticator;
};

class HttpP2PSessionServer {
public:
    HttpP2PSessionServer(HttpP2PSessionServerConfig config,
                         std::shared_ptr<IP2PSessionService> service,
                         std::shared_ptr<ILobbyService> lobbies = {},
                         std::shared_ptr<IMatchmakingService> matchmaking = {},
                         std::shared_ptr<IDedicatedServerService> dedicated = {});
    ~HttpP2PSessionServer();

    HttpP2PSessionServer(const HttpP2PSessionServer&) = delete;
    HttpP2PSessionServer& operator=(const HttpP2PSessionServer&) = delete;

    bool start();
    void stop();
    bool isRunning() const;
    uint16_t getBoundPort() const;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

} // namespace ayt::net
