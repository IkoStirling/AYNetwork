#pragma once

#include <AYNetwork/Session/HttpSessionService.h>

#include <httplib.h>

#include <functional>
#include <memory>

namespace ayt::net
{

struct HttpOnlineServicesRouteConfig {
    std::shared_ptr<ILobbyService> lobbies;
    std::shared_ptr<IMatchmakingService> matchmaking;
    std::shared_ptr<IDedicatedServerService> dedicated;
    std::function<bool(const httplib::Request&, httplib::Response&)> permit;
    std::function<bool(std::string_view, PeerId&)> playerAuthenticator;
    std::function<bool(const PeerId&, const std::vector<PeerId>&)> partyAuthorizer;
    std::function<bool(std::string_view)> dedicatedControlAuthenticator;
};

void installHttpOnlineServicesRoutes(
    httplib::Server& server, HttpOnlineServicesRouteConfig config);

} // namespace ayt::net
