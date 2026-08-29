// Multi-process smoke probe for SessionServer Online Services routes.

#include <AYNetwork/Session/HttpOnlineServices.h>

#include <charconv>
#include <chrono>
#include <cstdlib>
#include <cstdio>
#include <string>
#include <thread>

namespace
{

bool parsePort(const char* text, uint16_t& port) {
    if (!text) return false;
    unsigned value = 0;
    const std::string input{text};
    const auto result = std::from_chars(
        input.data(), input.data() + input.size(), value);
    if (result.ec != std::errc{} || result.ptr != input.data() + input.size() ||
        value == 0 || value > 65535) return false;
    port = static_cast<uint16_t>(value);
    return true;
}

std::string environment(const char* name) {
    const char* value = std::getenv(name);
    return value ? std::string{value} : std::string{};
}

ayt::net::HttpOnlineServices makeClient(
    const std::string& address, uint16_t port, const char* peer,
    const std::string& token, const std::string& fleetToken) {
    ayt::net::HttpOnlineServicesClientConfig config;
    config.serverAddress = address;
    config.serverPort = port;
    const std::string tls = environment("AY_SESSION_BACKEND_TLS");
    config.useTls = tls == "1" || tls == "true";
    config.localPeerId = ayt::net::PeerId{peer};
    config.playerAccessToken = token;
    config.dedicatedControlToken = fleetToken;
    return ayt::net::HttpOnlineServices(std::move(config));
}

} // namespace

int main(int argc, char** argv) {
    if (argc != 3) {
        std::fprintf(stderr,
            "usage: AYNetwork_OnlineProbe <server-address> <http-port>\n");
        return 2;
    }
    uint16_t port = 0;
    if (!parsePort(argv[2], port)) return 2;
    const std::string ownerToken = environment("AY_ONLINE_OWNER_TOKEN");
    const std::string guestToken = environment("AY_ONLINE_GUEST_TOKEN");
    const std::string fleetToken = environment("AY_ONLINE_SERVER_TOKEN");
    if (ownerToken.empty() || guestToken.empty() || fleetToken.empty()) return 2;

    auto owner = makeClient(argv[1], port, "online-owner", ownerToken, fleetToken);
    auto guest = makeClient(argv[1], port, "online-guest", guestToken, fleetToken);

    ayt::net::CreateLobbyRequest create;
    create.ownerPeerId = ayt::net::PeerId{"online-owner"};
    create.name = "E2E Lobby";
    create.region = "test-region";
    create.buildId = "test-build";
    create.content = {"maps/e2e", "content-1", 123};
    create.capacity = 2;
    const auto lobby = owner.createLobby(create);
    if (!lobby) return 10;
    const auto joined = guest.joinLobby(
        lobby.value.lobbyId, ayt::net::PeerId{"online-guest"});
    if (!joined || joined.value.members.size() != 2) return 11;
    if (owner.listLobbies({"test-region", "test-build", 0, 10}).value.size() != 1) {
        return 12;
    }

    ayt::net::LaunchLobbyRequest launch;
    launch.lobbyId = lobby.value.lobbyId;
    launch.actorPeerId = ayt::net::PeerId{"online-owner"};
    launch.expectedRevision = joined.value.revision;
    launch.virtualPort = 7350;
    const auto launched = owner.launchLobbyP2P(launch);
    if (!launched || launched.value.memberGrants.size() != 2) return 13;

    ayt::net::MatchmakingRequest first;
    first.partyMembers = {ayt::net::PeerId{"online-owner"}};
    first.queue = "e2e";
    first.region = "test-region";
    first.buildId = "test-build";
    first.content = {"maps/e2e", "content-1", 123};
    first.topology = ayt::net::MatchTopology::P2P;
    first.targetPlayers = 2;
    first.virtualPort = 7351;
    auto second = first;
    second.partyMembers = {ayt::net::PeerId{"online-guest"}};
    const auto firstTicket = owner.enqueueMatch(first);
    const auto secondTicket = guest.enqueueMatch(second);
    if (!firstTicket || !secondTicket) return 14;

    ayt::net::MatchTicketInfo firstResult;
    ayt::net::MatchTicketInfo secondResult;
    const auto deadline = std::chrono::steady_clock::now() +
        std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline) {
        const auto firstPoll = owner.getMatch(
            firstTicket.value.ticketId, ayt::net::PeerId{"online-owner"});
        const auto secondPoll = guest.getMatch(
            secondTicket.value.ticketId, ayt::net::PeerId{"online-guest"});
        if (!firstPoll || !secondPoll) return 15;
        firstResult = firstPoll.value;
        secondResult = secondPoll.value;
        if (firstResult.state == ayt::net::MatchTicketState::Matched &&
            secondResult.state == ayt::net::MatchTicketState::Matched) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (firstResult.state != ayt::net::MatchTicketState::Matched ||
        secondResult.state != ayt::net::MatchTicketState::Matched ||
        firstResult.assignment.p2pGrants.size() != 1 ||
        secondResult.assignment.p2pGrants.size() != 1) return 16;

    ayt::net::DedicatedServerRegistration registration;
    registration.instanceName = "e2e-dedicated";
    registration.region = "test-region";
    registration.buildId = "test-build";
    registration.address = "203.0.113.30";
    registration.port = 7200;
    registration.capacity = 8;
    const auto server = owner.registerServer(registration);
    if (!server || !owner.heartbeatServer(server.value.credential)) return 17;
    const auto allocation = owner.allocateServer(
        {"test-region", "test-build", 4, {}, 0,
         {"maps/e2e", "1", 1}});
    if (!allocation || !owner.setServerDraining(server.value.credential, true)) {
        return 18;
    }
    if (!owner.releaseAllocation(allocation.value.allocationId,
                                 allocation.value.reservationToken) ||
        !owner.unregisterServer(server.value.credential)) return 19;

    std::printf(
        "AY_ONLINE_RESULT state=passed lobby=%llu match_session=%llu\n",
        static_cast<unsigned long long>(lobby.value.lobbyId),
        static_cast<unsigned long long>(
            firstResult.assignment.p2pGrants.front().session.sessionId));
    return 0;
}
