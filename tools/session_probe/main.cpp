// Multi-process smoke probe for AYNetwork_SessionServer.

#include <AYNetwork/Session/HttpSessionService.h>
#include <AYNetwork/Session/P2PSessionBackend.h>
#include <AYNetwork/Session/SessionTicket.h>
#include <AYNetwork/Signaling/SecureUdpSignaling.h>

#include <charconv>
#include <chrono>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <memory>
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

bool waitReady(ayt::net::SecureUdpSignalingClient& first,
               ayt::net::SecureUdpSignalingClient& second) {
    const auto deadline = std::chrono::steady_clock::now() +
        std::chrono::seconds(5);
    const auto discard = [](const ayt::net::PeerId&, const void*, size_t) {};
    while (std::chrono::steady_clock::now() < deadline) {
        first.poll(discard);
        second.poll(discard);
        if (first.isReady() && second.isReady()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return false;
}

} // namespace

int main(int argc, char** argv) {
    if (argc != 3) {
        std::fprintf(stderr,
            "usage: AYNetwork_SessionProbe <server-address> <http-port>\n");
        return 2;
    }
    uint16_t httpPort = 0;
    if (!parsePort(argv[2], httpPort)) return 2;

    ayt::net::HttpP2PSessionClientConfig clientConfig;
    clientConfig.serverAddress = argv[1];
    clientConfig.serverPort = httpPort;
    if (const char* admission = std::getenv("AY_SESSION_ADMISSION_TOKEN")) {
        clientConfig.admissionToken = admission;
    }
    ayt::net::HttpP2PSessionService service(std::move(clientConfig));

    ayt::net::P2PSessionCreateRequest create;
    create.hostPeerId = ayt::net::PeerId{"session-host"};
    create.virtualPort = 7350;
    create.capacity = 3;
    const auto host = service.createSession(create);
    if (!host || !host.value.isValid()) return 10;

    ayt::net::P2PSessionJoinRequest join;
    join.sessionId = host.value.session.sessionId;
    join.peerId = ayt::net::PeerId{"session-join"};
    const auto member = service.joinSession(join);
    if (!member || !member.value.isValid()) return 11;
    if (member.value.session.epoch != host.value.session.epoch ||
        member.value.ticketPublicKey != host.value.ticketPublicKey) return 12;

    ayt::net::SessionJoinTicketClaims claims;
    if (ayt::net::verifySessionJoinTicket(
            member.value.joinTicket.data(), member.value.joinTicket.size(),
            member.value.ticketPublicKey, claims) !=
            ayt::net::SessionTicketError::None ||
        claims.peerId != join.peerId || claims.sessionId != join.sessionId) {
        return 13;
    }

    ayt::net::P2PConfig hostP2P;
    hostP2P.localPeerId = create.hostPeerId;
    ayt::net::SecureUdpSignalingClientConfig hostSignalConfig;
    if (!ayt::net::applyP2PSessionGrant(
            host.value, hostP2P, hostSignalConfig)) return 14;
    ayt::net::P2PConfig joinP2P;
    joinP2P.localPeerId = join.peerId;
    ayt::net::SecureUdpSignalingClientConfig joinSignalConfig;
    if (!ayt::net::applyP2PSessionGrant(
            member.value, joinP2P, joinSignalConfig)) return 15;

    ayt::net::SecureUdpSignalingClient hostSignal(hostSignalConfig);
    ayt::net::SecureUdpSignalingClient joinSignal(joinSignalConfig);
    const bool hostStarted = hostSignal.start(create.hostPeerId);
    const bool joinStarted = joinSignal.start(join.peerId);
    if (!hostStarted || !joinStarted || !waitReady(hostSignal, joinSignal)) {
        std::fprintf(stderr,
            "signal registration failed host_started=%d host_state=%u host_error=%u "
            "join_started=%d join_state=%u join_error=%u room=%s port=%u\n",
            hostStarted ? 1 : 0,
            static_cast<unsigned>(hostSignal.getState()),
            static_cast<unsigned>(hostSignal.getLastError()),
            joinStarted ? 1 : 0,
            static_cast<unsigned>(joinSignal.getState()),
            static_cast<unsigned>(joinSignal.getLastError()),
            hostSignalConfig.roomId.value.c_str(), hostSignalConfig.serverPort);
        return 16;
    }
    constexpr char payload[] = "session-issued-signal";
    if (!hostSignal.sendSignal(join.peerId, payload, sizeof(payload))) return 17;
    bool received = false;
    const auto discard = [](const ayt::net::PeerId&, const void*, size_t) {};
    const auto signalDeadline = std::chrono::steady_clock::now() +
        std::chrono::seconds(3);
    while (!received && std::chrono::steady_clock::now() < signalDeadline) {
        hostSignal.poll(discard);
        joinSignal.poll([&](const ayt::net::PeerId& sender,
                            const void* bytes, size_t size) {
            received = sender == create.hostPeerId && size == sizeof(payload) &&
                std::memcmp(bytes, payload, size) == 0;
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    hostSignal.stop();
    joinSignal.stop();
    if (!received) return 18;

    ayt::net::P2PSessionHeartbeatRequest heartbeat;
    heartbeat.member = host.value.member;
    heartbeat.expectedEpoch = host.value.session.epoch;
    const auto renewed = service.heartbeat(heartbeat);
    if (!renewed || renewed.value.epoch != heartbeat.expectedEpoch) return 19;

    ayt::net::P2PSessionClaimHostRequest claim;
    claim.member = host.value.member;
    claim.expectedEpoch = host.value.session.epoch;
    claim.newHostPeerId = join.peerId;
    const auto promoted = service.claimHost(claim);
    if (!promoted || promoted.value.epoch != claim.expectedEpoch + 1 ||
        promoted.value.hostPeerId != join.peerId) return 20;
    const auto stale = service.claimHost(claim);
    if (stale.error != ayt::net::SessionServiceError::EpochConflict) return 21;

    heartbeat.member = member.value.member;
    heartbeat.expectedEpoch = promoted.value.epoch;
    const auto newHostHeartbeat = service.heartbeat(heartbeat);
    if (!newHostHeartbeat ||
        newHostHeartbeat.value.hostPeerId != join.peerId) return 22;

    ayt::net::P2PSessionLeaveRequest leave;
    leave.member = host.value.member;
    if (!service.leaveSession(leave)) return 23;
    leave.member = member.value.member;
    if (!service.leaveSession(leave)) return 24;
    const auto missing = service.getSession(host.value.session.sessionId);
    if (missing.error != ayt::net::SessionServiceError::SessionNotFound) return 25;

    std::printf(
        "AY_SESSION_RESULT state=passed session=%llu epoch=%u host=%s\n",
        static_cast<unsigned long long>(promoted.value.sessionId),
        promoted.value.epoch,
        promoted.value.hostPeerId.value.c_str());
    return 0;
}
