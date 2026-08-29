#include <AYNetwork/Session/P2PSessionBackend.h>

namespace ayt::net
{
namespace
{

bool applyIdentity(const P2PSessionGrant& grant, P2PConfig& p2pConfig,
                   SignalingToken& token) {
    if (!grant.isValid() || grant.member.peerId != p2pConfig.localPeerId ||
        grant.session.virtualPort == 0 ||
        !parseSignalingTokenHex(grant.signalingToken, token)) return false;
    p2pConfig.virtualPort = grant.session.virtualPort;
    p2pConfig.sessionId = grant.session.sessionId;
    p2pConfig.sessionEpoch = grant.session.epoch;
    return true;
}

} // namespace

bool applyP2PSessionGrant(const P2PSessionGrant& grant,
                          P2PConfig& p2pConfig,
                          SecureUdpSignalingClientConfig& signalingConfig) {
    SignalingToken token;
    if (usesWebSocketSignaling(grant) ||
        !applyIdentity(grant, p2pConfig, token)) return false;

    signalingConfig.serverAddress = grant.session.signalingAddress;
    signalingConfig.serverPort = grant.session.signalingPort;
    signalingConfig.roomId = SignalingRoomId{grant.session.signalingRoom};
    signalingConfig.token = token;
    return signalingConfig.roomId.isValid();
}

bool usesWebSocketSignaling(const P2PSessionGrant& grant) {
    return grant.session.signalingAddress.rfind("ws://", 0) == 0 ||
           grant.session.signalingAddress.rfind("wss://", 0) == 0;
}

bool applyP2PSessionGrant(const P2PSessionGrant& grant,
                          P2PConfig& p2pConfig,
                          WebSocketSignalingClientConfig& signalingConfig) {
    SignalingToken token;
    if (!usesWebSocketSignaling(grant) ||
        !applyIdentity(grant, p2pConfig, token)) return false;
    const bool tls = grant.session.signalingAddress.rfind("wss://", 0) == 0;
    const size_t prefix = tls ? 6 : 5;
    const std::string endpoint =
        grant.session.signalingAddress.substr(prefix);
    const size_t slash = endpoint.find('/');
    signalingConfig.serverAddress = endpoint.substr(0, slash);
    signalingConfig.path = slash == std::string::npos
        ? "/v1/signaling" : endpoint.substr(slash);
    signalingConfig.serverPort = grant.session.signalingPort;
    signalingConfig.useTls = tls;
    signalingConfig.roomId = SignalingRoomId{grant.session.signalingRoom};
    signalingConfig.token = token;
    return signalingConfig.isValid();
}

P2PBackendJoinValidator makeP2PSessionJoinValidator(
    const P2PSessionGrant& grant) {
    // Every admitted member installs the same verifier.  A Client may become
    // Host after migration, so restricting construction to the original Host
    // would leave the promoted authority unable to validate new Join Tickets.
    if (!grant.isValid()) return {};
    const SessionTicketPublicKey key = grant.ticketPublicKey;
    const uint64_t sessionId = grant.session.sessionId;
    return [key, sessionId](uint64_t activeSessionId, uint32_t activeEpoch,
                            const PeerId& peer,
                            const uint8_t* ticket, size_t size) {
        if (activeSessionId != sessionId || activeEpoch == 0) {
            return P2PJoinDecision::reject(P2PJoinRejectReason::InvalidTicket);
        }
        return validateP2PSessionJoinTicket(
            key, sessionId, activeEpoch, peer, ticket, size);
    };
}

} // namespace ayt::net
