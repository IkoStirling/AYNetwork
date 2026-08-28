#pragma once
// Helpers that apply a session-service grant to existing AYNetwork P2P APIs.

#include <AYNetwork/Session/SessionTicket.h>
#include <AYNetwork/SessionService.h>
#include <AYNetwork/Signaling/SecureUdpSignaling.h>

#include <functional>

namespace ayt::net
{

using P2PBackendJoinValidator = std::function<P2PJoinDecision(
    uint64_t, uint32_t, const PeerId&, const uint8_t*, size_t)>;

bool applyP2PSessionGrant(const P2PSessionGrant& grant,
                          P2PConfig& p2pConfig,
                          SecureUdpSignalingClientConfig& signalingConfig);

P2PBackendJoinValidator makeP2PSessionJoinValidator(
    const P2PSessionGrant& grant);

} // namespace ayt::net
