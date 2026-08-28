#pragma once
// Backend-neutral signed player bearer used by Online Services HTTP routes.

#include <AYNetwork/P2P.h>

#include <array>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

namespace ayt::net
{

enum class PlayerAccessTokenError : uint8_t {
    None = 0,
    InvalidConfiguration,
    Malformed,
    InvalidSignature,
    NotYetValid,
    Expired,
    LifetimeExceeded,
};

struct PlayerAccessTokenVerifierConfig {
    std::array<uint8_t, 32> signingKey{};
    uint32_t maximumLifetimeSeconds = 24u * 60u * 60u;
    uint32_t clockSkewSeconds = 30;
    std::function<uint64_t()> nowUnixSeconds;

    bool isValid() const;
};

// Issuance belongs in the application's account/login service. AYNetwork
// exposes the primitive so a non-Steam backend can mint the same opaque bearer
// consumed by SessionServer without embedding an account provider in the engine.
bool issuePlayerAccessToken(
    const PeerId& peerId, uint64_t issuedAtUnixSeconds,
    uint64_t expiresAtUnixSeconds, const std::array<uint8_t, 32>& signingKey,
    std::string& token);

class PlayerAccessTokenVerifier {
public:
    explicit PlayerAccessTokenVerifier(PlayerAccessTokenVerifierConfig config);

    bool isReady() const;
    PlayerAccessTokenError verify(std::string_view token, PeerId& peerId) const;

private:
    PlayerAccessTokenVerifierConfig _config;
    bool _ready = false;
};

} // namespace ayt::net
