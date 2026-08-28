#pragma once
// Ed25519-signed, bounded Join Ticket used by the authority-session backend.

#include <AYNetwork/SessionService.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace ayt::net
{

constexpr size_t kSessionTicketPublicKeyBytes = 32;
constexpr size_t kSessionTicketSecretKeyBytes = 64;
constexpr size_t kSessionTicketSignatureBytes = 64;
constexpr size_t kSessionTicketNonceBytes = 16;

using SessionTicketPublicKey =
    std::array<uint8_t, kSessionTicketPublicKeyBytes>;
using SessionTicketSecretKey =
    std::array<uint8_t, kSessionTicketSecretKeyBytes>;

struct SessionTicketKeyPair {
    SessionTicketPublicKey publicKey{};
    SessionTicketSecretKey secretKey{};
};

struct SessionJoinTicketClaims {
    uint64_t sessionId = 0;
    uint32_t epoch = 0;
    PeerId peerId;
    uint64_t issuedAtUnixSeconds = 0;
    uint64_t expiresAtUnixSeconds = 0;
    std::array<uint8_t, kSessionTicketNonceBytes> nonce{};
};

enum class SessionTicketError : uint8_t {
    None = 0,
    InvalidInput,
    InvalidFormat,
    InvalidSignature,
    NotYetValid,
    Expired,
    SessionMismatch,
    EpochMismatch,
    PeerMismatch,
};

bool generateSessionTicketKeyPair(SessionTicketKeyPair& keyPair);
bool validateSessionTicketKeyPair(const SessionTicketKeyPair& keyPair);

enum class SessionTicketKeyFileError : uint8_t {
    None = 0,
    NotFound,
    InvalidFormat,
    InvalidKey,
    IoError,
};

// Stores the signing identity in a versioned binary file. save refuses to
// replace an existing file; loadOrCreate therefore remains safe when two local
// service processes race during first startup.
SessionTicketKeyFileError loadSessionTicketKeyPair(
    const std::string& path, SessionTicketKeyPair& keyPair);
SessionTicketKeyFileError saveSessionTicketKeyPair(
    const std::string& path, const SessionTicketKeyPair& keyPair);
SessionTicketKeyFileError loadOrCreateSessionTicketKeyPair(
    const std::string& path, SessionTicketKeyPair& keyPair);

bool issueSessionJoinTicket(const SessionJoinTicketClaims& claims,
                            const SessionTicketSecretKey& secretKey,
                            std::vector<uint8_t>& ticket);

SessionTicketError verifySessionJoinTicket(
    const void* ticket, size_t size,
    const SessionTicketPublicKey& publicKey,
    SessionJoinTicketClaims& claims,
    uint64_t nowUnixSeconds = 0);

P2PJoinDecision validateP2PSessionJoinTicket(
    const SessionTicketPublicKey& publicKey,
    uint64_t expectedSessionId, uint32_t expectedEpoch,
    const PeerId& expectedPeer,
    const void* ticket, size_t size,
    uint64_t nowUnixSeconds = 0);

} // namespace ayt::net
