#include <AYNetwork/Session/SessionTicket.h>

#include <sodium.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <system_error>

namespace ayt::net
{
namespace
{

constexpr uint8_t kMagic[4] = {'A', 'Y', 'S', 'T'};
constexpr uint8_t kVersion = 1;
constexpr uint8_t kKeyFileMagic[8] = {'A', 'Y', 'S', 'K', 'E', 'Y', '0', '1'};
constexpr size_t kKeyFileBytes = sizeof(kKeyFileMagic) +
    kSessionTicketPublicKeyBytes + kSessionTicketSecretKeyBytes;
constexpr size_t kFixedClaimsBytes =
    4 + 1 + 8 + 4 + 8 + 8 + 1 + kSessionTicketNonceBytes;

uint64_t unixNow() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}

void appendU32(std::vector<uint8_t>& out, uint32_t value) {
    for (unsigned i = 0; i < 4; ++i) {
        out.push_back(static_cast<uint8_t>((value >> (i * 8)) & 0xffu));
    }
}

void appendU64(std::vector<uint8_t>& out, uint64_t value) {
    for (unsigned i = 0; i < 8; ++i) {
        out.push_back(static_cast<uint8_t>((value >> (i * 8)) & 0xffu));
    }
}

uint32_t readU32(const uint8_t* data) {
    uint32_t value = 0;
    for (unsigned i = 0; i < 4; ++i) value |= uint32_t(data[i]) << (i * 8);
    return value;
}

uint64_t readU64(const uint8_t* data) {
    uint64_t value = 0;
    for (unsigned i = 0; i < 8; ++i) value |= uint64_t(data[i]) << (i * 8);
    return value;
}

bool sodiumReady() {
    static const bool ready = sodium_init() >= 0;
    return ready;
}

void wipeKeyPair(SessionTicketKeyPair& keyPair) {
    if (sodiumReady()) {
        sodium_memzero(keyPair.secretKey.data(), keyPair.secretKey.size());
    }
    keyPair = {};
}

} // namespace

bool generateSessionTicketKeyPair(SessionTicketKeyPair& keyPair) {
    keyPair = {};
    if (!sodiumReady()) return false;
    if (crypto_sign_keypair(keyPair.publicKey.data(),
                            keyPair.secretKey.data()) == 0) return true;
    keyPair = {};
    return false;
}

bool validateSessionTicketKeyPair(const SessionTicketKeyPair& keyPair) {
    if (!sodiumReady()) return false;
    SessionTicketPublicKey derived{};
    if (crypto_sign_ed25519_sk_to_pk(
            derived.data(), keyPair.secretKey.data()) != 0) return false;
    return sodium_memcmp(derived.data(), keyPair.publicKey.data(),
                         derived.size()) == 0;
}

SessionTicketKeyFileError loadSessionTicketKeyPair(
    const std::string& path, SessionTicketKeyPair& keyPair) {
    wipeKeyPair(keyPair);
    if (path.empty()) return SessionTicketKeyFileError::InvalidFormat;
    std::ifstream input(std::filesystem::u8path(path), std::ios::binary);
    if (!input) {
        std::error_code error;
        return std::filesystem::exists(std::filesystem::u8path(path), error)
            ? SessionTicketKeyFileError::IoError
            : SessionTicketKeyFileError::NotFound;
    }
    std::array<uint8_t, kKeyFileBytes> encoded{};
    input.read(reinterpret_cast<char*>(encoded.data()),
               static_cast<std::streamsize>(encoded.size()));
    char trailing = 0;
    if (input.gcount() != static_cast<std::streamsize>(encoded.size()) ||
        input.read(&trailing, 1)) {
        return SessionTicketKeyFileError::InvalidFormat;
    }
    if (!std::equal(std::begin(kKeyFileMagic), std::end(kKeyFileMagic),
                    encoded.begin())) {
        return SessionTicketKeyFileError::InvalidFormat;
    }
    size_t offset = sizeof(kKeyFileMagic);
    std::copy_n(encoded.begin() + offset, keyPair.publicKey.size(),
                keyPair.publicKey.begin());
    offset += keyPair.publicKey.size();
    std::copy_n(encoded.begin() + offset, keyPair.secretKey.size(),
                keyPair.secretKey.begin());
    if (!validateSessionTicketKeyPair(keyPair)) {
        wipeKeyPair(keyPair);
        return SessionTicketKeyFileError::InvalidKey;
    }
    return SessionTicketKeyFileError::None;
}

SessionTicketKeyFileError saveSessionTicketKeyPair(
    const std::string& path, const SessionTicketKeyPair& keyPair) {
    if (path.empty() || !validateSessionTicketKeyPair(keyPair)) {
        return SessionTicketKeyFileError::InvalidKey;
    }
    const std::filesystem::path destination = std::filesystem::u8path(path);
    std::error_code error;
    if (std::filesystem::exists(destination, error)) {
        return error ? SessionTicketKeyFileError::IoError
                     : SessionTicketKeyFileError::IoError;
    }
    if (!destination.parent_path().empty()) {
        std::filesystem::create_directories(destination.parent_path(), error);
        if (error) return SessionTicketKeyFileError::IoError;
    }

    std::array<uint8_t, kKeyFileBytes> encoded{};
    std::copy(std::begin(kKeyFileMagic), std::end(kKeyFileMagic),
              encoded.begin());
    size_t offset = sizeof(kKeyFileMagic);
    std::copy(keyPair.publicKey.begin(), keyPair.publicKey.end(),
              encoded.begin() + offset);
    offset += keyPair.publicKey.size();
    std::copy(keyPair.secretKey.begin(), keyPair.secretKey.end(),
              encoded.begin() + offset);

    const auto suffix = std::to_string(
        std::chrono::steady_clock::now().time_since_epoch().count());
    const std::filesystem::path temporary = destination.string() + ".tmp." + suffix;
    {
        std::ofstream output(temporary, std::ios::binary | std::ios::trunc);
        if (!output) return SessionTicketKeyFileError::IoError;
        output.write(reinterpret_cast<const char*>(encoded.data()),
                     static_cast<std::streamsize>(encoded.size()));
        output.flush();
        if (!output) {
            output.close();
            std::filesystem::remove(temporary, error);
            return SessionTicketKeyFileError::IoError;
        }
    }
    std::filesystem::rename(temporary, destination, error);
    if (error) {
        std::error_code cleanupError;
        std::filesystem::remove(temporary, cleanupError);
        return SessionTicketKeyFileError::IoError;
    }
    return SessionTicketKeyFileError::None;
}

SessionTicketKeyFileError loadOrCreateSessionTicketKeyPair(
    const std::string& path, SessionTicketKeyPair& keyPair) {
    SessionTicketKeyFileError result = loadSessionTicketKeyPair(path, keyPair);
    if (result != SessionTicketKeyFileError::NotFound) return result;
    SessionTicketKeyPair generated;
    if (!generateSessionTicketKeyPair(generated)) {
        return SessionTicketKeyFileError::IoError;
    }
    result = saveSessionTicketKeyPair(path, generated);
    if (result == SessionTicketKeyFileError::None) {
        keyPair = generated;
        return result;
    }
    // Another process may have atomically installed the same service identity.
    result = loadSessionTicketKeyPair(path, keyPair);
    wipeKeyPair(generated);
    return result;
}

bool issueSessionJoinTicket(const SessionJoinTicketClaims& claims,
                            const SessionTicketSecretKey& secretKey,
                            std::vector<uint8_t>& ticket) {
    ticket.clear();
    if (!sodiumReady() || claims.sessionId == 0 || claims.epoch == 0 ||
        !claims.peerId.isValid() || claims.issuedAtUnixSeconds == 0 ||
        claims.expiresAtUnixSeconds <= claims.issuedAtUnixSeconds ||
        claims.peerId.value.size() > std::numeric_limits<uint8_t>::max()) {
        return false;
    }

    ticket.reserve(kFixedClaimsBytes + claims.peerId.value.size() +
                   kSessionTicketSignatureBytes);
    ticket.insert(ticket.end(), std::begin(kMagic), std::end(kMagic));
    ticket.push_back(kVersion);
    appendU64(ticket, claims.sessionId);
    appendU32(ticket, claims.epoch);
    appendU64(ticket, claims.issuedAtUnixSeconds);
    appendU64(ticket, claims.expiresAtUnixSeconds);
    ticket.push_back(static_cast<uint8_t>(claims.peerId.value.size()));
    ticket.insert(ticket.end(), claims.peerId.value.begin(), claims.peerId.value.end());
    ticket.insert(ticket.end(), claims.nonce.begin(), claims.nonce.end());

    const size_t signedSize = ticket.size();
    ticket.resize(signedSize + kSessionTicketSignatureBytes);
    unsigned long long signatureSize = 0;
    if (crypto_sign_detached(ticket.data() + signedSize, &signatureSize,
                             ticket.data(), static_cast<unsigned long long>(signedSize),
                             secretKey.data()) != 0 ||
        signatureSize != kSessionTicketSignatureBytes) {
        ticket.clear();
        return false;
    }
    return ticket.size() <= kP2PMaxJoinTicketBytes;
}

SessionTicketError verifySessionJoinTicket(
    const void* ticket, size_t size,
    const SessionTicketPublicKey& publicKey,
    SessionJoinTicketClaims& claims,
    uint64_t nowUnixSeconds) {
    claims = {};
    if (!ticket || !sodiumReady()) return SessionTicketError::InvalidInput;
    if (size < kFixedClaimsBytes + 1 + kSessionTicketSignatureBytes ||
        size > kP2PMaxJoinTicketBytes) {
        return SessionTicketError::InvalidFormat;
    }

    const auto* bytes = static_cast<const uint8_t*>(ticket);
    if (!std::equal(std::begin(kMagic), std::end(kMagic), bytes) ||
        bytes[4] != kVersion) {
        return SessionTicketError::InvalidFormat;
    }

    constexpr size_t kPeerLengthOffset = 4 + 1 + 8 + 4 + 8 + 8;
    const size_t peerLength = bytes[kPeerLengthOffset];
    const size_t signedSize = kFixedClaimsBytes + peerLength;
    if (peerLength == 0 || size != signedSize + kSessionTicketSignatureBytes) {
        return SessionTicketError::InvalidFormat;
    }
    if (crypto_sign_verify_detached(bytes + signedSize, bytes,
                                    static_cast<unsigned long long>(signedSize),
                                    publicKey.data()) != 0) {
        return SessionTicketError::InvalidSignature;
    }

    size_t offset = 5;
    claims.sessionId = readU64(bytes + offset);
    offset += 8;
    claims.epoch = readU32(bytes + offset);
    offset += 4;
    claims.issuedAtUnixSeconds = readU64(bytes + offset);
    offset += 8;
    claims.expiresAtUnixSeconds = readU64(bytes + offset);
    offset += 8;
    offset += 1;
    claims.peerId = PeerId{std::string(
        reinterpret_cast<const char*>(bytes + offset), peerLength)};
    offset += peerLength;
    std::copy_n(bytes + offset, claims.nonce.size(), claims.nonce.begin());

    if (claims.sessionId == 0 || claims.epoch == 0 || !claims.peerId.isValid() ||
        claims.issuedAtUnixSeconds == 0 ||
        claims.expiresAtUnixSeconds <= claims.issuedAtUnixSeconds) {
        claims = {};
        return SessionTicketError::InvalidFormat;
    }
    const uint64_t now = nowUnixSeconds == 0 ? unixNow() : nowUnixSeconds;
    constexpr uint64_t kClockSkewSeconds = 30;
    if (claims.issuedAtUnixSeconds > now + kClockSkewSeconds) {
        return SessionTicketError::NotYetValid;
    }
    if (now >= claims.expiresAtUnixSeconds) return SessionTicketError::Expired;
    return SessionTicketError::None;
}

P2PJoinDecision validateP2PSessionJoinTicket(
    const SessionTicketPublicKey& publicKey,
    uint64_t expectedSessionId, uint32_t expectedEpoch,
    const PeerId& expectedPeer,
    const void* ticket, size_t size,
    uint64_t nowUnixSeconds) {
    if (!ticket || size == 0) {
        return P2PJoinDecision::reject(P2PJoinRejectReason::MissingTicket);
    }
    SessionJoinTicketClaims claims;
    const SessionTicketError result = verifySessionJoinTicket(
        ticket, size, publicKey, claims, nowUnixSeconds);
    if (result != SessionTicketError::None ||
        claims.sessionId != expectedSessionId ||
        claims.epoch != expectedEpoch || claims.peerId != expectedPeer) {
        return P2PJoinDecision::reject(P2PJoinRejectReason::InvalidTicket);
    }
    return P2PJoinDecision::accept();
}

} // namespace ayt::net
