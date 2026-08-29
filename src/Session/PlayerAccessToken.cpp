#include <AYNetwork/Session/PlayerAccessToken.h>

#include <sodium.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <utility>
#include <vector>

namespace ayt::net
{
namespace
{

constexpr std::string_view kVersion = "ay1";
constexpr size_t kNonceBytes = 16;
constexpr size_t kMacBytes = crypto_auth_hmacsha256_BYTES;
constexpr size_t kMaxTokenBytes = 256;

uint64_t systemNow() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}

bool hasNonZero(const std::array<uint8_t, 32>& value) {
    return std::any_of(value.begin(), value.end(),
                       [](uint8_t byte) { return byte != 0; });
}

std::string base64Url(const uint8_t* bytes, size_t size) {
    const size_t capacity = sodium_base64_encoded_len(
        size, sodium_base64_VARIANT_URLSAFE_NO_PADDING);
    std::string value(capacity, '\0');
    sodium_bin2base64(value.data(), value.size(), bytes, size,
                      sodium_base64_VARIANT_URLSAFE_NO_PADDING);
    value.resize(std::char_traits<char>::length(value.c_str()));
    return value;
}

bool decodeBase64Url(std::string_view text, uint8_t* bytes, size_t size,
                     size_t& written) {
    written = 0;
    if (!bytes || text.empty()) return false;
    if (sodium_base642bin(
        bytes, size, text.data(), text.size(), nullptr, &written, nullptr,
        sodium_base64_VARIANT_URLSAFE_NO_PADDING) != 0) return false;
    // Reject alternate spellings that only differ in unused trailing bits.
    // The MAC authenticates the payload fields, but the signature field itself
    // must also have one canonical wire representation.
    return base64Url(bytes, written) == text;
}

bool parseU64(std::string_view text, uint64_t& value) {
    if (text.empty()) return false;
    const auto result = std::from_chars(
        text.data(), text.data() + text.size(), value);
    return result.ec == std::errc{} && result.ptr == text.data() + text.size();
}

bool computeMac(std::string_view payload,
                const std::array<uint8_t, 32>& key,
                std::array<uint8_t, kMacBytes>& mac) {
    crypto_auth_hmacsha256_state state;
    if (crypto_auth_hmacsha256_init(&state, key.data(), key.size()) != 0) {
        return false;
    }
    crypto_auth_hmacsha256_update(
        &state, reinterpret_cast<const uint8_t*>(payload.data()), payload.size());
    crypto_auth_hmacsha256_final(&state, mac.data());
    sodium_memzero(&state, sizeof(state));
    return true;
}

} // namespace

bool PlayerAccessTokenVerifierConfig::isValid() const {
    if (!hasNonZero(signingKey) || maximumLifetimeSeconds == 0 ||
        acceptedSigningKeys.size() > 8) return false;
    for (const auto& key : acceptedSigningKeys) {
        if (!hasNonZero(key)) return false;
    }
    return true;
}

bool issuePlayerAccessToken(
    const PeerId& peerId, uint64_t issuedAtUnixSeconds,
    uint64_t expiresAtUnixSeconds, const std::array<uint8_t, 32>& signingKey,
    std::string& token) {
    token.clear();
    if (sodium_init() < 0 || !peerId.isValid() || !hasNonZero(signingKey) ||
        issuedAtUnixSeconds == 0 || expiresAtUnixSeconds <= issuedAtUnixSeconds) {
        return false;
    }
    std::array<uint8_t, kNonceBytes> nonce{};
    randombytes_buf(nonce.data(), nonce.size());
    const std::string encodedPeer = base64Url(
        reinterpret_cast<const uint8_t*>(peerId.value.data()), peerId.value.size());
    const std::string encodedNonce = base64Url(nonce.data(), nonce.size());
    std::string payload = std::string{kVersion} + '.' + encodedPeer + '.' +
        std::to_string(issuedAtUnixSeconds) + '.' +
        std::to_string(expiresAtUnixSeconds) + '.' + encodedNonce;
    std::array<uint8_t, kMacBytes> mac{};
    if (!computeMac(payload, signingKey, mac)) {
        sodium_memzero(nonce.data(), nonce.size());
        return false;
    }
    token = payload + '.' + base64Url(mac.data(), mac.size());
    sodium_memzero(nonce.data(), nonce.size());
    sodium_memzero(mac.data(), mac.size());
    if (token.size() > kMaxTokenBytes) {
        token.clear();
        return false;
    }
    return true;
}

PlayerAccessTokenVerifier::PlayerAccessTokenVerifier(
    PlayerAccessTokenVerifierConfig config)
    : _config(std::move(config)) {
    if (!_config.nowUnixSeconds) _config.nowUnixSeconds = systemNow;
    _ready = sodium_init() >= 0 && _config.isValid();
}

bool PlayerAccessTokenVerifier::isReady() const { return _ready; }

PlayerAccessTokenError PlayerAccessTokenVerifier::verify(
    std::string_view token, PeerId& peerId) const {
    peerId = {};
    if (!_ready) return PlayerAccessTokenError::InvalidConfiguration;
    if (token.empty() || token.size() > kMaxTokenBytes) {
        return PlayerAccessTokenError::Malformed;
    }
    std::array<std::string_view, 6> fields{};
    size_t start = 0;
    for (size_t index = 0; index < fields.size(); ++index) {
        const size_t end = token.find('.', start);
        if (index + 1 == fields.size()) {
            if (end != std::string_view::npos) return PlayerAccessTokenError::Malformed;
            fields[index] = token.substr(start);
        } else {
            if (end == std::string_view::npos) return PlayerAccessTokenError::Malformed;
            fields[index] = token.substr(start, end - start);
            start = end + 1;
        }
        if (fields[index].empty()) return PlayerAccessTokenError::Malformed;
    }
    if (fields[0] != kVersion) return PlayerAccessTokenError::Malformed;
    std::array<uint8_t, 63> peerBytes{};
    size_t peerSize = 0;
    std::array<uint8_t, kNonceBytes> nonce{};
    size_t nonceSize = 0;
    std::array<uint8_t, kMacBytes> suppliedMac{};
    size_t macSize = 0;
    uint64_t issuedAt = 0;
    uint64_t expiresAt = 0;
    if (!decodeBase64Url(fields[1], peerBytes.data(), peerBytes.size(), peerSize) ||
        peerSize == 0 || !parseU64(fields[2], issuedAt) ||
        !parseU64(fields[3], expiresAt) ||
        !decodeBase64Url(fields[4], nonce.data(), nonce.size(), nonceSize) ||
        nonceSize != nonce.size() ||
        !decodeBase64Url(fields[5], suppliedMac.data(), suppliedMac.size(), macSize) ||
        macSize != suppliedMac.size()) {
        return PlayerAccessTokenError::Malformed;
    }
    PeerId parsed{std::string(
        reinterpret_cast<const char*>(peerBytes.data()), peerSize)};
    if (!parsed.isValid() || issuedAt == 0 || expiresAt <= issuedAt) {
        return PlayerAccessTokenError::Malformed;
    }
    const size_t signatureOffset = token.size() - fields[5].size() - 1;
    std::array<uint8_t, kMacBytes> expectedMac{};
    bool signatureValid = false;
    const auto verifyKey = [&](const std::array<uint8_t, 32>& key) {
        if (!computeMac(token.substr(0, signatureOffset), key, expectedMac)) {
            return false;
        }
        const bool match = sodium_memcmp(
            suppliedMac.data(), expectedMac.data(), expectedMac.size()) == 0;
        signatureValid = signatureValid || match;
        return true;
    };
    if (!verifyKey(_config.signingKey)) {
        return PlayerAccessTokenError::InvalidConfiguration;
    }
    for (const auto& key : _config.acceptedSigningKeys) {
        if (!verifyKey(key)) {
            return PlayerAccessTokenError::InvalidConfiguration;
        }
    }
    sodium_memzero(expectedMac.data(), expectedMac.size());
    sodium_memzero(suppliedMac.data(), suppliedMac.size());
    sodium_memzero(nonce.data(), nonce.size());
    if (!signatureValid) return PlayerAccessTokenError::InvalidSignature;
    if (expiresAt - issuedAt > _config.maximumLifetimeSeconds) {
        return PlayerAccessTokenError::LifetimeExceeded;
    }
    const uint64_t now = _config.nowUnixSeconds();
    if (issuedAt > now && issuedAt - now > _config.clockSkewSeconds) {
        return PlayerAccessTokenError::NotYetValid;
    }
    if (now > expiresAt && now - expiresAt > _config.clockSkewSeconds) {
        return PlayerAccessTokenError::Expired;
    }
    peerId = std::move(parsed);
    return PlayerAccessTokenError::None;
}

} // namespace ayt::net
