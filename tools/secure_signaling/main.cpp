// AYNetwork_SecureSignalingServer - authenticated room-scoped rendezvous.

#include <AYNetwork/Signaling/SecureUdpSignaling.h>
#include <AYCrypto.h>

#include <atomic>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>

namespace
{

std::atomic<bool> g_running{true};
void onSignal(int) { g_running.store(false); }

struct CredentialKey {
    std::string peer;
    std::string room;
    friend bool operator==(const CredentialKey&, const CredentialKey&) = default;
};

struct CredentialKeyHash {
    size_t operator()(const CredentialKey& key) const {
        return std::hash<std::string>{}(key.peer) ^
               (std::hash<std::string>{}(key.room) << 1);
    }
};

using CredentialMap = std::unordered_map<CredentialKey,
    ayt::net::SecureSignalingCredential, CredentialKeyHash>;

bool parsePort(const char* text, uint16_t& port) {
    if (!text) return false;
    unsigned value = 0;
    const std::string input{text};
    const auto result = std::from_chars(input.data(), input.data() + input.size(), value);
    if (result.ec != std::errc{} || result.ptr != input.data() + input.size() ||
        value == 0 || value > 65535) return false;
    port = static_cast<uint16_t>(value);
    return true;
}

bool loadCredentials(const char* path, CredentialMap& credentials) {
    std::ifstream input(path);
    if (!input) return false;
    std::string line;
    size_t lineNumber = 0;
    while (std::getline(input, line)) {
        ++lineNumber;
        const size_t comment = line.find('#');
        if (comment != std::string::npos) line.resize(comment);
        std::istringstream fields(line);
        std::string peerText;
        std::string roomText;
        std::string tokenText;
        std::string expiresText;
        std::string trailingText;
        uint64_t expiresAt = 0;
        if (!(fields >> peerText)) continue;
        if (!(fields >> roomText >> tokenText)) {
            std::fprintf(stderr, "invalid credential line %zu\n", lineNumber);
            return false;
        }
        if (fields >> expiresText) {
            const auto expiryResult = std::from_chars(
                expiresText.data(), expiresText.data() + expiresText.size(), expiresAt);
            if (expiryResult.ec != std::errc{} ||
                expiryResult.ptr != expiresText.data() + expiresText.size() ||
                (fields >> trailingText)) {
                std::fprintf(stderr, "invalid credential line %zu\n", lineNumber);
                return false;
            }
        }
        const ayt::net::PeerId peer{peerText};
        const ayt::net::SignalingRoomId room{roomText};
        ayt::net::SignalingToken token;
        if (!peer.isValid() || !room.isValid() ||
            !ayt::net::parseSignalingTokenHex(tokenText, token)) {
            std::fprintf(stderr, "invalid credential line %zu\n", lineNumber);
            return false;
        }
        credentials.insert_or_assign(
            CredentialKey{peer.value, room.value},
            ayt::net::SecureSignalingCredential{token, expiresAt});
    }
    return !credentials.empty();
}

void clearCredentials(CredentialMap& credentials) {
    for (auto& [key, credential] : credentials) {
        (void)key;
        ayt::crypto::secureZero(credential.token.bytes.data(), credential.token.bytes.size());
    }
    credentials.clear();
}

} // namespace

int main(int argc, char** argv) {
    if (argc == 2 && std::string{argv[1]} == "--generate-token") {
        ayt::net::SignalingToken token;
        ayt::crypto::generateRandomBytes(token.bytes.data(), token.bytes.size());
        if (!token.isValid()) return 1;
        std::printf("%s\n", ayt::net::signalingTokenToHex(token).c_str());
        ayt::crypto::secureZero(token.bytes.data(), token.bytes.size());
        return 0;
    }
    if (argc != 4) {
        std::fprintf(stderr,
            "usage: AYNetwork_SecureSignalingServer <bind-address> <port> <credential-file>\n"
            "       AYNetwork_SecureSignalingServer --generate-token\n"
            "credential line: <peer-id> <room-id> <64-hex-token> [expires-unix-seconds]\n");
        return 2;
    }
    uint16_t port = 0;
    auto credentials = std::make_shared<CredentialMap>();
    if (!parsePort(argv[2], port) || !loadCredentials(argv[3], *credentials)) {
        std::fprintf(stderr, "invalid port or credential file\n");
        return 2;
    }

    ayt::net::SecureUdpSignalingServerConfig config;
    config.bindAddress = argv[1];
    config.port = port;
    config.resolveCredential =
        [credentials](const ayt::net::PeerId& peer,
                      const ayt::net::SignalingRoomId& room,
                      ayt::net::SecureSignalingCredential& result) {
            const auto it = credentials->find({peer.value, room.value});
            if (it == credentials->end()) return false;
            result = it->second;
            return true;
        };

    ayt::net::SecureUdpSignalingServer server(std::move(config));
    if (!server.start()) {
        std::fprintf(stderr, "failed to bind secure signaling server\n");
        return 1;
    }
    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);
    std::printf("AYNetwork secure signaling server listening on %s:%u (%zu credentials)\n",
                argv[1], server.getBoundPort(), credentials->size());
    while (g_running.load()) {
        if (server.pump() == 0) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    const auto stats = server.getStats();
    std::printf("peers=%zu authenticated=%llu forwarded=%llu auth_fail=%llu "
                "malformed_drop=%llu replay_drop=%llu room_drop=%llu rate_drop=%llu\n",
                server.getPeerCount(),
                static_cast<unsigned long long>(stats.authenticatedPackets),
                static_cast<unsigned long long>(stats.forwardedSignals),
                static_cast<unsigned long long>(stats.authenticationFailures),
                static_cast<unsigned long long>(stats.malformedDrops),
                static_cast<unsigned long long>(stats.replayDrops),
                static_cast<unsigned long long>(stats.roomMismatchDrops),
                static_cast<unsigned long long>(stats.rateLimitedDrops));
    server.stop();
    clearCredentials(*credentials);
    return 0;
}
