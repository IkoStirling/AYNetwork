// AYNetwork_SessionServer - reference HTTP authority directory + UDP signaling.

#include <AYNetwork/Session/HttpSessionService.h>
#include <AYNetwork/Session/InMemorySessionService.h>
#include <AYNetwork/Session/SqliteSessionService.h>
#include <AYNetwork/Signaling/SecureUdpSignaling.h>

#include <nlohmann/json.hpp>
#include <sodium.h>

#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstdio>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>

namespace
{

std::atomic<bool> g_running{true};
void onSignal(int) { g_running.store(false); }

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

bool enabled(const char* name) {
    const std::string value = environment(name);
    return value == "1" || value == "true" || value == "TRUE" ||
           value == "yes" || value == "YES";
}

bool parseU32(const std::string& text, uint32_t& value) {
    if (text.empty()) return false;
    const auto parsed = std::from_chars(
        text.data(), text.data() + text.size(), value);
    return parsed.ec == std::errc{} &&
           parsed.ptr == text.data() + text.size() && value != 0;
}

int hexValue(char value) {
    if (value >= '0' && value <= '9') return value - '0';
    if (value >= 'a' && value <= 'f') return value - 'a' + 10;
    if (value >= 'A' && value <= 'F') return value - 'A' + 10;
    return -1;
}

bool parseStorageKey(const std::string& text, std::array<uint8_t, 32>& key) {
    key = {};
    if (text.size() != key.size() * 2) return false;
    for (size_t i = 0; i < key.size(); ++i) {
        const int high = hexValue(text[i * 2]);
        const int low = hexValue(text[i * 2 + 1]);
        if (high < 0 || low < 0) {
            key = {};
            return false;
        }
        key[i] = static_cast<uint8_t>((high << 4) | low);
    }
    return true;
}

bool isLoopback(const std::string& address) {
    return address == "127.0.0.1" || address == "::1" ||
           address == "localhost";
}

bool constantTimeEqual(std::string_view left, std::string_view right) {
    return left.size() == right.size() && !left.empty() &&
           sodium_memcmp(left.data(), right.data(), left.size()) == 0;
}

class AuditWriter {
public:
    explicit AuditWriter(const std::string& path)
        : _output(path, std::ios::app) {}

    bool isReady() const { return static_cast<bool>(_output); }

    void write(const ayt::net::HttpP2PSessionServerConfig::AuditEvent& event) {
        std::lock_guard<std::mutex> lock(_mutex);
        _output << nlohmann::json{
            {"time", event.occurredAtUnixSeconds},
            {"method", event.method},
            {"path", event.path},
            {"remote", event.remoteAddress},
            {"status", event.status},
        }.dump() << '\n';
        _output.flush();
    }

private:
    std::mutex _mutex;
    std::ofstream _output;
};

} // namespace

int main(int argc, char** argv) {
    if (argc != 5) {
        std::fprintf(stderr,
            "usage: AYNetwork_SessionServer <bind-address> <http-port> "
            "<public-signaling-address> <signaling-udp-port>\n");
        return 2;
    }
    uint16_t httpPort = 0;
    uint16_t signalingPort = 0;
    if (!parsePort(argv[2], httpPort) || !parsePort(argv[4], signalingPort) ||
        std::string{argv[1]}.empty() || std::string{argv[3]}.empty()) {
        std::fprintf(stderr, "invalid SessionServer arguments\n");
        return 2;
    }

    const bool production = enabled("AY_SESSION_PRODUCTION");
    const std::string databasePath = environment("AY_SESSION_DB");
    const std::string keyFile = environment("AY_SESSION_TICKET_KEY_FILE");
    const std::string storageKeyText = environment("AY_SESSION_STATE_KEY");
    const std::string admissionToken = environment("AY_SESSION_ADMISSION_TOKEN");
    const std::string auditPath = environment("AY_SESSION_AUDIT_FILE");
    std::string httpBind = environment("AY_SESSION_HTTP_BIND");
    if (httpBind.empty()) httpBind = argv[1];

    if (production && (databasePath.empty() || keyFile.empty() ||
        storageKeyText.empty() || admissionToken.size() < 32 ||
        auditPath.empty())) {
        std::fprintf(stderr,
            "production mode requires AY_SESSION_DB, AY_SESSION_STATE_KEY, "
            "AY_SESSION_TICKET_KEY_FILE, AY_SESSION_ADMISSION_TOKEN (32+ chars), "
            "and AY_SESSION_AUDIT_FILE\n");
        return 2;
    }
    if (production && !isLoopback(httpBind) &&
        !enabled("AY_SESSION_ALLOW_PUBLIC_PLAINTEXT")) {
        std::fprintf(stderr,
            "production HTTP must bind loopback behind a TLS reverse proxy; "
            "set AY_SESSION_HTTP_BIND=127.0.0.1\n");
        return 2;
    }

    ayt::net::SessionTicketKeyPair ticketKeys;
    const auto keyResult = keyFile.empty()
        ? (ayt::net::generateSessionTicketKeyPair(ticketKeys)
            ? ayt::net::SessionTicketKeyFileError::None
            : ayt::net::SessionTicketKeyFileError::IoError)
        : ayt::net::loadOrCreateSessionTicketKeyPair(keyFile, ticketKeys);
    if (keyResult != ayt::net::SessionTicketKeyFileError::None) {
        std::fprintf(stderr, "failed to load or create session ticket key file\n");
        return 1;
    }

    using SignalingResolver = std::function<bool(
        const ayt::net::PeerId&, const ayt::net::SignalingRoomId&,
        ayt::net::SecureSignalingCredential&)>;
    std::shared_ptr<ayt::net::IP2PSessionService> core;
    SignalingResolver resolveSignaling;
    if (!databasePath.empty()) {
        ayt::net::SqliteP2PSessionServiceConfig coreConfig;
        coreConfig.databasePath = databasePath;
        coreConfig.publicSignalingAddress = argv[3];
        coreConfig.signalingPort = signalingPort;
        if (!parseStorageKey(storageKeyText, coreConfig.storageKey)) {
            std::fprintf(stderr,
                "AY_SESSION_STATE_KEY must contain exactly 64 hex characters\n");
            return 2;
        }
        auto durable = std::make_shared<ayt::net::SqliteP2PSessionService>(
            std::move(coreConfig), ticketKeys);
        if (!durable->isReady()) {
            std::fprintf(stderr, "failed to open session database: %s\n",
                         durable->getLastError().c_str());
            return 1;
        }
        resolveSignaling = [durable](
            const ayt::net::PeerId& peer,
            const ayt::net::SignalingRoomId& room,
            ayt::net::SecureSignalingCredential& result) {
            return durable->resolveSignalingCredential(
                peer, room.value, result.token.bytes,
                result.expiresAtUnixSeconds);
        };
        core = std::move(durable);
    } else {
        ayt::net::InMemoryP2PSessionServiceConfig coreConfig;
        coreConfig.publicSignalingAddress = argv[3];
        coreConfig.signalingPort = signalingPort;
        auto memory = std::make_shared<ayt::net::InMemoryP2PSessionService>(
            std::move(coreConfig), &ticketKeys);
        resolveSignaling = [memory](
            const ayt::net::PeerId& peer,
            const ayt::net::SignalingRoomId& room,
            ayt::net::SecureSignalingCredential& result) {
            return memory->resolveSignalingCredential(
                peer, room.value, result.token.bytes,
                result.expiresAtUnixSeconds);
        };
        core = std::move(memory);
    }

    ayt::net::SecureUdpSignalingServerConfig signalingConfig;
    signalingConfig.bindAddress = argv[1];
    signalingConfig.port = signalingPort;
    signalingConfig.resolveCredential = std::move(resolveSignaling);
    ayt::net::SecureUdpSignalingServer signaling(std::move(signalingConfig));
    if (!signaling.start()) {
        std::fprintf(stderr, "failed to bind SessionServer signaling socket\n");
        return 1;
    }

    ayt::net::HttpP2PSessionServerConfig httpConfig;
    httpConfig.bindAddress = httpBind;
    httpConfig.port = httpPort;
    if (!admissionToken.empty()) {
        httpConfig.requireAdmissionAuthentication = true;
        httpConfig.admissionAuthenticator =
            [admissionToken](std::string_view supplied,
                             const ayt::net::PeerId&) {
                return constantTimeEqual(supplied, admissionToken);
            };
    }
    if (production) {
        httpConfig.rateLimitRequestsPerMinute = 600;
        httpConfig.rateLimitBurst = 100;
    }
    const std::string rateText = environment("AY_SESSION_RATE_PER_MINUTE");
    const std::string burstText = environment("AY_SESSION_RATE_BURST");
    if (!rateText.empty() &&
        !parseU32(rateText, httpConfig.rateLimitRequestsPerMinute)) {
        std::fprintf(stderr, "invalid AY_SESSION_RATE_PER_MINUTE\n");
        return 2;
    }
    if (!burstText.empty() &&
        !parseU32(burstText, httpConfig.rateLimitBurst)) {
        std::fprintf(stderr, "invalid AY_SESSION_RATE_BURST\n");
        return 2;
    }
    if ((httpConfig.rateLimitRequestsPerMinute == 0) !=
        (httpConfig.rateLimitBurst == 0)) {
        std::fprintf(stderr,
            "AY_SESSION_RATE_PER_MINUTE and AY_SESSION_RATE_BURST must be set together\n");
        return 2;
    }
    std::shared_ptr<AuditWriter> audit;
    if (!auditPath.empty()) {
        audit = std::make_shared<AuditWriter>(auditPath);
        if (!audit->isReady()) {
            std::fprintf(stderr, "failed to open AY_SESSION_AUDIT_FILE\n");
            return 1;
        }
        httpConfig.auditSink = [audit](const auto& event) {
            audit->write(event);
        };
    }
    ayt::net::HttpP2PSessionServer http(std::move(httpConfig), core);
    if (!http.start()) {
        signaling.stop();
        std::fprintf(stderr, "failed to bind SessionServer HTTP socket\n");
        return 1;
    }

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);
    std::printf("AY_SESSION_SERVER state=ready mode=%s store=%s http=%s:%u signaling=%s:%u\n",
                production ? "production" : "development",
                databasePath.empty() ? "memory" : "sqlite",
                httpBind.c_str(), http.getBoundPort(), argv[3],
                signaling.getBoundPort());
    std::fflush(stdout);

    while (g_running.load() && http.isRunning()) {
        if (signaling.pump() == 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    http.stop();
    const auto stats = signaling.getStats();
    signaling.stop();
    std::printf("AY_SESSION_SERVER state=stopped forwarded=%llu auth_fail=%llu\n",
                static_cast<unsigned long long>(stats.forwardedSignals),
                static_cast<unsigned long long>(stats.authenticationFailures));
    return 0;
}
