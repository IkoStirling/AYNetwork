// AYNetwork_P2PSmokePeer - production-subsystem P2P/ICE quality probe.

#include <AYNetwork.h>
#include <AYNetwork/Signaling/SecureUdpSignaling.h>
#include <AYNetwork/Signaling/UdpSignaling.h>
#include <AYNetwork/Transport/GnsConnection.h>
#include <AYNetwork/RPC/RpcHandler.h>

#include <AYReflect/IReflect.h>
#include <AYReflect/detail/ReflectImpl.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <tuple>
#include <type_traits>
#include <unordered_set>
#include <vector>

using namespace ayt::net;

namespace
{

using Clock = std::chrono::steady_clock;
enum class ExpectedPath { Any, Direct, Relayed };

struct ProbeOptions {
    uint32_t durationMs = 0;
    uint32_t intervalMs = 100;
    uint32_t replyWaitMs = 2000;
    uint32_t hostTimeoutSeconds = 60;
    uint32_t expectedSessions = 1;
    uint32_t reconnects = 0;
    std::string joinTicket;
    std::string expectedJoinTicket;
    bool resumeReconnects = false;
    bool hostMigration = false;
    uint32_t migrationMembers = 3;
};

struct ProbeStats {
    uint32_t sent = 0;
    std::vector<double> rttMs;
    std::unordered_set<uint32_t> receivedSequences;
};

constexpr uint32_t kProbeMagic = 0x42505941u; // "AYPB" little-endian
constexpr size_t kProbeBytes = 17;
constexpr uint8_t kProbePing = 1;
constexpr uint8_t kProbeDone = 2;
constexpr uint32_t kProbeNetId = 0xA710u;

struct ProbeReplicatedState {
    int32_t value = -1;
};

struct ProbeRpcReceiver {};
ProbeReplicatedState* gProbeAuthorityState = nullptr;

template<typename Ret, typename... Args>
class ProbeRpcMethodInfo final : public ayt::reflect::IMethodInfo {
public:
    using Invoker = std::function<Ret(Args...)>;
    ProbeRpcMethodInfo(const char* name, ayt::reflect::RpcKind kind,
                       ayt::reflect::ITypeInfo* returnType,
                       std::vector<ayt::reflect::ITypeInfo*> parameterTypes,
                       Invoker invoker)
        : _name(name), _kind(kind), _returnType(returnType),
          _parameterTypes(std::move(parameterTypes)), _invoker(std::move(invoker)) {}

    const char* getName() const override { return _name; }
    ayt::reflect::ITypeInfo* getReturnType() const override { return _returnType; }
    size_t getParamCount() const override { return _parameterTypes.size(); }
    ayt::reflect::ITypeInfo* getParamType(size_t index) const override {
        return index < _parameterTypes.size() ? _parameterTypes[index] : nullptr;
    }
    bool getParamIsOut(size_t) const override { return false; }
    const char* getParamName(size_t index) const override { return index == 0 ? "value" : ""; }
    ayt::reflect::RpcKind getRpcKind() const override { return _kind; }
    bool isUnreliable() const override { return false; }
    bool validate(const void*) const override { return true; }
    const void* invoke(const void*, const void* const* args) const override {
        if (!_invoker) return nullptr;
        if constexpr (std::is_void_v<Ret> && sizeof...(Args) == 1) {
            using A0 = std::tuple_element_t<0, std::tuple<Args...>>;
            _invoker(*static_cast<const A0*>(args[0]));
        }
        return nullptr;
    }

private:
    const char* _name;
    ayt::reflect::RpcKind _kind;
    ayt::reflect::ITypeInfo* _returnType;
    std::vector<ayt::reflect::ITypeInfo*> _parameterTypes;
    Invoker _invoker;
};

ayt::reflect::ITypeInfo* registerProbeReflection() {
    using ayt::reflect::FieldAttribute;
    using ayt::reflect::FieldInfoImpl;
    using ayt::reflect::TypeInfoImpl;
    using ayt::reflect::TypeRegistryImpl;
    auto& registry = TypeRegistryImpl::instance();
    if (!registry.findType("AYP2PProbeState")) {
        auto* type = new TypeInfoImpl<ProbeReplicatedState>(
            "AYP2PProbeState",
            ayt::reflect::detail::defaultCreate<ProbeReplicatedState>,
            ayt::reflect::detail::defaultDestroy<ProbeReplicatedState>,
            ayt::reflect::detail::defaultCopy<ProbeReplicatedState>);
        type->addField(new FieldInfoImpl(
            "value", registry.findType<int32_t>(), offsetof(ProbeReplicatedState, value),
            FieldAttribute::Serialize | FieldAttribute::NetReplicate));
        registry.registerTypeInfo("AYP2PProbeState", type);
    }
    if (!registry.findType("AYP2PProbeRpc")) {
        auto* type = new TypeInfoImpl<ProbeRpcReceiver>(
            "AYP2PProbeRpc",
            ayt::reflect::detail::defaultCreate<ProbeRpcReceiver>,
            ayt::reflect::detail::defaultDestroy<ProbeRpcReceiver>,
            ayt::reflect::detail::defaultCopy<ProbeRpcReceiver>);
        type->addMethod(new ProbeRpcMethodInfo<void, int32_t>(
            "SetAuthorityValue", ayt::reflect::RpcKind::Server,
            registry.findType<void>(), {registry.findType<int32_t>()},
            [](int32_t value) {
                if (gProbeAuthorityState) gProbeAuthorityState->value = value;
            }));
        registry.registerTypeInfo("AYP2PProbeRpc", type);
    }
    return registry.findType("AYP2PProbeState");
}

uint64_t nowUs() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
        Clock::now().time_since_epoch()).count());
}

void putU32(uint8_t* out, uint32_t value) {
    for (size_t i = 0; i < 4; ++i) out[i] = static_cast<uint8_t>(value >> (i * 8));
}

void putU64(uint8_t* out, uint64_t value) {
    for (size_t i = 0; i < 8; ++i) out[i] = static_cast<uint8_t>(value >> (i * 8));
}

uint32_t getU32(const uint8_t* in) {
    uint32_t value = 0;
    for (size_t i = 0; i < 4; ++i) value |= static_cast<uint32_t>(in[i]) << (i * 8);
    return value;
}

uint64_t getU64(const uint8_t* in) {
    uint64_t value = 0;
    for (size_t i = 0; i < 8; ++i) value |= static_cast<uint64_t>(in[i]) << (i * 8);
    return value;
}

std::vector<uint8_t> makeProbe(uint8_t type, uint32_t sequence, uint64_t sentUs) {
    std::vector<uint8_t> wire(kProbeBytes);
    putU32(wire.data(), kProbeMagic);
    wire[4] = type;
    putU32(wire.data() + 5, sequence);
    putU64(wire.data() + 9, sentUs);
    return wire;
}

bool parseProbe(const void* data, size_t size,
                uint8_t& type, uint32_t& sequence, uint64_t& sentUs) {
    if (!data || size != kProbeBytes) return false;
    const auto* bytes = static_cast<const uint8_t*>(data);
    if (getU32(bytes) != kProbeMagic) return false;
    type = bytes[4];
    sequence = getU32(bytes + 5);
    sentUs = getU64(bytes + 9);
    return type == kProbePing || type == kProbeDone;
}

bool parseUnsigned(const char* text, uint32_t& value) {
    if (!text || !*text) return false;
    unsigned parsed = 0;
    const std::string input{text};
    const auto result = std::from_chars(input.data(), input.data() + input.size(), parsed);
    if (result.ec != std::errc{} || result.ptr != input.data() + input.size()) return false;
    value = static_cast<uint32_t>(parsed);
    return true;
}

bool parsePort(const char* text, uint16_t& value) {
    uint32_t parsed = 0;
    if (!parseUnsigned(text, parsed) || parsed > 65535) return false;
    value = static_cast<uint16_t>(parsed);
    return true;
}

bool envUnsigned(const char* name, uint32_t defaultValue,
                 uint32_t minValue, uint32_t maxValue, uint32_t& out) {
    out = defaultValue;
    const char* text = std::getenv(name);
    if (!text || !*text) return true;
    uint32_t parsed = 0;
    if (!parseUnsigned(text, parsed) || parsed < minValue || parsed > maxValue) return false;
    out = parsed;
    return true;
}

bool envFlag(const char* name, bool& value) {
    value = false;
    const char* text = std::getenv(name);
    if (!text || !*text) return true;
    if (std::strcmp(text, "1") == 0 || std::strcmp(text, "true") == 0) {
        value = true;
        return true;
    }
    return std::strcmp(text, "0") == 0 || std::strcmp(text, "false") == 0;
}

std::vector<std::string> envList(const char* name) {
    const char* raw = std::getenv(name);
    if (!raw || !*raw) return {};
    std::vector<std::string> result;
    const std::string text{raw};
    size_t begin = 0;
    while (begin <= text.size()) {
        const size_t end = text.find(',', begin);
        const std::string item = text.substr(begin,
            end == std::string::npos ? std::string::npos : end - begin);
        if (!item.empty()) result.push_back(item);
        if (end == std::string::npos) break;
        begin = end + 1;
    }
    return result;
}

bool makeP2PConfig(const PeerId& local, uint16_t virtualPort,
                   P2PConfig& config, ExpectedPath& expected) {
    config.localPeerId = local;
    config.virtualPort = virtualPort;
    config.icePolicy = P2PIcePolicy::DirectOnly;
    config.allowPrivateCandidates = true;
    config.stunServers = envList("AY_P2P_STUN");
    config.turnServers = envList("AY_P2P_TURN");
    config.turnUsers = envList("AY_P2P_TURN_USER");
    config.turnPasswords = envList("AY_P2P_TURN_PASS");

    if (const char* policy = std::getenv("AY_P2P_ICE_POLICY")) {
        if (std::strcmp(policy, "direct") == 0) config.icePolicy = P2PIcePolicy::DirectOnly;
        else if (std::strcmp(policy, "direct-or-relay") == 0) config.icePolicy = P2PIcePolicy::DirectOrRelay;
        else if (std::strcmp(policy, "relay") == 0) config.icePolicy = P2PIcePolicy::RelayOnly;
        else return false;
    }
    if (const char* allowPrivate = std::getenv("AY_P2P_ALLOW_PRIVATE")) {
        if (std::strcmp(allowPrivate, "0") == 0 || std::strcmp(allowPrivate, "false") == 0) {
            config.allowPrivateCandidates = false;
        } else if (std::strcmp(allowPrivate, "1") != 0 &&
                   std::strcmp(allowPrivate, "true") != 0) return false;
    }
    expected = ExpectedPath::Any;
    if (const char* path = std::getenv("AY_P2P_EXPECT_PATH")) {
        if (std::strcmp(path, "any") == 0) expected = ExpectedPath::Any;
        else if (std::strcmp(path, "direct") == 0) expected = ExpectedPath::Direct;
        else if (std::strcmp(path, "relayed") == 0) expected = ExpectedPath::Relayed;
        else return false;
    }
    return config.isValid();
}

bool makeProbeOptions(ProbeOptions& options) {
    uint32_t seconds = 0;
    if (const char* ticket = std::getenv("AY_P2P_JOIN_TICKET")) {
        options.joinTicket = ticket;
    }
    if (const char* expected = std::getenv("AY_P2P_EXPECT_JOIN_TICKET")) {
        options.expectedJoinTicket = expected;
    }
    if (options.joinTicket.size() > kP2PMaxJoinTicketBytes ||
        options.expectedJoinTicket.size() > kP2PMaxJoinTicketBytes) return false;
    return envFlag("AY_P2P_RESUME_RECONNECTS", options.resumeReconnects) &&
           envFlag("AY_P2P_HOST_MIGRATION", options.hostMigration) &&
           envUnsigned("AY_P2P_PROBE_SECONDS", 0, 0, 3600, seconds) &&
           envUnsigned("AY_P2P_PROBE_INTERVAL_MS", 100, 10, 60000, options.intervalMs) &&
           envUnsigned("AY_P2P_PROBE_REPLY_WAIT_MS", 2000, 100, 30000, options.replyWaitMs) &&
           envUnsigned("AY_P2P_HOST_TIMEOUT_SECONDS", 60, 5, 7200, options.hostTimeoutSeconds) &&
           envUnsigned("AY_P2P_EXPECT_SESSIONS", 1, 1, 64, options.expectedSessions) &&
           envUnsigned("AY_P2P_MIGRATION_MEMBERS", 3, 2, 64,
                       options.migrationMembers) &&
           envUnsigned("AY_P2P_RECONNECTS", 0, 0, 16, options.reconnects) &&
           ((options.durationMs = seconds * 1000u), true);
}

std::shared_ptr<ISignalingTransport> makeSignaling(const char* address, uint16_t port) {
    const char* room = std::getenv("AY_P2P_SIGNAL_ROOM");
    const char* tokenText = std::getenv("AY_P2P_SIGNAL_TOKEN");
    if ((room && *room) || (tokenText && *tokenText)) {
        if (!room || !*room || !tokenText || !*tokenText) return nullptr;
        SignalingToken token;
        if (!parseSignalingTokenHex(tokenText, token)) return nullptr;
        SecureUdpSignalingClientConfig config;
        config.serverAddress = address;
        config.serverPort = port;
        config.roomId = SignalingRoomId{room};
        config.token = token;
        return std::make_shared<SecureUdpSignalingClient>(std::move(config));
    }
    return std::make_shared<UdpSignalingClient>(
        UdpSignalingClientConfig{.serverAddress = address, .serverPort = port});
}

const char* signalingStateName(SecureSignalingState state) {
    switch (state) {
    case SecureSignalingState::Stopped: return "stopped";
    case SecureSignalingState::Registering: return "registering";
    case SecureSignalingState::Ready: return "ready";
    case SecureSignalingState::Failed: return "failed";
    }
    return "unknown";
}

const char* signalingErrorName(SecureSignalingError error) {
    switch (error) {
    case SecureSignalingError::None: return "none";
    case SecureSignalingError::InvalidConfig: return "invalid-config";
    case SecureSignalingError::SocketFailure: return "socket-failure";
    case SecureSignalingError::AuthenticationFailed: return "authentication-failed";
    case SecureSignalingError::CredentialExpired: return "credential-expired";
    case SecureSignalingError::NotRegistered: return "not-registered";
    case SecureSignalingError::RoomMismatch: return "room-mismatch";
    case SecureSignalingError::PeerUnavailable: return "peer-unavailable";
    case SecureSignalingError::RateLimited: return "rate-limited";
    case SecureSignalingError::QueueFull: return "queue-full";
    case SecureSignalingError::ProtocolError: return "protocol-error";
    }
    return "unknown";
}

void printSignalingStatus(const PeerId& local,
                          const std::shared_ptr<ISignalingTransport>& signaling,
                          const char* phase) {
    const auto* secure = dynamic_cast<const SecureUdpSignalingClient*>(signaling.get());
    if (!secure) return;
    std::fprintf(stderr,
                 "AY_P2P_SIGNAL local=%s phase=%s state=%s error=%s pending=%zu\n",
                 local.value.c_str(), phase,
                 signalingStateName(secure->getState()),
                 signalingErrorName(secure->getLastError()),
                 secure->getPendingSignalCount());
}

bool waitForSignalingReady(const PeerId& local,
                           const std::shared_ptr<ISignalingTransport>& signaling,
                           INetworkSubSystem& network) {
    auto* secure = dynamic_cast<SecureUdpSignalingClient*>(signaling.get());
    if (!secure) return true;
    const auto deadline = Clock::now() + std::chrono::seconds(15);
    while (!secure->isReady() && secure->getState() != SecureSignalingState::Failed &&
           Clock::now() < deadline) {
        network.update(0.001f);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    printSignalingStatus(local, signaling,
                         secure->isReady() ? "registered" : "registration-timeout");
    return secure->isReady();
}

const char* pathName(P2PPathKind path) {
    switch (path) {
    case P2PPathKind::Direct: return "direct";
    case P2PPathKind::Relayed: return "relayed";
    default: return "unknown";
    }
}

bool pathMatches(P2PPathKind path, ExpectedPath expected) {
    return expected == ExpectedPath::Any ||
        (expected == ExpectedPath::Direct && path == P2PPathKind::Direct) ||
        (expected == ExpectedPath::Relayed && path == P2PPathKind::Relayed);
}

void printResult(const PeerId& local, const P2PConnectionInfo& info, uint32_t session) {
    std::printf("AY_P2P_RESULT local=%s remote=%s session=%u path=%s ping_ms=%d address=%s\n",
                local.value.c_str(), info.remotePeerId.value.c_str(), session,
                pathName(info.path), info.pingMs,
                info.remoteAddress.empty() ? "-" : info.remoteAddress.c_str());
}

double percentile(const std::vector<double>& sorted, double fraction) {
    if (sorted.empty()) return -1.0;
    const size_t index = static_cast<size_t>(
        std::ceil(fraction * static_cast<double>(sorted.size())) - 1.0);
    return sorted[std::min(index, sorted.size() - 1)];
}

void printQuality(const PeerId& local, uint32_t session, const ProbeStats& stats) {
    std::vector<double> sorted = stats.rttMs;
    std::sort(sorted.begin(), sorted.end());
    double jitter = 0.0;
    for (size_t i = 1; i < stats.rttMs.size(); ++i) {
        jitter += std::abs(stats.rttMs[i] - stats.rttMs[i - 1]);
    }
    if (stats.rttMs.size() > 1) jitter /= static_cast<double>(stats.rttMs.size() - 1);
    const double loss = stats.sent == 0 ? 100.0 :
        100.0 * static_cast<double>(stats.sent - stats.receivedSequences.size()) /
        static_cast<double>(stats.sent);
    std::printf(
        "AY_P2P_QUALITY local=%s session=%u sent=%u received=%zu loss_pct=%.2f "
        "rtt_min_ms=%.2f rtt_p50_ms=%.2f rtt_p95_ms=%.2f rtt_p99_ms=%.2f "
        "rtt_max_ms=%.2f jitter_ms=%.2f\n",
        local.value.c_str(), session, stats.sent, stats.receivedSequences.size(), loss,
        sorted.empty() ? -1.0 : sorted.front(), percentile(sorted, 0.50),
        percentile(sorted, 0.95), percentile(sorted, 0.99),
        sorted.empty() ? -1.0 : sorted.back(), jitter);
}

void pump(INetworkSubSystem& network) {
    network.update(0.001f);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
}

int runHost(const PeerId& local,
            const std::shared_ptr<ISignalingTransport>& signaling,
            INetworkSubSystem& network,
            const P2PConfig& config, ExpectedPath expected,
            const ProbeOptions& options) {
    uint32_t completedSessions = 0;
    bool pathOk = true;
    bool rosterOk = true;
    ProbeReplicatedState authorityState;
    authorityState.value = 4242;
    ProbeRpcReceiver rpcReceiver;
    auto* probeType = registerProbeReflection();
    if (!probeType || !network.getRpcHandler()->registerMethod(
            "AYP2PProbeRpc", "SetAuthorityValue", &rpcReceiver)) {
        std::fprintf(stderr, "AY_P2P_FAILURE local=%s phase=engine-protocol-register\n",
                     local.value.c_str());
        return 10;
    }
    gProbeAuthorityState = &authorityState;
    struct AuthorityReset {
        ~AuthorityReset() { gProbeAuthorityState = nullptr; }
    } authorityReset;
    network.getReplicationManager()->registerObject(
        &authorityState, probeType, kProbeNetId);
    network.setP2PHostMigrationEnabled(options.hostMigration);
    (void)network.setP2PReconnectGracePeriodMs(30000);

    if (!options.expectedJoinTicket.empty()) {
        network.setP2PJoinValidator(
            [expected = options.expectedJoinTicket](
                const PeerId&, const uint8_t* ticket, size_t ticketSize) {
                if (ticketSize == 0) {
                    return P2PJoinDecision::reject(P2PJoinRejectReason::MissingTicket);
                }
                const bool matches = ticketSize == expected.size() &&
                    std::memcmp(ticket, expected.data(), ticketSize) == 0;
                return matches ? P2PJoinDecision::accept() :
                    P2PJoinDecision::reject(P2PJoinRejectReason::InvalidTicket);
            });
    }

    network.onConnectionChange([&](NetConnection* connection, bool connected,
                                    DisconnectReason reason) {
        const auto info = network.getP2PConnectionInfo(connection);
        std::printf("AY_P2P_PEER local=%s remote=%s phase=%s reason=%u\n",
                    local.value.c_str(), info.remotePeerId.value.c_str(),
                    connected ? "connected" : "disconnected",
                    static_cast<unsigned>(reason));
    });
    network.onMessage(CHANNEL_UNRELIABLE,
        [&](NetConnection* from, uint8_t channel, const void* data, size_t size) {
            uint8_t type = 0;
            uint32_t sequence = 0;
            uint64_t sentUs = 0;
            if (parseProbe(data, size, type, sequence, sentUs) && type == kProbePing) {
                network.sendTo(from, channel, data, size);
            }
        });
    network.onMessage(CHANNEL_RELIABLE,
        [&](NetConnection* from, uint8_t channel, const void* data, size_t size) {
            uint8_t type = 0;
            uint32_t sequence = 0;
            uint64_t sentUs = 0;
            if (!parseProbe(data, size, type, sequence, sentUs)) return;
            if (type == kProbePing) {
                network.sendTo(from, channel, data, size);
                return;
            }
            ++completedSessions;
            const auto info = network.getP2PConnectionInfo(from);
            const auto peers = network.getP2PPeers();
            const auto peerIt = std::find_if(
                peers.begin(), peers.end(), [&](const P2PPeerInfo& peer) {
                    return peer.peerId == info.remotePeerId &&
                           peer.connectionId == from->getId();
                });
            const auto sessionInfo = network.getP2PSessionInfo();
            const auto barrier = network.getP2PReadyBarrierInfo();
            const bool sessionViewOk =
                network.findP2PPeer(info.remotePeerId) == from &&
                peerIt != peers.end() && peerIt->state == P2PPeerState::Ready &&
                peerIt->admitted &&
                sessionInfo.role == P2PSessionRole::Host &&
                sessionInfo.state == P2PSessionState::Active &&
                sessionInfo.hostPeerId == local && sessionInfo.readyPeerCount >= 1 &&
                barrier.open && barrier.localReady &&
                barrier.readyMemberCount == barrier.totalMemberCount &&
                barrier.totalMemberCount >= 2;
            rosterOk = rosterOk && sessionViewOk;
            std::printf(
                "AY_P2P_SESSION_VIEW local=%s remote=%s session=%u role=host "
                "roster=%s ready_peers=%zu connection_id=%u\n",
                local.value.c_str(), info.remotePeerId.value.c_str(), completedSessions,
                sessionViewOk ? "ok" : "invalid", sessionInfo.readyPeerCount,
                from->getId());
            std::printf(
                "AY_P2P_ADMISSION local=%s remote=%s session=%u state=admitted ticket=accepted\n",
                local.value.c_str(), info.remotePeerId.value.c_str(), completedSessions);
            std::printf(
                "AY_P2P_BARRIER local=%s session=%u revision=%u ready=%zu total=%zu open=%s\n",
                local.value.c_str(), completedSessions, barrier.revision,
                barrier.readyMemberCount, barrier.totalMemberCount,
                barrier.open ? "true" : "false");
            printResult(local, info, completedSessions);
            pathOk = pathOk && pathMatches(info.path, expected);
            std::printf("AY_P2P_SESSION local=%s remote=%s phase=complete index=%u\n",
                        local.value.c_str(), info.remotePeerId.value.c_str(), completedSessions);
            // The probe's reliable Done frame is an application-level close
            // acknowledgement. End this completed session from the Host side
            // so resource reclamation does not depend on an ICE timeout.
            if (!options.resumeReconnects ||
                completedSessions >= options.expectedSessions) {
                network.kickConnection(from, "P2P probe session complete");
            }
        });

    if (!network.listenP2P()) {
        printSignalingStatus(local, signaling, "listen-route-failed");
        return 9;
    }
    if (!network.setP2PLocalReady(true)) {
        std::fprintf(stderr, "AY_P2P_FAILURE local=%s phase=host-ready\n",
                     local.value.c_str());
        return 14;
    }
    std::printf("AY_P2P_HOST_READY local=%s vport=%u protocol=%u expected_sessions=%u\n",
                local.value.c_str(), config.virtualPort,
                network.getProtocolVersion(), options.expectedSessions);
    std::fflush(stdout);

    if (options.hostMigration) {
        const auto migrationDeadline =
            Clock::now() + std::chrono::seconds(options.hostTimeoutSeconds);
        while (Clock::now() < migrationDeadline) {
            const auto barrier = network.getP2PReadyBarrierInfo();
            const auto members = network.getP2PSessionMembers();
            if (barrier.open &&
                barrier.totalMemberCount == options.migrationMembers &&
                members.size() == options.migrationMembers) break;
            pump(network);
        }
        const auto before = network.getP2PSessionInfo();
        const auto barrier = network.getP2PReadyBarrierInfo();
        if (!barrier.open || barrier.totalMemberCount != options.migrationMembers ||
            !network.requestP2PHostMigration()) {
            std::fprintf(stderr,
                         "AY_P2P_FAILURE local=%s phase=migration-handoff "
                         "ready=%zu total=%zu\n",
                         local.value.c_str(), barrier.readyMemberCount,
                         barrier.totalMemberCount);
            return 16;
        }
        std::printf(
            "AY_P2P_MIGRATION local=%s phase=handoff session_id=%llu epoch=%u members=%zu\n",
            local.value.c_str(),
            static_cast<unsigned long long>(before.sessionId), before.epoch,
            barrier.totalMemberCount);
        std::fflush(stdout);
        // Keep servicing the reliable signaling/control path long enough for
        // every client to receive the migration plan before the old Host
        // performs its scheduled departure.
        const auto handoffLinger = Clock::now() + std::chrono::milliseconds(300);
        while (Clock::now() < handoffLinger) pump(network);
        return 0;
    }

    const auto deadline = Clock::now() + std::chrono::seconds(options.hostTimeoutSeconds);
    while (completedSessions < options.expectedSessions && Clock::now() < deadline) pump(network);
    if (completedSessions < options.expectedSessions) {
        printSignalingStatus(local, signaling, "host-session-timeout");
        return 3;
    }
    if (!rosterOk) return 12;
    return pathOk ? 0 : 7;
}

bool runJoinSession(const PeerId& local, const PeerId& remote,
                    const std::shared_ptr<ISignalingTransport>& signaling,
                    INetworkSubSystem& network, ExpectedPath expected,
                    const ProbeOptions& options, uint32_t session,
                    ProbeReplicatedState& replicatedState, int& failureCode,
                    bool connectionAlreadyStarted) {
    ProbeStats stats;
    bool disconnected = false;
    DisconnectReason disconnectReason = DisconnectReason::Unknown;
    network.onConnectionChange([&](NetConnection*, bool connected, DisconnectReason reason) {
        if (!connected && reason != DisconnectReason::Unknown) {
            disconnected = true;
            disconnectReason = reason;
        }
    });
    auto receiveProbe = [&](NetConnection*, uint8_t, const void* data, size_t size) {
        uint8_t type = 0;
        uint32_t sequence = 0;
        uint64_t sentUs = 0;
        if (!parseProbe(data, size, type, sequence, sentUs) || type != kProbePing) return;
        if (stats.receivedSequences.insert(sequence).second) {
            stats.rttMs.push_back(static_cast<double>(nowUs() - sentUs) / 1000.0);
        }
    };
    network.onMessage(CHANNEL_UNRELIABLE, receiveProbe);
    network.onMessage(CHANNEL_RELIABLE, receiveProbe);

    if (!connectionAlreadyStarted &&
        !network.setP2PJoinTicket(options.joinTicket.data(),
                                  options.joinTicket.size())) {
        std::fprintf(stderr,
                     "AY_P2P_FAILURE local=%s remote=%s phase=join-ticket\n",
                     local.value.c_str(), remote.value.c_str());
        failureCode = 13;
        return false;
    }

    if (!connectionAlreadyStarted && !network.connectP2P(remote)) {
        printSignalingStatus(local, signaling, "connect-start-failed");
        failureCode = 4;
        return false;
    }
    if (!network.setP2PLocalReady(true)) {
        std::fprintf(stderr,
                     "AY_P2P_FAILURE local=%s remote=%s phase=client-ready\n",
                     local.value.c_str(), remote.value.c_str());
        failureCode = 14;
        return false;
    }
    const auto connectDeadline = Clock::now() + std::chrono::seconds(30);
    while (!network.isConnected() && !disconnected && Clock::now() < connectDeadline) pump(network);
    if (!network.isConnected()) {
        const auto rejectedSession = network.getP2PSessionInfo();
        if (disconnectReason == DisconnectReason::AdmissionRejected ||
            rejectedSession.admission == P2PAdmissionState::Rejected) {
            std::fprintf(stderr,
                         "AY_P2P_FAILURE local=%s remote=%s phase=admission-rejected "
                         "reason=%u\n",
                         local.value.c_str(), remote.value.c_str(),
                         static_cast<unsigned>(rejectedSession.rejectionReason));
            failureCode = 13;
            return false;
        }
        std::fprintf(stderr,
                     "AY_P2P_FAILURE local=%s remote=%s phase=ice-connect reason=%u\n",
                     local.value.c_str(), remote.value.c_str(),
                     static_cast<unsigned>(disconnectReason));
        printSignalingStatus(local, signaling, "connect-timeout");
        failureCode = 5;
        return false;
    }
    std::printf("AY_P2P_SESSION local=%s remote=%s phase=connected index=%u\n",
                local.value.c_str(), remote.value.c_str(), session);

    const auto barrierDeadline = Clock::now() + std::chrono::seconds(10);
    while (!network.getP2PReadyBarrierInfo().open && network.isConnected() &&
           Clock::now() < barrierDeadline) pump(network);
    const auto barrier = network.getP2PReadyBarrierInfo();
    if (!barrier.open || !barrier.localReady ||
        barrier.readyMemberCount != barrier.totalMemberCount ||
        barrier.totalMemberCount < 2) {
        std::fprintf(stderr,
                     "AY_P2P_FAILURE local=%s remote=%s phase=ready-barrier "
                     "revision=%u ready=%zu total=%zu open=%u\n",
                     local.value.c_str(), remote.value.c_str(), barrier.revision,
                     barrier.readyMemberCount, barrier.totalMemberCount,
                     barrier.open ? 1u : 0u);
        failureCode = 14;
        return false;
    }
    std::printf(
        "AY_P2P_ADMISSION local=%s remote=%s session=%u state=admitted ticket=%s\n",
        local.value.c_str(), remote.value.c_str(), session,
        options.joinTicket.empty() ? "empty" : "accepted");
    std::printf(
        "AY_P2P_BARRIER local=%s session=%u revision=%u ready=%zu total=%zu open=true\n",
        local.value.c_str(), session, barrier.revision,
        barrier.readyMemberCount, barrier.totalMemberCount);

    const auto sessionInfo = network.getP2PSessionInfo();
    const auto peers = network.getP2PPeers();
    const auto peerIt = std::find_if(
        peers.begin(), peers.end(), [&](const P2PPeerInfo& peer) {
            return peer.peerId == remote && peer.state == P2PPeerState::Ready;
        });
    const bool sessionViewOk =
        sessionInfo.role == P2PSessionRole::Client &&
        sessionInfo.state == P2PSessionState::Active &&
        sessionInfo.hostPeerId == remote && sessionInfo.readyPeerCount == 1 &&
        sessionInfo.admission == P2PAdmissionState::Admitted &&
        peerIt != peers.end() && peerIt->isSessionHost && peerIt->admitted &&
        network.findP2PPeer(remote) == network.getConnection();
    std::printf(
        "AY_P2P_SESSION_VIEW local=%s remote=%s session=%u role=client "
        "roster=%s ready_peers=%zu connection_id=%u\n",
        local.value.c_str(), remote.value.c_str(), session,
        sessionViewOk ? "ok" : "invalid", sessionInfo.readyPeerCount,
        peerIt == peers.end() ? 0u : peerIt->connectionId);
    if (!sessionViewOk) {
        std::fprintf(stderr,
                     "AY_P2P_FAILURE local=%s remote=%s phase=session-roster\n",
                     local.value.c_str(), remote.value.c_str());
        failureCode = 12;
        return false;
    }

    const int32_t authorityValue = static_cast<int32_t>(9000 + session);
    const void* rpcArgs[1] = {&authorityValue};
    uint64_t callId = 0;
    if (!network.getRpcHandler()->callServer(
            "AYP2PProbeRpc", "SetAuthorityValue", rpcArgs, nullptr, 1, callId)) {
        std::fprintf(stderr,
                     "AY_P2P_FAILURE local=%s remote=%s phase=rpc-send\n",
                     local.value.c_str(), remote.value.c_str());
        failureCode = 10;
        return false;
    }
    const auto engineDeadline = Clock::now() + std::chrono::seconds(10);
    while (network.isConnected() && replicatedState.value != authorityValue &&
           Clock::now() < engineDeadline) pump(network);
    if (replicatedState.value != authorityValue) {
        std::fprintf(stderr,
                     "AY_P2P_FAILURE local=%s remote=%s phase=rpc-replication "
                     "expected=%d actual=%d\n",
                     local.value.c_str(), remote.value.c_str(),
                     authorityValue, replicatedState.value);
        failureCode = 11;
        return false;
    }
    std::printf(
        "AY_P2P_ENGINE local=%s remote=%s session=%u handshake=ok rpc=ok replication=ok value=%d\n",
        local.value.c_str(), remote.value.c_str(), session, replicatedState.value);

    uint32_t sequence = 1;
    if (options.durationMs == 0) {
        const auto wire = makeProbe(kProbePing, sequence, nowUs());
        network.send(CHANNEL_RELIABLE, wire.data(), wire.size());
        ++stats.sent;
        const auto deadline = Clock::now() + std::chrono::seconds(10);
        while (stats.receivedSequences.empty() && network.isConnected() && Clock::now() < deadline) {
            pump(network);
        }
    } else {
        const auto end = Clock::now() + std::chrono::milliseconds(options.durationMs);
        auto nextSend = Clock::now();
        while (network.isConnected() && Clock::now() < end) {
            const auto current = Clock::now();
            if (current >= nextSend) {
                const auto wire = makeProbe(kProbePing, sequence++, nowUs());
                network.send(CHANNEL_UNRELIABLE, wire.data(), wire.size());
                ++stats.sent;
                nextSend = current + std::chrono::milliseconds(options.intervalMs);
            }
            pump(network);
        }
        const auto replyDeadline = Clock::now() + std::chrono::milliseconds(options.replyWaitMs);
        while (network.isConnected() && stats.receivedSequences.size() < stats.sent &&
               Clock::now() < replyDeadline) pump(network);
    }

    const auto info = network.getP2PConnectionInfo();
    printQuality(local, session, stats);
    if (stats.receivedSequences.empty()) {
        std::fprintf(stderr,
                     "AY_P2P_FAILURE local=%s remote=%s phase=probe-echo reason=no-replies\n",
                     local.value.c_str(), remote.value.c_str());
        failureCode = 6;
        return false;
    }
    printResult(local, info, session);
    if (!pathMatches(info.path, expected)) {
        failureCode = 7;
        return false;
    }

    const auto done = makeProbe(kProbeDone, sequence, nowUs());
    network.send(CHANNEL_RELIABLE, done.data(), done.size());
    const auto linger = Clock::now() + std::chrono::milliseconds(300);
    while (Clock::now() < linger) pump(network);
    return true;
}

int runMigrationJoin(const PeerId& local, const PeerId& remote,
                     const std::shared_ptr<ISignalingTransport>& signaling,
                     INetworkSubSystem& network, const ProbeOptions& options) {
    struct SessionEventCounts {
        uint32_t migrationStarted = 0;
        uint32_t authorityChanged = 0;
        uint32_t seatRestored = 0;
        uint32_t migrationFailed = 0;
    } events;
    const uint64_t eventListenerId = network.addP2PSessionEventListener(
        [&events](const P2PSessionEvent& event) {
            switch (event.type) {
            case P2PSessionEventType::MigrationStarted:
                ++events.migrationStarted;
                break;
            case P2PSessionEventType::AuthorityChanged:
                ++events.authorityChanged;
                break;
            case P2PSessionEventType::SeatRestored:
                ++events.seatRestored;
                break;
            case P2PSessionEventType::MigrationFailed:
                ++events.migrationFailed;
                break;
            case P2PSessionEventType::SeatReserved:
            case P2PSessionEventType::SeatReservationExpired:
                break;
            }
        });
    if (eventListenerId == 0) return 16;
    struct SessionEventListenerReset {
        INetworkSubSystem& network;
        uint64_t id;
        ~SessionEventListenerReset() {
            (void)network.removeP2PSessionEventListener(id);
        }
    } eventListenerReset{network, eventListenerId};

    ProbeReplicatedState state;
    ProbeRpcReceiver rpcReceiver;
    auto* probeType = registerProbeReflection();
    if (!probeType || !network.getRpcHandler()->registerMethod(
            "AYP2PProbeRpc", "SetAuthorityValue", &rpcReceiver)) return 10;
    network.getReplicationManager()->registerObject(&state, probeType, kProbeNetId);
    gProbeAuthorityState = &state;
    struct AuthorityReset {
        ~AuthorityReset() { gProbeAuthorityState = nullptr; }
    } authorityReset;

    network.setP2PHostMigrationEnabled(true);
    (void)network.setP2PReconnectGracePeriodMs(30000);
    if (!network.setP2PJoinTicket(options.joinTicket.data(),
                                  options.joinTicket.size()) ||
        !network.connectP2P(remote) || !network.setP2PLocalReady(true)) {
        return 16;
    }
    P2PSessionInfo before;
    bool capturedInitialSession = false;
    const auto joinDeadline = Clock::now() + std::chrono::seconds(30);
    while (Clock::now() < joinDeadline) {
        const auto info = network.getP2PSessionInfo();
        const auto members = network.getP2PSessionMembers();
        if (info.sessionId != 0 && info.localSeatId != 0 &&
            (members.size() == options.migrationMembers || info.epoch > 1)) {
            before = info;
            // A fast graceful handoff can be consumed by the same update that
            // completed the initial join. Reconstruct the immediately prior
            // epoch so the remainder of the probe still validates convergence
            // instead of failing an observation race.
            if (info.epoch > 1) {
                before.epoch = info.epoch - 1;
                before.hostPeerId = info.previousHostPeerId;
            }
            capturedInitialSession = true;
            break;
        }
        pump(network);
    }
    if (!capturedInitialSession) {
        printSignalingStatus(local, signaling, "migration-initial-session");
        return 16;
    }
    std::printf(
        "AY_P2P_MIGRATION local=%s phase=armed session_id=%llu epoch=%u seat=%u\n",
        local.value.c_str(),
        static_cast<unsigned long long>(before.sessionId), before.epoch,
        before.localSeatId);
    std::fflush(stdout);

    const auto migrationDeadline = Clock::now() + std::chrono::seconds(45);
    while (Clock::now() < migrationDeadline) {
        const auto info = network.getP2PSessionInfo();
        const auto barrier = network.getP2PReadyBarrierInfo();
        if (info.epoch > before.epoch &&
            info.migration == P2PHostMigrationState::Stable &&
            barrier.open && barrier.totalMemberCount == options.migrationMembers - 1 &&
            (info.role == P2PSessionRole::Host || network.isConnected())) break;
        pump(network);
    }
    const auto after = network.getP2PSessionInfo();
    const auto migratedBarrier = network.getP2PReadyBarrierInfo();
    if (after.epoch != before.epoch + 1 ||
        after.localSeatId != before.localSeatId ||
        after.migration != P2PHostMigrationState::Stable ||
        !migratedBarrier.open ||
        migratedBarrier.totalMemberCount != options.migrationMembers - 1 ||
        events.migrationStarted == 0 || events.authorityChanged != 1 ||
        events.migrationFailed != 0 ||
        (after.role == P2PSessionRole::Client && events.seatRestored == 0)) {
        std::fprintf(stderr,
                     "AY_P2P_FAILURE local=%s phase=migration-converge "
                     "old_epoch=%u new_epoch=%u seat=%u role=%u state=%u "
                     "ready=%zu total=%zu open=%u events=%u/%u/%u/%u\n",
                     local.value.c_str(), before.epoch, after.epoch,
                     after.localSeatId, static_cast<unsigned>(after.role),
                     static_cast<unsigned>(after.migration),
                     migratedBarrier.readyMemberCount,
                     migratedBarrier.totalMemberCount,
                     migratedBarrier.open ? 1u : 0u,
                     events.migrationStarted, events.authorityChanged,
                     events.seatRestored, events.migrationFailed);
        return 16;
    }

    constexpr int32_t kMigratedAuthorityValue = 17777;
    if (after.role == P2PSessionRole::Client) {
        const void* args[1] = {&kMigratedAuthorityValue};
        uint64_t callId = 0;
        if (!network.getRpcHandler()->callServer(
                "AYP2PProbeRpc", "SetAuthorityValue", args, nullptr, 1, callId)) {
            return 17;
        }
    }
    const auto authorityDeadline = Clock::now() + std::chrono::seconds(10);
    while (state.value != kMigratedAuthorityValue &&
           Clock::now() < authorityDeadline) pump(network);
    if (state.value != kMigratedAuthorityValue) {
        std::fprintf(stderr,
                     "AY_P2P_FAILURE local=%s phase=migration-authority value=%d\n",
                     local.value.c_str(), state.value);
        return 17;
    }
    if (after.role == P2PSessionRole::Host) {
        // The RPC has reached the new authority. Keep the new Host alive for
        // several replication ticks so the surviving client can observe the
        // authoritative value before this finite probe exits.
        const auto replicationLinger = Clock::now() + std::chrono::seconds(1);
        while (Clock::now() < replicationLinger) pump(network);
    }
    std::printf(
        "AY_P2P_MIGRATION local=%s phase=complete role=%s old_host=%s new_host=%s "
        "epoch=%u seat=%u barrier=%zu/%zu authority=%d events=%u/%u/%u/%u\n",
        local.value.c_str(),
        after.role == P2PSessionRole::Host ? "host" : "client",
        before.hostPeerId.value.c_str(), after.hostPeerId.value.c_str(),
        after.epoch, after.localSeatId, migratedBarrier.readyMemberCount,
        migratedBarrier.totalMemberCount, state.value,
        events.migrationStarted, events.authorityChanged,
        events.seatRestored, events.migrationFailed);
    return 0;
}

int runJoin(const PeerId& local, const PeerId& remote,
            const std::shared_ptr<ISignalingTransport>& signaling,
            INetworkSubSystem& network, ExpectedPath expected,
            const ProbeOptions& options) {
    if (options.hostMigration) {
        return runMigrationJoin(local, remote, signaling, network, options);
    }
    ProbeReplicatedState replicatedState;
    auto* probeType = registerProbeReflection();
    if (!probeType) return 10;
    network.getReplicationManager()->registerObject(
        &replicatedState, probeType, kProbeNetId);
    network.setP2PHostMigrationEnabled(options.hostMigration);
    (void)network.setP2PReconnectGracePeriodMs(30000);
    uint32_t retainedSeatId = 0;
    bool connectionAlreadyStarted = false;
    for (uint32_t session = 1; session <= options.reconnects + 1; ++session) {
        int failureCode = 0;
        if (!runJoinSession(local, remote, signaling, network,
                            expected, options, session, replicatedState, failureCode,
                            connectionAlreadyStarted)) {
            network.disconnect();
            return failureCode;
        }
        const auto sessionInfo = network.getP2PSessionInfo();
        if (retainedSeatId == 0) retainedSeatId = sessionInfo.localSeatId;
        if (options.resumeReconnects && sessionInfo.localSeatId != retainedSeatId) {
            std::fprintf(stderr,
                         "AY_P2P_FAILURE local=%s phase=seat-changed old=%u new=%u\n",
                         local.value.c_str(), retainedSeatId, sessionInfo.localSeatId);
            network.disconnect();
            return 15;
        }
        if (session <= options.reconnects) {
            if (options.resumeReconnects) {
                if (!network.reconnectP2P()) {
                    std::fprintf(stderr,
                                 "AY_P2P_FAILURE local=%s phase=resume-start\n",
                                 local.value.c_str());
                    network.disconnect();
                    return 15;
                }
                connectionAlreadyStarted = true;
                std::printf(
                    "AY_P2P_RESUME local=%s remote=%s seat=%u next_session=%u\n",
                    local.value.c_str(), remote.value.c_str(), retainedSeatId,
                    session + 1);
            } else {
                network.disconnect();
                connectionAlreadyStarted = false;
            }
            std::printf("AY_P2P_RECONNECT local=%s remote=%s next_session=%u\n",
                        local.value.c_str(), remote.value.c_str(), session + 1);
            const auto pause = Clock::now() + std::chrono::milliseconds(250);
            while (Clock::now() < pause) pump(network);
        }
    }
    network.disconnect();
    return 0;
}

} // namespace

int main(int argc, char** argv) {
    if (argc != 6 && argc != 7) {
        std::fprintf(stderr,
            "host: AYNetwork_P2PSmokePeer host <local-peer> <signal-ip> <signal-port> <virtual-port>\n"
            "join: AYNetwork_P2PSmokePeer join <local-peer> <remote-peer> <signal-ip> <signal-port> <virtual-port>\n"
            "env: AY_P2P_SIGNAL_ROOM/TOKEN, AY_P2P_ICE_POLICY, AY_P2P_STUN, AY_P2P_TURN,\n"
            "     AY_P2P_TURN_USER/PASS, AY_P2P_ALLOW_PRIVATE, AY_P2P_EXPECT_PATH,\n"
            "     AY_P2P_PROBE_SECONDS/INTERVAL_MS/REPLY_WAIT_MS, AY_P2P_RECONNECTS,\n"
            "     AY_P2P_EXPECT_SESSIONS, AY_P2P_HOST_TIMEOUT_SECONDS,\n"
            "     AY_P2P_JOIN_TICKET (join), AY_P2P_EXPECT_JOIN_TICKET (host),\n"
            "     AY_P2P_RESUME_RECONNECTS, AY_P2P_HOST_MIGRATION,\n"
            "     AY_P2P_MIGRATION_MEMBERS\n");
        return 2;
    }
    const bool host = std::strcmp(argv[1], "host") == 0;
    const bool join = std::strcmp(argv[1], "join") == 0;
    if ((!host && !join) || (host && argc != 6) || (join && argc != 7)) return 2;
    const PeerId local{argv[2]};
    const PeerId remote{join ? argv[3] : ""};
    const char* signalAddress = argv[join ? 4 : 3];
    uint16_t signalPort = 0;
    uint16_t virtualPort = 0;
    if (!local.isValid() || (join && !remote.isValid()) ||
        !parsePort(argv[join ? 5 : 4], signalPort) || signalPort == 0 ||
        !parsePort(argv[join ? 6 : 5], virtualPort)) return 2;

    P2PConfig config;
    ExpectedPath expected = ExpectedPath::Any;
    ProbeOptions options;
    auto signaling = makeSignaling(signalAddress, signalPort);
    std::string prepareError;
    if (!signaling || !makeP2PConfig(local, virtualPort, config, expected) ||
        !makeProbeOptions(options) ||
        !GnsConnection::prepareP2PConfig(config, &prepareError)) {
        if (!prepareError.empty()) {
            std::fprintf(stderr, "AY_P2P_FAILURE local=%s phase=stun-resolve error=%s\n",
                         local.value.c_str(), prepareError.c_str());
        }
        return 1;
    }
    for (const auto& stun : config.stunServers) {
        std::printf("AY_P2P_STUN_READY local=%s endpoint=%s\n",
                    local.value.c_str(), stun.c_str());
    }

    registerNetworkSubSystem();
    INetworkSubSystem* network = findRegisteredNetworkSubSystem();
    if (!network || !network->initialize() || !network->configureP2P(config, signaling)) {
        if (network) network->shutdown();
        return 1;
    }
    if (!waitForSignalingReady(local, signaling, *network)) {
        network->shutdown();
        return 8;
    }
    const int result = host
        ? runHost(local, signaling, *network, config, expected, options)
        : runJoin(local, remote, signaling, *network, expected, options);
    network->disconnect();
    network->shutdown();
    return result;
}
