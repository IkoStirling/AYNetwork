// GnsConnection.cpp - GameNetworkingSockets wrapper implementation.
//
// R1 (2026-07-26): replaces the stub KcpConnection. Implements a single
// client OR server endpoint backed by ISteamNetworkingSockets in non-Steam
// ("libv12") mode (no Steam client required, runs standalone).
//
// Lifecycle:
//   1. gns::init() — call once per process from NetworkSubSystem::initialize().
//      Ref-counted. Fetches sockets + utils interfaces and registers the
//      global status-callback.
//   2. GnsConnection instance — initClient(addr, port) or initServer(port).
//   3. Per frame: GnsConnection::pump() runs callbacks once and drains the
//      shared poll group within the configured message/byte budget.
//   4. instance.disconnect(reason) — graceful close.
//   5. gns::shutdown() — call from NetworkSubSystem::shutdown().

#include <AYNetwork/Transport/GnsConnection.h>
#include <AYNetwork/Transport/UdpSocket.h>
#include <AYNetwork/INetwork.h>                  // R1 done: HandshakeMsgType / DisconnectReason / kProtocolVersion
#include <AYNetwork/Protocol/PacketCodec.h>                  // R2: framing layer
#include <AYNetwork/Profiler/ProfilerMsgType.h> // R5.5 (2026-08-25): handshake extras-key helper
#include "TransportFaultController.h"      // R5.4 (2026-08-25)
#include "TransportFaultInterceptor.h"     // R5.4

#include <steam/steamclientpublic.h>     // EResult
#include <steam/steamnetworkingtypes.h>  // identity, connection info, send flags
#include <steam/isteamnetworkingutils.h> // SetGlobalCallback_SteamNetConnectionStatusChanged
#include <steam/isteamnetworkingsockets.h>
#include <steam/steamnetworkingsockets.h> // GameNetworkingSockets_Init / _Kill
#include <steam/steamnetworkingcustomsignaling.h>

#include <atomic>
#include <algorithm>
#include <charconv>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace ayt::net
{

// =============================================================================
// Static GNS state (process-wide) — public so the gns:: namespace helpers below
// can manipulate them. Encapsulated by being in this .cpp only.
// =============================================================================
ISteamNetworkingSockets* GnsConnection::s_gns       = nullptr;
HSteamNetPollGroup       GnsConnection::s_pollGroup = k_HSteamNetPollGroup_Invalid;

// R6 (2026-08-25): clock seam. When non-null, nowMs() returns the override
// (downshifted to uint32 ms) instead of consulting ayt::performanceNowUs.
// Default null = production wall clock.
GnsConnection::NowOverrideFn GnsConnection::s_nowOverride;

// Active connection map (HSteamNetConnection -> GnsConnection*).
static std::unordered_map<HSteamNetConnection, GnsConnection*>& connMap() {
    static std::unordered_map<HSteamNetConnection, GnsConnection*> m;
    return m;
}

static std::unordered_map<HSteamListenSocket, GnsConnection::AdoptFactory>&
listenerFactories() {
    static std::unordered_map<HSteamListenSocket, GnsConnection::AdoptFactory> factories;
    return factories;
}

static std::unordered_map<HSteamListenSocket, GnsConnection*>& listenerOwners() {
    static std::unordered_map<HSteamListenSocket, GnsConnection*> owners;
    return owners;
}

struct P2PListenerRoute {
    P2PConfig config;
    std::shared_ptr<ISignalingTransport> signaling;
    GnsConnection::P2PAdoptFactory factory;
};

static std::unordered_map<uint16_t, P2PListenerRoute>& p2pListenerRoutes() {
    static std::unordered_map<uint16_t, P2PListenerRoute> routes;
    return routes;
}

static std::recursive_mutex registryMutex;
static std::mutex pumpMutex;
static thread_local bool insidePump = false;

// GNS reserves 1000..1999 for normal application disconnects. Keep AYNetwork
// in a small private sub-range so the remote endpoint can recover the stable
// DisconnectReason enum instead of collapsing every graceful close into
// ConnectionLost.
constexpr int kAyDisconnectReasonBase = k_ESteamNetConnectionEnd_App_Min + 100;

static int encodeDisconnectReason(DisconnectReason reason) {
    const int value = static_cast<int>(reason);
    if (value <= static_cast<int>(DisconnectReason::Unknown) ||
        value > static_cast<int>(DisconnectReason::AdmissionRejected)) {
        return k_ESteamNetConnectionEnd_App_Generic;
    }
    return kAyDisconnectReasonBase + value;
}

static DisconnectReason decodeDisconnectReason(int reason) {
    const int value = reason - kAyDisconnectReasonBase;
    if (value <= static_cast<int>(DisconnectReason::Unknown) ||
        value > static_cast<int>(DisconnectReason::AdmissionRejected)) {
        return DisconnectReason::Unknown;
    }
    return static_cast<DisconnectReason>(value);
}

static void gns_status_callback(SteamNetConnectionStatusChangedCallback_t* info) {
    if (!info) return;

    if (info->m_info.m_eState == k_ESteamNetworkingConnectionState_Connecting) {
        bool needsAdopt = false;
        {
            std::lock_guard<std::recursive_mutex> lk(registryMutex);
            needsAdopt = (connMap().find(info->m_hConn) == connMap().end());
        }
        if (needsAdopt) {
            GnsConnection::AdoptFactory factory;
            GnsConnection* fallbackOwner = nullptr;
            {
                std::lock_guard<std::recursive_mutex> lk(registryMutex);
                auto factoryIt = listenerFactories().find(info->m_info.m_hListenSocket);
                if (factoryIt != listenerFactories().end()) factory = factoryIt->second;
                auto ownerIt = listenerOwners().find(info->m_info.m_hListenSocket);
                if (ownerIt != listenerOwners().end()) fallbackOwner = ownerIt->second;
            }

            // Route by the exact listener.  This avoids the old process-wide
            // last-writer-wins factory and permits independent listeners.
            if (factory) {
                GnsConnection* child = factory(info->m_hConn);
                (void)child;  // result stored by factory's adoptIncomingConnection
                return;
            }

            // Direct GnsConnection tools use the listener owner itself as a
            // single accepted peer.  Subsystem mode always installs a factory.
            if (GnsConnection::s_gns && fallbackOwner) {
                EResult r = GnsConnection::s_gns->AcceptConnection(info->m_hConn);
                if (r != k_EResultOK) {
                    ::fprintf(stderr, "[GnsConnection] AcceptConnection failed: %d\n", r);
                    return;
                }
                fallbackOwner->adoptIncomingConnection(info->m_hConn);
                return;
            }

            if (GnsConnection::s_gns) {
                GnsConnection::s_gns->CloseConnection(
                    info->m_hConn, 0, "no listener owner", false);
            }
            ::fprintf(stderr, "[GnsConnection] incoming conn %u but no server-side adopter\n",
                      info->m_hConn);
            return;
        }
    }

    GnsConnection* owner = nullptr;
    {
        std::lock_guard<std::recursive_mutex> lk(registryMutex);
        auto it = connMap().find(info->m_hConn);
        if (it != connMap().end()) owner = it->second;
    }
    if (!owner) {
        ::fprintf(stderr, "[GnsConnection] status change for unregistered conn %u (state %d -> %d)\n",
                  info->m_hConn, info->m_eOldState, info->m_info.m_eState);
        return;
    }
    owner->handleStatusChange(info->m_eOldState, info->m_info.m_eState,
                              info->m_info.m_eEndReason);
}

namespace gns
{
    static std::atomic<uint32_t> g_initRefCount{0};
    static ISteamNetworkingUtils* g_utils = nullptr;

    bool init() {
        if (g_initRefCount.fetch_add(1) != 0) {
            return true;
        }

        SteamNetworkingErrMsg errMsg;
        if (!GameNetworkingSockets_Init(nullptr, errMsg)) {
            ::fprintf(stderr, "[GnsConnection] GameNetworkingSockets_Init failed: %s\n", errMsg);
            g_initRefCount.fetch_sub(1);
            return false;
        }

        // R1.5 debug: pipe GNS internal messages to stderr so we can see why
        // connect/listen fails. Disabled in release once R1.5 is stable.
        SteamNetworkingUtils_LibV4()->SetDebugOutputFunction(
            k_ESteamNetworkingSocketsDebugOutputType_Msg,
            [](ESteamNetworkingSocketsDebugOutputType /*type*/, const char* msg) {
                ::fprintf(stderr, "[gns] %s\n", msg);
            });

        ISteamNetworkingSockets* gns = SteamNetworkingSockets_LibV12();
        if (!gns) {
            ::fprintf(stderr, "[GnsConnection] SteamNetworkingSockets_LibV12 returned null\n");
            GameNetworkingSockets_Kill();
            g_initRefCount.fetch_sub(1);
            return false;
        }

        ISteamNetworkingUtils* utils = SteamNetworkingUtils_LibV4();
        if (!utils) {
            ::fprintf(stderr, "[GnsConnection] SteamNetworkingUtils_LibV4 returned null\n");
            GameNetworkingSockets_Kill();
            g_initRefCount.fetch_sub(1);
            return false;
        }

        HSteamNetPollGroup pollGroup = gns->CreatePollGroup();
        if (pollGroup == k_HSteamNetPollGroup_Invalid) {
            ::fprintf(stderr, "[GnsConnection] CreatePollGroup failed\n");
            GameNetworkingSockets_Kill();
            g_initRefCount.fetch_sub(1);
            return false;
        }

        if (!utils->SetGlobalCallback_SteamNetConnectionStatusChanged(&gns_status_callback)) {
            ::fprintf(stderr, "[GnsConnection] SetGlobalCallback_SteamNetConnectionStatusChanged failed\n");
            gns->DestroyPollGroup(pollGroup);
            GameNetworkingSockets_Kill();
            g_initRefCount.fetch_sub(1);
            return false;
        }

        GnsConnection::s_gns       = gns;
        GnsConnection::s_pollGroup = pollGroup;
        g_utils                    = utils;
        return true;
    }

    void shutdown() {
        uint32_t refs = g_initRefCount.load();
        while (refs != 0 &&
               !g_initRefCount.compare_exchange_weak(refs, refs - 1)) {
        }
        if (refs == 0 || refs != 1) {
            return;
        }
        if (g_utils) {
            g_utils->SetGlobalCallback_SteamNetConnectionStatusChanged(nullptr);
            g_utils = nullptr;
        }
        if (GnsConnection::s_gns && GnsConnection::s_pollGroup != k_HSteamNetPollGroup_Invalid) {
            GnsConnection::s_gns->DestroyPollGroup(GnsConnection::s_pollGroup);
        }
        GnsConnection::s_gns       = nullptr;
        GnsConnection::s_pollGroup = k_HSteamNetPollGroup_Invalid;
        {
            std::lock_guard<std::recursive_mutex> lk(registryMutex);
            connMap().clear();
            listenerFactories().clear();
            listenerOwners().clear();
            p2pListenerRoutes().clear();
        }
        GameNetworkingSockets_Kill();
    }
} // namespace gns

// =============================================================================
// GnsConnection
// =============================================================================

GnsConnection::GnsConnection() = default;

GnsConnection::~GnsConnection() {
    if (_state != GnsConnectionState::Disconnected) {
        disconnect("destructor");
    }
}

void GnsConnection::setAdoptFactory(HSteamListenSocket listener,
                                    AdoptFactory factory) {
    if (listener == k_HSteamListenSocket_Invalid) return;
    std::lock_guard<std::recursive_mutex> lk(registryMutex);
    if (factory) {
        listenerFactories()[listener] = std::move(factory);
    } else {
        listenerFactories().erase(listener);
    }
}

void GnsConnection::clearAdoptFactory(HSteamListenSocket listener) {
    if (listener == k_HSteamListenSocket_Invalid) return;
    std::lock_guard<std::recursive_mutex> lk(registryMutex);
    listenerFactories().erase(listener);
}

namespace
{

std::string joinCommaSeparated(const std::vector<std::string>& values) {
    std::string result;
    for (const auto& value : values) {
        if (value.empty()) continue;
        if (!result.empty()) result.push_back(',');
        result += value;
    }
    return result;
}

bool splitIceEndpoint(const std::string& endpoint,
                      std::string& prefix,
                      std::string& host,
                      std::string& portSuffix) {
    prefix.clear();
    host.clear();
    portSuffix.clear();
    if (endpoint.empty()) return false;

    std::string_view value{endpoint};
    const size_t scheme = value.find("://");
    if (scheme != std::string_view::npos) {
        prefix.assign(value.substr(0, scheme + 3));
        value.remove_prefix(scheme + 3);
    } else if (value.starts_with("stun:")) {
        prefix = "stun:";
        value.remove_prefix(5);
    }
    if (value.empty()) return false;
    // Numeric IPv6 endpoints are already resolved. Preserve their brackets
    // and optional port exactly as supplied.
    if (value.front() == '[') {
        const size_t close = value.find(']');
        if (close == std::string_view::npos) return false;
        host.assign(value);
        return true;
    }

    const size_t colon = value.rfind(':');
    if (colon != std::string_view::npos) {
        if (value.find(':') != colon) {
            // Unbracketed IPv6 is left unchanged; GNS accepts numeric IPv6.
            host.assign(value);
            return true;
        }
        const std::string_view port = value.substr(colon + 1);
        unsigned parsedPort = 0;
        const auto parsed = std::from_chars(
            port.data(), port.data() + port.size(), parsedPort);
        if (port.empty() || parsed.ec != std::errc{} ||
            parsed.ptr != port.data() + port.size() ||
            parsedPort == 0 || parsedPort > 65535) {
            return false;
        }
        host.assign(value.substr(0, colon));
        portSuffix.assign(value.substr(colon));
    } else {
        host.assign(value);
    }
    return !host.empty();
}

bool resolveStunServers(P2PConfig& config, std::string* error) {
    UdpSocket resolverSocket;
    if (!resolverSocket.create()) {
        if (error) *error = "unable to initialize UDP resolver";
        return false;
    }
    for (std::string& endpoint : config.stunServers) {
        std::string prefix;
        std::string host;
        std::string portSuffix;
        if (!splitIceEndpoint(endpoint, prefix, host, portSuffix)) {
            if (error) *error = "invalid STUN endpoint: " + endpoint;
            return false;
        }
        if (!host.empty() && (host.front() == '[' || host.find(':') != std::string::npos)) {
            continue;
        }
        std::string resolved;
        if (!UdpSocket::resolveIPv4(host.c_str(), resolved)) {
            if (error) *error = "failed to resolve STUN host: " + host;
            return false;
        }
        endpoint = prefix + resolved + portSuffix;
    }
    return true;
}

int iceCandidateMask(const P2PConfig& config) {
    int mask = 0;
    if (config.icePolicy != P2PIcePolicy::RelayOnly) {
        if (config.allowPrivateCandidates) {
            mask |= k_nSteamNetworkingConfig_P2P_Transport_ICE_Enable_Private;
        }
        if (!config.stunServers.empty()) {
            mask |= k_nSteamNetworkingConfig_P2P_Transport_ICE_Enable_Public;
        }
    }
    if (config.icePolicy != P2PIcePolicy::DirectOnly && !config.turnServers.empty()) {
        mask |= k_nSteamNetworkingConfig_P2P_Transport_ICE_Enable_Relay;
    }
    return mask;
}

bool applyIncomingP2PConfig(HSteamNetConnection connection,
                            const P2PConfig& config) {
    if (!gns::g_utils || connection == k_HSteamNetConnection_Invalid ||
        !config.isValid()) return false;

    const int iceMask = iceCandidateMask(config);
    const std::string stun = joinCommaSeparated(config.stunServers);
    const std::string turn = joinCommaSeparated(config.turnServers);
    const std::string turnUsers = joinCommaSeparated(config.turnUsers);
    const std::string turnPasswords = joinCommaSeparated(config.turnPasswords);

    // Custom-signaling incoming connections do not inherit the initiating
    // peer's options. Apply the listener policy before AcceptConnection(),
    // which is the point where GNS transitions into route discovery.
    return gns::g_utils->SetConnectionConfigValueInt32(
               connection, k_ESteamNetworkingConfig_P2P_Transport_ICE_Enable,
               iceMask) &&
           gns::g_utils->SetConnectionConfigValueString(
               connection, k_ESteamNetworkingConfig_P2P_STUN_ServerList,
               stun.c_str()) &&
           gns::g_utils->SetConnectionConfigValueString(
               connection, k_ESteamNetworkingConfig_P2P_TURN_ServerList,
               turn.c_str()) &&
           gns::g_utils->SetConnectionConfigValueString(
               connection, k_ESteamNetworkingConfig_P2P_TURN_UserList,
               turnUsers.c_str()) &&
           gns::g_utils->SetConnectionConfigValueString(
               connection, k_ESteamNetworkingConfig_P2P_TURN_PassList,
               turnPasswords.c_str());
}

class GnsCustomConnectionSignaling final : public ISteamNetworkingConnectionSignaling {
public:
    GnsCustomConnectionSignaling(std::shared_ptr<ISignalingTransport> signaling,
                                 PeerId remotePeer)
        : _signaling(std::move(signaling)), _remotePeer(std::move(remotePeer)) {}

    bool SendSignal(HSteamNetConnection,
                    const SteamNetConnectionInfo_t&,
                    const void* message, int messageBytes) override {
        return _signaling && _signaling->isRunning() && messageBytes > 0 &&
               _signaling->sendSignal(_remotePeer, message,
                                      static_cast<size_t>(messageBytes));
    }

    void Release() override { delete this; }

private:
    std::shared_ptr<ISignalingTransport> _signaling;
    PeerId _remotePeer;
};

class GnsCustomSignalReceiveContext final : public ISteamNetworkingSignalingRecvContext {
public:
    GnsCustomSignalReceiveContext(PeerId sender,
                                  std::shared_ptr<ISignalingTransport> signaling)
        : _sender(std::move(sender)), _signaling(std::move(signaling)) {}

    ISteamNetworkingConnectionSignaling* OnConnectRequest(
        HSteamNetConnection connection,
        const SteamNetworkingIdentity& identityPeer,
        int localVirtualPort) override {
        if (localVirtualPort < 0 || localVirtualPort > UINT16_MAX) return nullptr;
        P2PListenerRoute route;
        {
            std::lock_guard<std::recursive_mutex> lock(registryMutex);
            const auto it = p2pListenerRoutes().find(static_cast<uint16_t>(localVirtualPort));
            if (it == p2pListenerRoutes().end()) return nullptr;
            route = it->second;
        }
        const char* identity = identityPeer.GetGenericString();
        if (!identity) return nullptr;
        PeerId remote{identity};
        // The validated signaling envelope is the routing authority. An
        // authenticated application backend can additionally bind it to an
        // account. Missing or mismatching identity inside the GNS signal is
        // always suspicious and is rejected here.
        if (!remote.isValid() || remote != _sender) return nullptr;
        if (!applyIncomingP2PConfig(connection, route.config)) {
            ::fprintf(stderr,
                      "[GnsConnection] failed to apply incoming P2P config on vport %d\n",
                      localVirtualPort);
            return nullptr;
        }
        if (!route.factory || !route.factory(connection, remote)) return nullptr;
        return new GnsCustomConnectionSignaling(
            route.signaling ? route.signaling : _signaling, std::move(remote));
    }

    void SendRejectionSignal(const SteamNetworkingIdentity&,
                             const void* message, int messageBytes) override {
        if (_signaling && messageBytes > 0) {
            (void)_signaling->sendSignal(_sender, message,
                                         static_cast<size_t>(messageBytes));
        }
    }

private:
    PeerId _sender;
    std::shared_ptr<ISignalingTransport> _signaling;
};

} // namespace

bool GnsConnection::setLocalP2PIdentity(const PeerId& localPeer) {
    if (!s_gns || !localPeer.isValid()) return false;
    SteamNetworkingIdentity current;
    if (s_gns->GetIdentity(&current)) {
        if (const char* currentText = current.GetGenericString()) {
            if (localPeer.value == currentText) return true;
        }
    }
    {
        std::lock_guard<std::recursive_mutex> lock(registryMutex);
        if (!connMap().empty() || !listenerOwners().empty() ||
            !p2pListenerRoutes().empty()) return false;
    }
    SteamNetworkingIdentity identity;
    identity.Clear();
    if (!identity.SetGenericString(localPeer.value.c_str())) return false;

    // ResetIdentity invalidates interface-owned routing state in standalone
    // GNS, including the poll group created by gns::init(). Destroy it before
    // the reset and recreate it afterwards; otherwise the first P2P
    // SetConnectionPollGroup fails and every receive pump returns -1.
    if (s_pollGroup != k_HSteamNetPollGroup_Invalid) {
        s_gns->DestroyPollGroup(s_pollGroup);
        s_pollGroup = k_HSteamNetPollGroup_Invalid;
    }
    s_gns->ResetIdentity(&identity);
    s_pollGroup = s_gns->CreatePollGroup();
    if (s_pollGroup == k_HSteamNetPollGroup_Invalid) return false;
    return true;
}

bool GnsConnection::prepareP2PConfig(P2PConfig& config, std::string* error) {
    if (error) error->clear();
    if (!config.isValid()) {
        if (error) *error = "invalid P2P configuration";
        return false;
    }
    return resolveStunServers(config, error) && config.isValid();
}

bool GnsConnection::setP2PAdoptFactory(
    const P2PConfig& config,
    std::shared_ptr<ISignalingTransport> signaling,
    P2PAdoptFactory factory) {
    P2PConfig prepared = config;
    std::string prepareError;
    if (!signaling || !factory ||
        !prepareP2PConfig(prepared, &prepareError)) {
        std::lock_guard<std::recursive_mutex> lock(registryMutex);
        p2pListenerRoutes().erase(config.virtualPort);
        if (!prepareError.empty()) {
            ::fprintf(stderr, "[GnsConnection] P2P listener config rejected: %s\n",
                      prepareError.c_str());
        }
        return false;
    }
    std::lock_guard<std::recursive_mutex> lock(registryMutex);
    p2pListenerRoutes()[prepared.virtualPort] = {
        std::move(prepared), std::move(signaling), std::move(factory)};
    return true;
}

void GnsConnection::clearP2PAdoptFactory(uint16_t virtualPort) {
    std::lock_guard<std::recursive_mutex> lock(registryMutex);
    p2pListenerRoutes().erase(virtualPort);
}

bool GnsConnection::receiveP2PSignal(
    const PeerId& sender,
    const void* data, size_t size,
    std::shared_ptr<ISignalingTransport> signaling) {
    if (!s_gns || !sender.isValid() || !data || size == 0 || size > INT32_MAX || !signaling) {
        return false;
    }
    GnsCustomSignalReceiveContext context(sender, std::move(signaling));
    return s_gns->ReceivedP2PCustomSignal(data, static_cast<int>(size), &context);
}

void GnsConnection::initClient(const char* address, uint16_t virtualPort) {
    if (!s_gns) {
        ::fprintf(stderr, "[GnsConnection] initClient called before gns::init()\n");
        return;
    }
    if (_state != GnsConnectionState::Disconnected) {
        ::fprintf(stderr, "[GnsConnection] initClient called while not Disconnected\n");
        return;
    }

    _address = address ? address : "";
    _port    = virtualPort;
    _role    = GnsConnectionRole::Client;

    // R1.5 (2026-07-26): switch from P2P to IP-mode. The P2P API requires
    // a relay service (Steam backend or custom signaling) which is not
    // available in libv12 standalone mode. ConnectByIPAddress works over
    // plain UDP on loopback, which is what we need for the echo test.
    SteamNetworkingIPAddr addr;
    addr.Clear();
    // Parse "address" — currently we accept IPv4 dotted-decimal only;
    // IPv6 support is R4 work.
    if (!addr.ParseString(address)) {
        ::fprintf(stderr, "[GnsConnection] initClient: invalid address '%s'\n", address);
        return;
    }
    // If the caller didn't specify a port, fill in the one they passed.
    if (addr.m_port == 0) addr.m_port = virtualPort;

    _conn = s_gns->ConnectByIPAddress(addr, 0, nullptr);
    if (_conn == k_HSteamNetConnection_Invalid) {
        ::fprintf(stderr, "[GnsConnection] ConnectByIPAddress failed\n");
        return;
    }

    if (!s_gns->SetConnectionPollGroup(_conn, s_pollGroup)) {
        ::fprintf(stderr, "[GnsConnection] SetConnectionPollGroup failed\n");
        s_gns->CloseConnection(_conn, 0, "poll group setup failed", false);
        _conn = k_HSteamNetConnection_Invalid;
        return;
    }

    {
        std::lock_guard<std::recursive_mutex> lk(registryMutex);
        connMap()[_conn] = this;
    }

    setState(GnsConnectionState::Connecting);
}

bool GnsConnection::initP2PClient(
    const PeerId& remotePeer,
    const P2PConfig& config,
    std::shared_ptr<ISignalingTransport> signaling) {
    if (!s_gns || _state != GnsConnectionState::Disconnected ||
        !remotePeer.isValid() || !config.isValid() ||
        !signaling || !signaling->isRunning()) {
        return false;
    }
    P2PConfig prepared = config;
    std::string prepareError;
    if (!prepareP2PConfig(prepared, &prepareError)) {
        ::fprintf(stderr, "[GnsConnection] P2P client config rejected: %s\n",
                  prepareError.c_str());
        return false;
    }
    const int iceMask = iceCandidateMask(prepared);
    if (iceMask == 0) return false;

    SteamNetworkingIdentity remoteIdentity;
    remoteIdentity.Clear();
    if (!remoteIdentity.SetGenericString(remotePeer.value.c_str())) return false;

    const std::string stun = joinCommaSeparated(prepared.stunServers);
    const std::string turn = joinCommaSeparated(prepared.turnServers);
    const std::string turnUsers = joinCommaSeparated(prepared.turnUsers);
    const std::string turnPasswords = joinCommaSeparated(prepared.turnPasswords);
    std::vector<SteamNetworkingConfigValue_t> options;
    options.reserve(5);
    SteamNetworkingConfigValue_t option;
    option.SetInt32(k_ESteamNetworkingConfig_P2P_Transport_ICE_Enable, iceMask);
    options.push_back(option);
    if (!stun.empty()) {
        option.SetString(k_ESteamNetworkingConfig_P2P_STUN_ServerList, stun.c_str());
        options.push_back(option);
    }
    if (!turn.empty()) {
        option.SetString(k_ESteamNetworkingConfig_P2P_TURN_ServerList, turn.c_str());
        options.push_back(option);
    }
    if (!turnUsers.empty()) {
        option.SetString(k_ESteamNetworkingConfig_P2P_TURN_UserList, turnUsers.c_str());
        options.push_back(option);
        option.SetString(k_ESteamNetworkingConfig_P2P_TURN_PassList, turnPasswords.c_str());
        options.push_back(option);
    }

    auto* customSignaling =
        new GnsCustomConnectionSignaling(std::move(signaling), remotePeer);
    _conn = s_gns->ConnectP2PCustomSignaling(
        customSignaling, &remoteIdentity, config.virtualPort,
        static_cast<int>(options.size()), options.data());
    // GNS assumes ownership and calls Release(), including the failure path.
    if (_conn == k_HSteamNetConnection_Invalid) return false;
    if (!s_gns->SetConnectionPollGroup(_conn, s_pollGroup)) {
        s_gns->CloseConnection(_conn, 0, "poll group setup failed", false);
        _conn = k_HSteamNetConnection_Invalid;
        return false;
    }

    _address = "p2p:" + remotePeer.value;
    _port = config.virtualPort;
    _remotePeerId = remotePeer;
    _isP2P = true;
    _role = GnsConnectionRole::Client;
    {
        std::lock_guard<std::recursive_mutex> lock(registryMutex);
        connMap()[_conn] = this;
    }
    setState(GnsConnectionState::Connecting);
    return true;
}

void GnsConnection::initServer(uint16_t virtualPort) {
    if (!s_gns) {
        ::fprintf(stderr, "[GnsConnection] initServer called before gns::init()\n");
        return;
    }
    if (_state != GnsConnectionState::Disconnected) {
        ::fprintf(stderr, "[GnsConnection] initServer called while not Disconnected\n");
        return;
    }

    // R1.5: bind to any IPv4 address (0.0.0.0) on the requested port.
    SteamNetworkingIPAddr localAddr;
    localAddr.Clear();
    localAddr.SetIPv4(0, virtualPort);  // 0.0.0.0:virtualPort

    _listen = s_gns->CreateListenSocketIP(localAddr, 0, nullptr);
    if (_listen == k_HSteamListenSocket_Invalid) {
        ::fprintf(stderr, "[GnsConnection] CreateListenSocketIP failed on port %u\n",
                  virtualPort);
        return;
    }

    {
        std::lock_guard<std::recursive_mutex> lk(registryMutex);
        listenerOwners()[_listen] = this;
    }

    _port = virtualPort;
    _role = GnsConnectionRole::Listener;
    setState(GnsConnectionState::Connected);
}

void GnsConnection::update() {
    (void)pump();
}

bool GnsConnection::isPumping() {
    return insidePump;
}

GnsPumpResult GnsConnection::pump(const GnsPumpBudget& requestedBudget) {
    GnsPumpResult result;
    if (!s_gns || s_pollGroup == k_HSteamNetPollGroup_Invalid || insidePump) {
        // R6 C8 M-10 (2026-08-25): the re-entry guard silently swallowed
        // nested-pump calls. Document the behaviour with a single-shot
        // stderr line in debug builds so misuse is visible.
#ifndef NDEBUG
        static thread_local bool warned = false;
        if (insidePump && !warned) {
            ::fprintf(stderr, "[GnsConnection] nested pump() detected — ignored "
                              "(callers must not re-enter pump()).\n");
            warned = true;
        }
#endif
        return result;
    }

    std::lock_guard<std::mutex> pumpLock(pumpMutex);
    insidePump = true;
    struct PumpScope {
        ~PumpScope() { insidePump = false; }
    } pumpScope;

    const uint32_t maxMessages = std::max(1u, requestedBudget.maxMessages);
    const uint32_t maxBytes = std::max(1u, requestedBudget.maxBytes);
    const uint32_t maintenanceNowMs = nowMs();
    std::vector<GnsConnection*> owners;
    {
        std::lock_guard<std::recursive_mutex> lk(registryMutex);
        owners.reserve(connMap().size());
        for (const auto& [handle, owner] : connMap()) {
            (void)handle;
            if (owner) owners.push_back(owner);
        }
    }
    // R6 C8 H-02 (2026-08-25): sort owners by netId, not pointer. Pointer
    // ordering is allocator-dependent (ASLR, address-space layout) and
    // varied across runs. NetId is stable across processes and recordings.
    std::sort(owners.begin(), owners.end(),
              [](const GnsConnection* a, const GnsConnection* b) {
                  return a->getNetId() < b->getNetId();
              });
    owners.erase(std::unique(owners.begin(), owners.end()), owners.end());
    for (GnsConnection* owner : owners) owner->runMaintenance(maintenanceNowMs);

    // R5.4 (2026-08-25): drain send-side fault queues BEFORE the receive
    // loop so frames that are "ready to go on the wire" actually leave
    // the system this pump iteration (not the next one).
    {
        for (GnsConnection* owner : owners) {
            if (!owner->_faultCtl) continue;
            TransportFaultInterceptor* ic = owner->getFaultInterceptor();
            if (!ic || !ic->isEnabled()) continue;  // R5.4: skip if no profile
            std::vector<std::pair<std::vector<uint8_t>, uint8_t>> sendOut;
            // dtSeconds for the rate-limit refill — the caller doesn't
            // supply one so we use a small constant (1ms). Tests that
            // care about exact refill behavior inject virtual time.
            ic->tickSend(maintenanceNowMs, 0.001, sendOut);
            for (auto& [bytes, channel] : sendOut) {
                owner->_rawSend(bytes.data(),
                                static_cast<uint32_t>(bytes.size()),
                                channel);
            }
        }
    }

    // Exactly one callback pump for the shared GNS context.
    s_gns->RunCallbacks();

    constexpr uint32_t kBatchSize = 32;
    while (result.messages < maxMessages && result.bytes < maxBytes) {
        SteamNetworkingMessage_t* messages[kBatchSize]{};
        const uint32_t remaining = maxMessages - result.messages;
        const int requestCount = static_cast<int>(std::min(kBatchSize, remaining));
        const int count = s_gns->ReceiveMessagesOnPollGroup(
            s_pollGroup, messages, requestCount);
        if (count < 0) {
            ::fprintf(stderr, "[GnsConnection] ReceiveMessagesOnPollGroup returned %d\n", count);
            break;
        }
        if (count == 0) break;

        for (int i = 0; i < count; ++i) {
            SteamNetworkingMessage_t* message = messages[i];
            GnsConnection* owner = nullptr;
            {
                std::lock_guard<std::recursive_mutex> lk(registryMutex);
                auto it = connMap().find(message->m_conn);
                if (it != connMap().end()) owner = it->second;
            }
            if (message->m_cbSize > 0) {
                result.bytes += static_cast<uint32_t>(message->m_cbSize);
            }
            ++result.messages;
            if (owner && message->m_pData && message->m_cbSize > 0) {
                const uint8_t* pData = static_cast<const uint8_t*>(message->m_pData);
                const size_t   cbSize = static_cast<size_t>(message->m_cbSize);

                // R5.4 (2026-08-25): test seam — fake receiver bypasses
                // both the fault interceptor and onRawData.
                if (owner->_fakeReceiver) {
                    // Pull channel from the sealed frame's PacketHeader
                    // (offset 4 = channel byte, see PacketCodec.h).
                    uint8_t channel = 0;
                    if (cbSize >= PacketCodec::kHeaderSize) {
                        channel = pData[4];
                    }
                    owner->_fakeReceiver(pData, cbSize, channel);
                } else if (auto* ic = owner->getFaultInterceptor(); ic && ic->isEnabled()) {
                    // Only route through the interceptor if it has a real
                    // (non-no-op) profile installed. Otherwise the frame
                    // would queue, drain on the next tick, and add a
                    // spurious one-pump lag to every byte — which would
                    // break every existing GNS test that pumps until a
                    // frame is received. isEnabled() returns false when
                    // no profile is installed OR the installed profile
                    // is a no-op (zero knobs).
                    uint8_t channel = 0;
                    if (cbSize >= PacketCodec::kHeaderSize) {
                        channel = pData[4];
                    }
                    ic->onRecv(static_cast<uint64_t>(maintenanceNowMs),
                               pData, cbSize, channel);
                } else {
                    owner->onRawData(pData, cbSize);
                }
            }
            message->Release();
        }
    }

    // R5.4 (2026-08-25): drain the recv-side fault queues. Frames that
    // are released (deadline reached, rate-limit token available) are
    // forwarded to onRawData → PacketCodec::decode → _packetHandler.
    {
        for (GnsConnection* owner : owners) {
            if (!owner->_faultCtl) continue;
            TransportFaultInterceptor* ic = owner->getFaultInterceptor();
            if (!ic || !ic->isEnabled()) continue;  // R5.4: skip if no profile
            std::vector<std::pair<std::vector<uint8_t>, uint8_t>> recvOut;
            ic->tickRecv(maintenanceNowMs, 0.001, recvOut);
            for (auto& [bytes, channel] : recvOut) {
                (void)channel;
                owner->onRawData(bytes.data(), bytes.size());
            }
        }
    }

    result.budgetExhausted =
        result.messages >= maxMessages || result.bytes >= maxBytes;
    // R5.5 (2026-08-25): cache the inbound byte total on every known
    // owner so getLastPumpBytes() can return it from the profiler path
    // without re-running GNS. We attribute the full pump total to each
    // owner — the per-message m_conn ownership information is already
    // discarded by the time we reach this point, and the profiler only
    // needs a per-connection trend ("is this connection draining
    // inbound traffic?"), not byte-accurate attribution.
    for (GnsConnection* owner : owners) {
        owner->_lastPumpBytes = result.bytes;
    }
    return result;
}

int GnsConnection::send(uint8_t channel, const void* data, size_t len) {
    if (!s_gns || _conn == k_HSteamNetConnection_Invalid) return -1;
    if ((data == nullptr && len != 0) ||
        len > PacketCodec::kMaxDecodedBodySize || channel > CHANNEL_ACK) {
        return -1;
    }
    // R1 done: allow send in Connected (handshake off), Handshaking
    // (handshake frames), or Ready (post-handshake app data).
    if (_state != GnsConnectionState::Connected &&
        _state != GnsConnectionState::Handshaking &&
        _state != GnsConnectionState::Ready) {
        return -1;
    }

    // R2: seal the payload through PacketCodec. If the resulting frame
    // fits in kFrameMtu, send it as a single sealed frame; otherwise
    // fragment the payload first via the Assembler's static fragmenter.
    // Compressed flag is opt-in — send() never auto-compresses (R3 will
    // add a higher-level API to mark "this payload is compressible").
    const uint32_t tsMs = nowMs();

    // R5.4 (2026-08-25): fault injector hook. If a controller is
    // attached and a profile is installed for this netId, route the
    // sealed bytes through the interceptor instead of going directly
    // to _rawSend. The interceptor holds the frames in a delay queue
    // and releases them on the next pump tick.
    TransportFaultInterceptor* interceptor = getFaultInterceptor();
    const bool faultActive = interceptor && interceptor->isEnabled();

    if (len <= kFrameMtu - PacketCodec::kHeaderSize - PacketCodec::kCrcSize) {
        // Single frame, no Fragmented flag.
        auto wire = PacketCodec::encode(
            static_cast<const uint8_t*>(data), len,
            kMsgTypeApp, kSchemaVersion,
            channel,
            /*flags=*/0,
            tsMs,
            /*compress=*/false);
        if (wire.empty()) return -1;
        if (faultActive) {
            interceptor->onSend(static_cast<uint64_t>(tsMs),
                                wire.data(), wire.size(), channel);
            return 0;
        }
        return _rawSend(wire.data(), static_cast<uint32_t>(wire.size()), channel);
    }

    // Multi-frame path. R2 does not auto-compress — only fragment.
    auto frames = PacketAssembler::fragment(
        static_cast<const uint8_t*>(data), len,
        static_cast<uint32_t>(kFrameMtu),
        kMsgTypeApp, kSchemaVersion,
        channel, tsMs, _nextFragmentId.fetch_add(1, std::memory_order_relaxed));
    if (frames.empty()) {
        ::fprintf(stderr, "[GnsConnection] send: fragment() returned empty (MTU too small?)\n");
        return -1;
    }
    int lastResult = 0;
    if (faultActive) {
        // Queue each frame through the interceptor; release happens on
        // the next pump tick.
        for (const auto& f : frames) {
            interceptor->onSend(static_cast<uint64_t>(tsMs),
                                f.data(), f.size(), channel);
        }
        return 0;
    }
    for (const auto& f : frames) {
        lastResult = _rawSend(f.data(), static_cast<uint32_t>(f.size()), channel);
        if (lastResult != 0) break;
    }
    return lastResult;
}

int GnsConnection::sendEncoded(uint8_t channel, const void* data, size_t len) {
    if (!s_gns || _conn == k_HSteamNetConnection_Invalid || !data || len == 0 ||
        len > UINT32_MAX || channel > CHANNEL_ACK ||
        (_state != GnsConnectionState::Connected &&
         _state != GnsConnectionState::Handshaking &&
         _state != GnsConnectionState::Ready)) return -1;
    const auto* wire = static_cast<const uint8_t*>(data);
    TransportFaultInterceptor* interceptor = getFaultInterceptor();
    const bool faultActive = interceptor && interceptor->isEnabled();
    if (len <= kFrameMtu) {
        if (faultActive) {
            interceptor->onSend(static_cast<uint64_t>(nowMs()),
                                wire, len, channel);
            return 0;
        }
        return _rawSend(wire, static_cast<uint32_t>(len), channel);
    }

    // Internal protocol producers hand us an already sealed PacketCodec
    // frame. Re-open oversized frames and fragment their decoded body while
    // preserving the original protocol identity; otherwise replication/RPC
    // payloads larger than the transport MTU bypass the normal fragmenter.
    DecodedPacket decoded = PacketCodec::decode(wire, len);
    if (!decoded.ok ||
        hasFlag(decoded.header.flags, PacketFlag::Fragmented) ||
        hasFlag(decoded.header.flags, PacketFlag::RequiresAck)) {
        return -1;
    }
    auto frames = PacketAssembler::fragment(
        decoded.body.data(), decoded.body.size(), static_cast<uint32_t>(kFrameMtu),
        decoded.header.msgType, decoded.header.schemaVersion, channel,
        decoded.header.timestampMs, _nextFragmentId.fetch_add(1, std::memory_order_relaxed));
    if (frames.empty()) return -1;

    int lastResult = 0;
    for (const auto& frame : frames) {
        if (faultActive) {
            interceptor->onSend(static_cast<uint64_t>(nowMs()),
                                frame.data(), frame.size(), channel);
            lastResult = 0;
        } else {
            lastResult = _rawSend(frame.data(), static_cast<uint32_t>(frame.size()), channel);
        }
        if (lastResult != 0) break;
    }
    return lastResult;
}

int GnsConnection::sendRequireAck(uint16_t msgType, uint8_t channel,
                                  const void* data, size_t len,
                                  AckTracker::Callback onAck) {
    if (!s_gns || _conn == k_HSteamNetConnection_Invalid ||
        !data || len == 0 || len > UINT32_MAX || channel > CHANNEL_ACK) return -1;
    if (_state != GnsConnectionState::Connected &&
        _state != GnsConnectionState::Handshaking &&
        _state != GnsConnectionState::Ready) {
        return -1;
    }
    const uint32_t seq = _ackTracker.allocateSeq();
    auto wire = AckPipeline::sealAckable(
        static_cast<const uint8_t*>(data), len,
        msgType, channel, seq, nowMs(), /*compress=*/ false);
    if (onAck) {
        _ackTracker.registerPending(seq, std::move(onAck));
    }
    if (auto* interceptor = getFaultInterceptor();
        interceptor && interceptor->isEnabled()) {
        interceptor->onSend(static_cast<uint64_t>(nowMs()),
                            wire.data(), wire.size(), channel);
        return 0;
    }
    return _rawSend(wire.data(), static_cast<uint32_t>(wire.size()), channel);
}

// R2: low-level GNS send. R4.0 (2026-07-29) expands the channel -> GNS
// send-flag map to all 4 channels declared in AYNetwork/INetwork.h:39-42.
//
//   CHANNEL_RELIABLE   = 0  -> k_nSteamNetworkingSend_Reliable
//                            (default — RPC default + Replication Full)
//
//   CHANNEL_UNRELIABLE = 1  -> k_nSteamNetworkingSend_Unreliable
//                            (high-freq RPC + Replication Delta)
//
//   CHANNEL_FRAGMENTED = 2  -> k_nSteamNetworkingSend_Reliable
//                            | k_nSteamNetworkingSend_NoNagle
//                            (Nagle coalescing defeats multi-frame
//                            payloads; Reliable+NoNagle keeps GNS
//                            ordering without batching. PacketCodec
//                            does the actual split via PacketAssembler.)
//
//   CHANNEL_ACK        = 3  -> k_nSteamNetworkingSend_Reliable
//                            (R4.0 no-op marker; R4.1 will route
//                            ACKs through a reserved short-payload
//                            slot in GnsConnection.
//
// MSVC strict enum: do NOT do arithmetic on the GNS send-flag
// constants — assign to `int flags` first then bitwise-OR.
int GnsConnection::_rawSend(const uint8_t* data, uint32_t len, uint8_t channel) {
    if (!data || len == 0 || channel > CHANNEL_ACK) return -1;

    // R5.4 (2026-08-25): test seam — if a fake sender is installed,
    // route through it instead of GNS. Tests use this to inject sealed
    // bytes without spinning up GNS.
    if (_fakeSender) {
        return _fakeSender(data, len, channel) ? 0 : -1;
    }

    int flags;
    switch (channel) {
        case CHANNEL_UNRELIABLE:
            flags = k_nSteamNetworkingSend_Unreliable;
            break;
        case CHANNEL_FRAGMENTED:
            flags = k_nSteamNetworkingSend_Reliable
                  | k_nSteamNetworkingSend_NoNagle;
            break;
        case CHANNEL_ACK:
            // R4.1-B: small ack-only frames — reliable but no nagle delay.
            flags = k_nSteamNetworkingSend_Reliable
                  | k_nSteamNetworkingSend_NoNagle;
            break;
        case CHANNEL_RELIABLE:
        default:
            flags = k_nSteamNetworkingSend_Reliable;
            break;
    }
    EResult r = s_gns->SendMessageToConnection(_conn, data, len, flags, nullptr);
    return (r == k_EResultOK) ? 0 : -1;
}

// R2: monotonic-ish clock used to stamp PacketHeader.timestampMs.
//
// R6 (2026-08-25): when s_nowOverride is non-null, consult it first so
// determinism tests can drive the wire-time stamp from a logical clock.
uint32_t GnsConnection::nowMs() {
    if (s_nowOverride) {
        return static_cast<uint32_t>((s_nowOverride() / 1000u) & 0xFFFFFFFFu);
    }
    return static_cast<uint32_t>((ayt::performanceNowUs() / 1000u) & 0xFFFFFFFFu);
}

void GnsConnection::setNowOverrideForTesting(NowOverrideFn fn) {
    s_nowOverride = std::move(fn);
}

void GnsConnection::clearNowOverride() {
    s_nowOverride = nullptr;
}

void GnsConnection::setNowOverrideForTickRate(uint32_t serverTick, uint32_t tickRate) {
    const uint32_t safeRate = (tickRate > 0u) ? tickRate : 1u;
    s_nowOverride = [serverTick, safeRate]() {
        // Logical time: (serverTick * 1'000'000) / tickRate microseconds.
        // The captured `serverTick` is taken at install time — tests that
        // want the override to track a moving counter must update the
        // override via setNowOverrideForTesting each tick.
        return (static_cast<uint64_t>(serverTick) * 1000000u) /
               static_cast<uint64_t>(safeRate);
    };
}

// =============================================================================
// R5.4 (2026-08-25): fault-controller wiring.
// =============================================================================

void GnsConnection::attachFaultController(TransportFaultController* ctl) {
    _faultCtl = ctl;
    // Destroy the existing interceptor; a fresh one will be created
    // lazily on the next send/recv that needs it (via getFaultInterceptor).
    _faultInterceptor.reset();
}

TransportFaultInterceptor* GnsConnection::getFaultInterceptor() {
    if (!_faultCtl) return nullptr;
    if (_netId == 0) return nullptr;
    if (!_faultInterceptor) {
        _faultInterceptor = std::make_unique<TransportFaultInterceptor>(_netId, *_faultCtl);
    }
    return _faultInterceptor.get();
}

void GnsConnection::runMaintenance(uint32_t monotonicNowMs) {
    _ackTracker.expire();
    _assembler.reapExpired(monotonicNowMs);
}

void GnsConnection::disconnect(const char* reason, DisconnectReason code) {
    if (!s_gns) return;
    if (_state == GnsConnectionState::Disconnected) return;

    setState(GnsConnectionState::Disconnecting);

    if (_conn != k_HSteamNetConnection_Invalid) {
        {
            std::lock_guard<std::recursive_mutex> lk(registryMutex);
            connMap().erase(_conn);
        }
        _lastDisconnectReason = code == DisconnectReason::Unknown
            ? DisconnectReason::UserQuit : code;
        // Ask GNS to push queued reliable frames before closing. Keeping the
        // connection in GNS linger state is unsafe here because this wrapper
        // intentionally releases its handle synchronously; the close itself
        // still carries the reason to a reachable peer.
        (void)s_gns->FlushMessagesOnConnection(_conn);
        s_gns->CloseConnection(_conn, encodeDisconnectReason(_lastDisconnectReason),
                               reason, false);
        _conn = k_HSteamNetConnection_Invalid;
    }
    if (_listen != k_HSteamListenSocket_Invalid) {
        const HSteamListenSocket closingListen = _listen;
        clearAdoptFactory(closingListen);
        {
            std::lock_guard<std::recursive_mutex> lk(registryMutex);
            listenerOwners().erase(closingListen);
        }
        s_gns->CloseListenSocket(_listen);
        _listen = k_HSteamListenSocket_Invalid;
        _lastDisconnectReason = DisconnectReason::HostShutdown;
    }
    _assembler.clear();
    _role = GnsConnectionRole::None;
    setState(GnsConnectionState::Disconnected);
}

int GnsConnection::getPing() const {
    if (!s_gns || _conn == k_HSteamNetConnection_Invalid) return -1;
    SteamNetConnectionRealTimeStatus_t status{};
    SteamNetConnectionRealTimeLaneStatus_t lane;
    // R1.5: query 1 lane (matches GNS default config). Asking for more
    // triggers an "[gns] Invalid lane count" warning.
    if (s_gns->GetConnectionRealTimeStatus(_conn, &status, 1, &lane)
        == k_EResultOK) {
        return status.m_nPing;
    }
    return -1;
}

void GnsConnection::adoptIncomingConnection(HSteamNetConnection conn) {
    _conn = conn;
    if (_role != GnsConnectionRole::Listener) {
        _role = GnsConnectionRole::AcceptedServerPeer;
    }
    if (s_gns && s_pollGroup != k_HSteamNetPollGroup_Invalid) {
        s_gns->SetConnectionPollGroup(_conn, s_pollGroup);
        SteamNetConnectionInfo_t info{};
        if (s_gns->GetConnectionInfo(_conn, &info)) {
            char address[SteamNetworkingIPAddr::k_cchMaxString]{};
            info.m_addrRemote.ToString(address, sizeof(address), true);
            _address = address;
            _port = info.m_addrRemote.m_port;
        }
    }
    {
        std::lock_guard<std::recursive_mutex> lk(registryMutex);
        connMap()[_conn] = this;
    }
    setState(GnsConnectionState::Connected);
}

void GnsConnection::adoptIncomingP2PConnection(HSteamNetConnection conn,
                                               const PeerId& remotePeer,
                                               uint16_t virtualPort) {
    _remotePeerId = remotePeer;
    _isP2P = true;
    _address = "p2p:" + remotePeer.value;
    _port = virtualPort;
    adoptIncomingConnection(conn);
    // P2P route identity is more useful than an empty pre-ICE IP address.
    _address = "p2p:" + remotePeer.value;
    _port = virtualPort;
}

P2PConnectionInfo GnsConnection::getP2PConnectionInfo(const PeerId& localPeer) const {
    P2PConnectionInfo result;
    result.localPeerId = localPeer;
    result.remotePeerId = _remotePeerId;
    result.pingMs = getPing();
    if (!_isP2P || !s_gns || _conn == k_HSteamNetConnection_Invalid) return result;
    if (_state != GnsConnectionState::Connected &&
        _state != GnsConnectionState::Handshaking &&
        _state != GnsConnectionState::Ready) return result;
    SteamNetConnectionInfo_t info{};
    if (!s_gns->GetConnectionInfo(_conn, &info)) return result;
    result.path = (info.m_nFlags & k_nSteamNetworkConnectionInfoFlags_Relayed)
        ? P2PPathKind::Relayed : P2PPathKind::Direct;
    if (!info.m_addrRemote.IsIPv6AllZeros()) {
        char address[SteamNetworkingIPAddr::k_cchMaxString]{};
        info.m_addrRemote.ToString(address, sizeof(address), true);
        result.remoteAddress = address;
    }
    return result;
}

void GnsConnection::setState(GnsConnectionState newState) {
    if (_state == newState) return;
    GnsConnectionState old = _state;
    _state = newState;
    if (_stateHandler) {
        _stateHandler(old, newState);
    }
}

void GnsConnection::handleStatusChange(int /*oldGnsState*/, int newGnsState,
                                       int endReason) {
    switch (newGnsState) {
        case k_ESteamNetworkingConnectionState_Connected: {
            // Custom-signaling P2P can deliver and complete the protocol
            // handshake from ReceivedP2PCustomSignal before GNS dispatches a
            // queued Connected status callback. Connection state is monotonic:
            // a late transport callback must never downgrade Ready and cause
            // all subsequent application packets to be rejected.
            if (_state == GnsConnectionState::Ready) break;
            // R1 done: branch on protocol version. Legacy (version=0) goes
            // straight to Connected (which == Ready under isConnected()).
            // With handshake enabled, transition to Handshaking and the
            // client immediately sends HELLO; the server waits for HELLO
            // before responding with WELCOME.
            if (_protocolVersion != 0 && _role == GnsConnectionRole::Client) {
                // Only the initiating client sends HELLO.
                _sendHello();
                setState(GnsConnectionState::Handshaking);
            } else if (_protocolVersion != 0) {
                // Listener/accepted server peer waits for the client's HELLO.
                setState(GnsConnectionState::Connected);
            } else {
                setState(GnsConnectionState::Connected);
            }
            break;
        }
        case k_ESteamNetworkingConnectionState_ClosedByPeer:
        case k_ESteamNetworkingConnectionState_ProblemDetectedLocally: {
            // Graceful peer closes carry AYNetwork's code in GNS's 1xxx app
            // range. Transport/system failures have no AY code and remain
            // ConnectionLost. A handshake REJECT may already have installed
            // the more specific ProtocolMismatch reason, which we preserve.
            const DisconnectReason remoteReason = decodeDisconnectReason(endReason);
            if (remoteReason != DisconnectReason::Unknown) {
                _lastDisconnectReason = remoteReason;
            } else if (_lastDisconnectReason == DisconnectReason::Unknown) {
                _lastDisconnectReason = DisconnectReason::ConnectionLost;
            }
            setState(GnsConnectionState::Disconnecting);
            if (s_gns && _conn != k_HSteamNetConnection_Invalid) {
                s_gns->CloseConnection(_conn, 0, "peer closed", false);
            }
            {
                std::lock_guard<std::recursive_mutex> lk(registryMutex);
                connMap().erase(_conn);
            }
            _conn = k_HSteamNetConnection_Invalid;
            setState(GnsConnectionState::Disconnected);
            break;
        }
        default:
            // Connecting / FindingRoute / None: leave state as-is.
            break;
    }
}

// =============================================================================
// R1 done (2026-07-27): Handshake implementation.
//
// Wire format is explicitly little-endian, independent of host byte order:
//   HELLO:    [u8 msgType=1][u32 version][u8 nameLen][name bytes]
//   WELCOME:  [u8 msgType=2][u32 version][u8 reasonCode=0]
//   REJECT:   [u8 msgType=3][u8 reasonCode=DisconnectReason]
//
// HELLO/WELCOME max payload = 38 bytes; REJECT = 2 bytes. Both fit in a
// single UDP datagram comfortably (GNS reliable layer handles framing).
// =============================================================================

// Append a little-endian uint32 to buf; returns bytes written.
static size_t appendU32LE(uint8_t* buf, uint32_t v) {
    buf[0] = static_cast<uint8_t>(v & 0xFF);
    buf[1] = static_cast<uint8_t>((v >> 8)  & 0xFF);
    buf[2] = static_cast<uint8_t>((v >> 16) & 0xFF);
    buf[3] = static_cast<uint8_t>((v >> 24) & 0xFF);
    return 4;
}

static size_t appendU16LE(uint8_t* buf, uint16_t v) {
    buf[0] = static_cast<uint8_t>(v & 0xFF);
    buf[1] = static_cast<uint8_t>((v >> 8) & 0xFF);
    return 2;
}

// Read a little-endian uint32 from buf; returns bytes consumed.
static size_t readU32LE(const uint8_t* buf, uint32_t& out) {
    out =  static_cast<uint32_t>(buf[0])
        | (static_cast<uint32_t>(buf[1]) << 8)
        | (static_cast<uint32_t>(buf[2]) << 16)
        | (static_cast<uint32_t>(buf[3]) << 24);
    return 4;
}

static size_t readU16LE(const uint8_t* buf, uint16_t& out) {
    out = static_cast<uint16_t>(buf[0]) |
          static_cast<uint16_t>(static_cast<uint16_t>(buf[1]) << 8);
    return 2;
}

bool GnsConnection::setHandshakeAdmissionToken(
    const void* bytes, size_t size) {
    if ((!bytes && size != 0) || size > kConnectionAdmissionMaxBytes ||
        _state != GnsConnectionState::Disconnected) {
        return false;
    }
    if (size == 0) {
        _handshakeAdmissionToken.clear();
        return true;
    }
    const auto* first = static_cast<const uint8_t*>(bytes);
    _handshakeAdmissionToken.assign(first, first + size);
    return true;
}

void GnsConnection::_sendHello() {
    // R2: build the same R1 body bytes (HandshakeMsgType=Hello, version,
    // name) then seal via PacketCodec with msgType=kMsgTypeHandshake so
    // onRawData can demux by msgType instead of sniffing the first byte.
    std::vector<uint8_t> buf(
        1 + 4 + 1 + kHandshakeMaxNameLen + 2 +
        _handshakeAdmissionToken.size());
    size_t pos = 0;
    buf[pos++] = static_cast<uint8_t>(HandshakeMsgType::Hello);
    pos += appendU32LE(buf.data() + pos, _protocolVersion);
    std::string name = _address;
    if (name.size() > kHandshakeMaxNameLen) name.resize(kHandshakeMaxNameLen);
    buf[pos++] = static_cast<uint8_t>(name.size());
    if (!name.empty()) {
        std::memcpy(buf.data() + pos, name.data(), name.size());
        pos += name.size();
    }
    pos += appendU16LE(buf.data() + pos,
                       static_cast<uint16_t>(_handshakeAdmissionToken.size()));
    if (!_handshakeAdmissionToken.empty()) {
        std::memcpy(buf.data() + pos, _handshakeAdmissionToken.data(),
                    _handshakeAdmissionToken.size());
        pos += _handshakeAdmissionToken.size();
    }
    buf.resize(pos);
    auto wire = PacketCodec::encode(
        buf.data(), buf.size(),
        kMsgTypeHandshake, kSchemaVersion,
        CHANNEL_RELIABLE,
        /*flags=*/0,
        nowMs(),
        /*compress=*/false);
    (void)_rawSend(wire.data(), static_cast<uint32_t>(wire.size()), CHANNEL_RELIABLE);
    // R5.5 (2026-08-25): profiler hook — handshake bytes count toward
    // the byMsgTypeExtras map keyed by handshakeExtrasKey(Hello).
    if (_profilerSendHook) {
        _profilerSendHook(profiler::handshakeExtrasKey(HandshakeMsgType::Hello),
                          static_cast<uint64_t>(wire.size()));
    }
}

void GnsConnection::_sendWelcome() {
    uint8_t buf[1 + 4 + 1] = {};
    size_t pos = 0;
    buf[pos++] = static_cast<uint8_t>(HandshakeMsgType::Welcome);
    pos += appendU32LE(buf + pos, _protocolVersion);
    buf[pos++] = 0;  // reasonCode = 0 (accept)
    auto wire = PacketCodec::encode(
        buf, pos,
        kMsgTypeHandshake, kSchemaVersion,
        CHANNEL_RELIABLE,
        /*flags=*/0,
        nowMs(),
        /*compress=*/false);
    (void)_rawSend(wire.data(), static_cast<uint32_t>(wire.size()), CHANNEL_RELIABLE);
    if (_profilerSendHook) {
        _profilerSendHook(profiler::handshakeExtrasKey(HandshakeMsgType::Welcome),
                          static_cast<uint64_t>(wire.size()));
    }
}

void GnsConnection::_sendReject(DisconnectReason reason) {
    uint8_t buf[1 + 1] = {};
    buf[0] = static_cast<uint8_t>(HandshakeMsgType::Reject);
    buf[1] = static_cast<uint8_t>(reason);
    auto wire = PacketCodec::encode(
        buf, sizeof(buf),
        kMsgTypeHandshake, kSchemaVersion,
        CHANNEL_RELIABLE,
        /*flags=*/0,
        nowMs(),
        /*compress=*/false);
    // R1 pitfall preserved: REJECT must reach the client before GNS tears
    // down. The seal just gives us a framed envelope; CloseConnection's
    // linger=true (in _handleHandshake's protocol-mismatch path) is what
    // guarantees the flush.
    (void)_rawSend(wire.data(), static_cast<uint32_t>(wire.size()), CHANNEL_RELIABLE);
    if (_profilerSendHook) {
        _profilerSendHook(profiler::handshakeExtrasKey(HandshakeMsgType::Reject),
                          static_cast<uint64_t>(wire.size()));
    }
}

void GnsConnection::_handleHandshake(const uint8_t* data, size_t len) {
    if (!data || len < 1) return;
    HandshakeMsgType type = static_cast<HandshakeMsgType>(data[0]);

    if (type == HandshakeMsgType::Hello) {
        // Server-side: validate, then WELCOME or REJECT.
        if (len < 1 + 4) {
            ::fprintf(stderr, "[GnsConnection] HELLO too short (%zu bytes)\n", len);
            return;
        }
        uint32_t peerVersion = 0;
        readU32LE(data + 1, peerVersion);
        if (peerVersion != _protocolVersion) {
            ::fprintf(stderr, "[GnsConnection] HELLO version mismatch (peer=%u mine=%u)\n",
                      peerVersion, _protocolVersion);
            _sendReject(DisconnectReason::ProtocolMismatch);
            _lastDisconnectReason = DisconnectReason::ProtocolMismatch;
            setState(GnsConnectionState::Disconnecting);
            if (s_gns && _conn != k_HSteamNetConnection_Invalid) {
                // bLinger=true so GNS flushes the REJECT before tearing down.
                // linger duration is bounded by enableLingerMessage for finer
                // control, but for R1 a simple linger works.
                s_gns->CloseConnection(_conn, 0, "protocol mismatch", true);
            }
            return;
        }
        size_t pos = 1 + 4;
        if (pos >= len) {
            _sendReject(DisconnectReason::ProtocolMismatch);
            _lastDisconnectReason = DisconnectReason::ProtocolMismatch;
            setState(GnsConnectionState::Disconnecting);
            if (s_gns && _conn != k_HSteamNetConnection_Invalid) {
                s_gns->CloseConnection(_conn, 0, "malformed hello", true);
            }
            return;
        }
        const size_t nameSize = data[pos++];
        if (nameSize > kHandshakeMaxNameLen || pos + nameSize + 2 > len) {
            _sendReject(DisconnectReason::ProtocolMismatch);
            _lastDisconnectReason = DisconnectReason::ProtocolMismatch;
            setState(GnsConnectionState::Disconnecting);
            if (s_gns && _conn != k_HSteamNetConnection_Invalid) {
                s_gns->CloseConnection(_conn, 0, "malformed hello", true);
            }
            return;
        }
        pos += nameSize;
        uint16_t admissionSize = 0;
        pos += readU16LE(data + pos, admissionSize);
        if (admissionSize > kConnectionAdmissionMaxBytes ||
            pos + admissionSize != len) {
            _sendReject(DisconnectReason::ProtocolMismatch);
            _lastDisconnectReason = DisconnectReason::ProtocolMismatch;
            setState(GnsConnectionState::Disconnecting);
            if (s_gns && _conn != k_HSteamNetConnection_Invalid) {
                s_gns->CloseConnection(_conn, 0, "malformed admission", true);
            }
            return;
        }
        bool admitted = true;
        if (_handshakeAdmissionValidator) {
            try {
                admitted = _handshakeAdmissionValidator(
                    admissionSize == 0 ? nullptr : data + pos,
                    admissionSize);
            } catch (...) {
                admitted = false;
            }
        }
        if (!admitted) {
            _sendReject(DisconnectReason::AdmissionRejected);
            _lastDisconnectReason = DisconnectReason::AdmissionRejected;
            setState(GnsConnectionState::Disconnecting);
            if (s_gns && _conn != k_HSteamNetConnection_Invalid) {
                s_gns->CloseConnection(_conn, 0, "admission rejected", true);
            }
            return;
        }
        _sendWelcome();
        // Server transitions to Ready after sending WELCOME.
        setState(GnsConnectionState::Ready);
        return;
    }

    if (type == HandshakeMsgType::Welcome) {
        // Client-side: peer accepted. Move Handshaking -> Ready.
        if (len < 1 + 4 + 1) {
            ::fprintf(stderr, "[GnsConnection] WELCOME too short (%zu bytes)\n", len);
            return;
        }
        uint32_t peerVersion = 0;
        readU32LE(data + 1, peerVersion);
        if (peerVersion != _protocolVersion) {
            ::fprintf(stderr, "[GnsConnection] WELCOME version mismatch\n");
            _lastDisconnectReason = DisconnectReason::ProtocolMismatch;
            setState(GnsConnectionState::Disconnecting);
            if (s_gns && _conn != k_HSteamNetConnection_Invalid) {
                s_gns->CloseConnection(_conn, 0, "welcome version mismatch", false);
            }
            return;
        }
        setState(GnsConnectionState::Ready);
        return;
    }

    if (type == HandshakeMsgType::Reject) {
        // Client-side: peer rejected. Capture reason and disconnect.
        if (len < 1 + 1) {
            ::fprintf(stderr, "[GnsConnection] REJECT too short (%zu bytes)\n", len);
            return;
        }
        uint8_t reasonCode = data[1];
        _lastDisconnectReason = static_cast<DisconnectReason>(reasonCode);
        setState(GnsConnectionState::Disconnecting);
        if (s_gns && _conn != k_HSteamNetConnection_Invalid) {
            s_gns->CloseConnection(_conn, 0, "rejected by peer", false);
        }
        {
            std::lock_guard<std::recursive_mutex> lock(registryMutex);
            connMap().erase(_conn);
        }
        _conn = k_HSteamNetConnection_Invalid;
        setState(GnsConnectionState::Disconnected);
        return;
    }

    // Unknown handshake type: forward as app data and let the caller complain.
    if (_dataHandler) {
        _dataHandler(data, len);
    }
}

void GnsConnection::onRawData(const uint8_t* data, size_t len) {
    if (!data || len == 0) return;

    // R2: every inbound frame goes through PacketCodec::decode first.
    // CRC failure or truncation -> drop. The legacy first-byte handshake
    // sniff is GONE — handshake frames are now identified by their
    // PacketHeader.msgType (kMsgTypeHandshake = 0xFFFF).
    DecodedPacket decoded = PacketCodec::decode(data, len);
    if (!decoded.ok) {
        // PacketCodec already logged the precise failure (CRC mismatch
        // / length mismatch). Drop silently here.
        return;
    }

    const PacketHeader& hdr = decoded.header;

    // Fragmented frames: body = [FragmentHeader 8B][chunk]. Feed to the
    // assembler; if a full payload is now ready, dispatch it via the
    // msgType path below.
    if (hasFlag(hdr.flags, PacketFlag::Fragmented)) {
        auto reassembled = _assembler.consume(decoded.body.data(),
                                              decoded.body.size());
        if (!reassembled) return;
        // Synthesize a "decoded" view where body is the full payload.
        // We don't re-seal/re-CRC; we just re-route through the same
        // dispatch by patching the body vector and clearing Fragmented.
        decoded.body = std::move(*reassembled);
        decoded.header.flags = static_cast<uint8_t>(
            decoded.header.flags & ~static_cast<uint8_t>(PacketFlag::Fragmented));
    }

    // R5.5 (2026-08-25): profiler hook — count decoded inbound bytes by
    // msgType (or by handshake sub-type for kMsgTypeHandshake). We
    // deliberately record BEFORE any downstream drop (CRC-decode-fail is
    // already filtered above; the AppAck / Handshake early-returns
    // below are still "received" so they count).
    if (_profilerRecvHook) {
        if (hdr.msgType == kMsgTypeHandshake && !decoded.body.empty()) {
            uint8_t sub = decoded.body[0];
            _profilerRecvHook(profiler::handshakeExtrasKey(
                                  static_cast<HandshakeMsgType>(sub)),
                              static_cast<uint64_t>(decoded.body.size()));
        } else {
            _profilerRecvHook(hdr.msgType,
                              static_cast<uint64_t>(decoded.body.size()));
        }
    }

    // Handshake path. The HandshakeMsgType byte lives at body[0] (R1 wire
    // format unchanged — only the envelope changed). Only valid during
    // Handshaking or Connected (server-side: just-accepted children fire
    // Connected state on GNS, then expect HELLO from the client).
    if (hdr.msgType == kMsgTypeAppAck) {
        uint32_t ackSeq = 0;
        if (AckPipeline::parseAckBody(decoded.body.data(), decoded.body.size(), ackSeq)) {
            _ackTracker.onAck(ackSeq);
        }
        return;
    }

    if (hasFlag(hdr.flags, PacketFlag::RequiresAck)) {
        uint32_t ackSeq = 0;
        if (AckPipeline::unwrapAckableBody(decoded.body, ackSeq)) {
            auto ackWire = AckPipeline::sealAck(ackSeq, nowMs());
            (void)_rawSend(ackWire.data(), static_cast<uint32_t>(ackWire.size()), CHANNEL_ACK);
            // R5.5 (2026-08-25): profiler hook — AppAck wire bytes
            // count toward the byMsgTypeExtras map keyed by msgType
            // (AppAck has its own slot entry, no handshake sub-encoding).
            if (_profilerSendHook) {
                _profilerSendHook(kMsgTypeAppAck,
                                  static_cast<uint64_t>(ackWire.size()));
            }
        } else {
            return;
        }
    }

    if (hdr.msgType == kMsgTypeHandshake) {
        if (_state == GnsConnectionState::Handshaking ||
            _state == GnsConnectionState::Connected) {
            _handleHandshake(decoded.body.data(), decoded.body.size());
        } else {
            ::fprintf(stderr,
                      "[GnsConnection] late handshake frame (%zu bytes) in state=%d, dropped\n",
                      decoded.body.size(), static_cast<int>(_state));
        }
        return;
    }

    // Application traffic — only forward once handshake is done (Ready) or
    // when handshake is disabled (Connected with version=0).
    if (_state == GnsConnectionState::Ready ||
        (_protocolVersion == 0 && _state == GnsConnectionState::Connected)) {
        if (_packetHandler) {
            _packetHandler(decoded.header, decoded.body.data(), decoded.body.size());
        } else if (_dataHandler) {
            _dataHandler(decoded.body.data(), decoded.body.size());
        }
    } else {
        // App data arriving before Ready (or after disconnect). Drop.
        ::fprintf(stderr,
                  "[GnsConnection] dropped %zu bytes app data (state=%d, msgType=0x%04x)\n",
                  decoded.body.size(), static_cast<int>(_state), hdr.msgType);
    }
}

} // namespace ayt::net
