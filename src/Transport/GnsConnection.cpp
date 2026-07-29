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
//   3. Per frame: instance.update() drains ReceiveMessagesOnPollGroup and
//      pumps RunCallbacks (status changes).
//   4. instance.disconnect(reason) — graceful close.
//   5. gns::shutdown() — call from NetworkSubSystem::shutdown().

#include <GnsConnection.h>
#include <IAYNetwork.h>                  // R1 done: HandshakeMsgType / DisconnectReason / kProtocolVersion
#include <PacketCodec.h>                  // R2: framing layer

#include <steam/steamclientpublic.h>     // EResult
#include <steam/steamnetworkingtypes.h>  // identity, connection info, send flags
#include <steam/isteamnetworkingutils.h> // SetGlobalCallback_SteamNetConnectionStatusChanged
#include <steam/isteamnetworkingsockets.h>
#include <steam/steamnetworkingsockets.h> // GameNetworkingSockets_Init / _Kill

#include <atomic>
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace ayt::net
{

// =============================================================================
// Static GNS state (process-wide) — public so the gns:: namespace helpers below
// can manipulate them. Encapsulated by being in this .cpp only.
// =============================================================================
ISteamNetworkingSockets* GnsConnection::s_gns       = nullptr;
HSteamNetPollGroup       GnsConnection::s_pollGroup = k_HSteamNetPollGroup_Invalid;
GnsConnection::AdoptFactory GnsConnection::s_adoptFactory = nullptr;

// Active connection map (HSteamNetConnection -> GnsConnection*).
static std::unordered_map<HSteamNetConnection, GnsConnection*>& connMap() {
    static std::unordered_map<HSteamNetConnection, GnsConnection*> m;
    return m;
}

// R1.A (2026-07-27): server-side adopters. When no s_adoptFactory is
// registered, the global status callback picks the first available
// server-side GnsConnection from this list. Protected by connMapMutex.
static std::vector<GnsConnection*>& serverAdopters() {
    static std::vector<GnsConnection*> v;
    return v;
}
// R1.5 (2026-07-27): recursive mutex. The status callback can recurse: an
// incoming connection state change fires while we're inside adoptIncomingConnection
// (which itself updates the map). A plain std::mutex deadlocks.
static std::recursive_mutex connMapMutex;

static void gns_status_callback(SteamNetConnectionStatusChangedCallback_t* info) {
    if (!info) return;

    // R1.A (2026-07-27): when an incoming connection arrives on a server
    // listen socket, GNS posts a Connecting state callback for the NEW
    // HSteamNetConnection. We delegate the adopt decision to a factory
    // registered by AYNetworkSubSystem (so GnsConnection stays decoupled
    // from the subsystem type). The factory returns a fresh GnsConnection*
    // that owns the new conn, or nullptr to reject.
    if (info->m_info.m_eState == k_ESteamNetworkingConnectionState_Connecting) {
        // Quick check whether this conn is already known (avoid factory call
        // for our own outgoing client connections, which also go through the
        // Connecting path).
        bool needsAdopt = false;
        {
            std::lock_guard<std::recursive_mutex> lk(connMapMutex);
            needsAdopt = (connMap().find(info->m_hConn) == connMap().end());
        }
        if (needsAdopt) {
            // Prefer a registered factory (subsystem mode). Otherwise fall
            // back to the first server-side GnsConnection in serverAdopters().
            if (GnsConnection::s_adoptFactory) {
                GnsConnection* child = GnsConnection::s_adoptFactory(info->m_hConn);
                (void)child;  // result stored by factory's adoptIncomingConnection
                return;
            }
            // R1.A fallback (no factory registered): the first available
            // server-side GnsConnection in serverAdopters() adopts the
            // incoming connection. Used by tests that use GnsConnection
            // directly without going through the subsystem. Real
            // applications should always register a factory.
            if (GnsConnection::s_gns) {
                EResult r = GnsConnection::s_gns->AcceptConnection(info->m_hConn);
                if (r != k_EResultOK) {
                    ::fprintf(stderr, "[GnsConnection] AcceptConnection failed: %d\n", r);
                    return;
                }
            }
            std::lock_guard<std::recursive_mutex> lk(connMapMutex);
            auto& adopters = serverAdopters();
            if (!adopters.empty()) {
                GnsConnection* adopter = adopters.front();
                adopters.erase(adopters.begin());
                adopter->adoptIncomingConnection(info->m_hConn);
                return;
            }
            ::fprintf(stderr, "[GnsConnection] incoming conn %u but no server-side adopter\n",
                      info->m_hConn);
            return;
        }
    }

    std::lock_guard<std::recursive_mutex> lk(connMapMutex);
    auto it = connMap().find(info->m_hConn);
    if (it == connMap().end()) {
        ::fprintf(stderr, "[GnsConnection] status change for unregistered conn %u (state %d -> %d)\n",
                  info->m_hConn, info->m_eOldState, info->m_info.m_eState);
        return;
    }
    it->second->handleStatusChange(info->m_eOldState, info->m_info.m_eState);
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
        if (g_initRefCount.fetch_sub(1) != 1) {
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
        std::lock_guard<std::recursive_mutex> lk(connMapMutex);
        connMap()[_conn] = this;
    }

    setState(GnsConnectionState::Connecting);
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

    // R1.A (2026-07-27): the listen socket itself doesn't carry a
    // HSteamNetConnection, so it isn't registered in connMap. But the
    // GnsConnection still needs to be reachable from the global status
    // callback when an incoming connection arrives, so we register it in
    // serverAdopters(). This is the no-factory fallback path; when a
    // factory is registered (subsystem mode), it owns the routing and
    // serverAdopters is unused.
    {
        std::lock_guard<std::recursive_mutex> lk(connMapMutex);
        serverAdopters().push_back(this);
    }

    _port = virtualPort;
    setState(GnsConnectionState::Connected);
}

void GnsConnection::update() {
    if (!s_gns) return;
    if (_state == GnsConnectionState::Disconnected) return;

    _ackTracker.expire();

    // RunCallbacks triggers any pending status-change callbacks. Calling this
    // every frame is the canonical GNS pattern — it's cheap (just drains a
    // queue).
    s_gns->RunCallbacks();

    // Drain the shared poll group. R3 will switch to per-connection drains
    // once we have many simultaneous connections.
    constexpr int kMaxMessages = 32;
    SteamNetworkingMessage_t* msgs[kMaxMessages];
    int n = s_gns->ReceiveMessagesOnPollGroup(s_pollGroup, msgs, kMaxMessages);
    if (n < 0) {
        // Negative return means an error; GNS doesn't expose details.
        ::fprintf(stderr, "[GnsConnection] ReceiveMessagesOnPollGroup returned %d\n", n);
        return;
    }
    for (int i = 0; i < n; ++i) {
        HSteamNetConnection msgConn = msgs[i]->m_conn;
        GnsConnection* owner = nullptr;
        {
            std::lock_guard<std::recursive_mutex> lk(connMapMutex);
            auto it = connMap().find(msgConn);
            if (it != connMap().end()) owner = it->second;
        }
        if (owner && msgs[i]->m_pData && msgs[i]->m_cbSize > 0) {
            // R1 done: route through onRawData so handshake frames can be
            // intercepted before reaching the user's data handler. onRawData
            // itself forwards non-handshake frames to _dataHandler.
            owner->onRawData(static_cast<const uint8_t*>(msgs[i]->m_pData),
                             static_cast<size_t>(msgs[i]->m_cbSize));
        }
        msgs[i]->Release();
    }
}

int GnsConnection::send(uint8_t channel, const void* data, size_t len) {
    if (!s_gns || _conn == k_HSteamNetConnection_Invalid) return -1;
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
    if (len <= kFrameMtu - PacketCodec::kHeaderSize - PacketCodec::kCrcSize) {
        // Single frame, no Fragmented flag.
        auto wire = PacketCodec::encode(
            static_cast<const uint8_t*>(data), len,
            kMsgTypeApp, kSchemaVersion,
            channel,
            /*flags=*/0,
            tsMs,
            /*compress=*/false);
        return _rawSend(wire.data(), static_cast<uint32_t>(wire.size()), channel);
    }

    // Multi-frame path. R2 does not auto-compress — only fragment.
    auto frames = PacketAssembler::fragment(
        static_cast<const uint8_t*>(data), len,
        static_cast<uint32_t>(kFrameMtu),
        kMsgTypeApp, kSchemaVersion,
        channel, tsMs, ++_nextFragmentId);
    if (frames.empty()) {
        ::fprintf(stderr, "[GnsConnection] send: fragment() returned empty (MTU too small?)\n");
        return -1;
    }
    int lastResult = 0;
    for (const auto& f : frames) {
        lastResult = _rawSend(f.data(), static_cast<uint32_t>(f.size()), channel);
        if (lastResult != 0) break;
    }
    return lastResult;
}

int GnsConnection::sendRequireAck(uint16_t msgType, uint8_t channel,
                                  const void* data, size_t len,
                                  AckTracker::Callback onAck) {
    if (!s_gns || _conn == k_HSteamNetConnection_Invalid) return -1;
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
    return _rawSend(wire.data(), static_cast<uint32_t>(wire.size()), channel);
}

// R2: low-level GNS send. R4.0 (2026-07-29) expands the channel -> GNS
// send-flag map to all 4 channels declared in IAYNetwork.h:39-42.
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
uint32_t GnsConnection::nowMs() {
    using namespace std::chrono;
    return static_cast<uint32_t>(
        duration_cast<milliseconds>(
            steady_clock::now().time_since_epoch()).count() & 0xFFFFFFFFu);
}

void GnsConnection::disconnect(const char* reason) {
    if (!s_gns) return;
    if (_state == GnsConnectionState::Disconnected) return;

    setState(GnsConnectionState::Disconnecting);

    if (_conn != k_HSteamNetConnection_Invalid) {
        {
            std::lock_guard<std::recursive_mutex> lk(connMapMutex);
            connMap().erase(_conn);
        }
        // R1 done: local-initiated disconnect reports UserQuit. R2 will
        // add a richer API to specify the reason.
        if (_lastDisconnectReason == DisconnectReason::Unknown) {
            _lastDisconnectReason = DisconnectReason::UserQuit;
        }
        // linger=false: drop immediately. R2 will switch to graceful close.
        s_gns->CloseConnection(_conn, 0, reason, false);
        _conn = k_HSteamNetConnection_Invalid;
    }
    if (_listen != k_HSteamListenSocket_Invalid) {
        s_gns->CloseListenSocket(_listen);
        _listen = k_HSteamListenSocket_Invalid;
        // R1.A: remove from serverAdopters so a stale server doesn't try
        // to adopt incoming conns after disconnect.
        std::lock_guard<std::recursive_mutex> lk(connMapMutex);
        auto& adopters = serverAdopters();
        adopters.erase(std::remove(adopters.begin(), adopters.end(), this), adopters.end());
        _lastDisconnectReason = DisconnectReason::HostShutdown;
    }
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
    if (s_gns && s_pollGroup != k_HSteamNetPollGroup_Invalid) {
        s_gns->SetConnectionPollGroup(_conn, s_pollGroup);
    }
    {
        std::lock_guard<std::recursive_mutex> lk(connMapMutex);
        connMap()[_conn] = this;
    }
    setState(GnsConnectionState::Connected);
}

void GnsConnection::setState(GnsConnectionState newState) {
    if (_state == newState) return;
    GnsConnectionState old = _state;
    _state = newState;
    if (_stateHandler) {
        _stateHandler(old, newState);
    }
}

void GnsConnection::handleStatusChange(int /*oldGnsState*/, int newGnsState) {
    switch (newGnsState) {
        case k_ESteamNetworkingConnectionState_Connected: {
            // R1 done: branch on protocol version. Legacy (version=0) goes
            // straight to Connected (which == Ready under isConnected()).
            // With handshake enabled, transition to Handshaking and the
            // client immediately sends HELLO; the server waits for HELLO
            // before responding with WELCOME.
            if (_protocolVersion != 0 && _listen == k_HSteamListenSocket_Invalid) {
                // Client side (or server-side adopted child): send HELLO.
                _sendHello();
                setState(GnsConnectionState::Handshaking);
            } else if (_protocolVersion != 0 && _listen != k_HSteamListenSocket_Invalid) {
                // Server-side parent listen socket itself — irrelevant, the
                // child GnsConnections each have their own _listen==0.
                setState(GnsConnectionState::Connected);
            } else {
                setState(GnsConnectionState::Connected);
            }
            break;
        }
        case k_ESteamNetworkingConnectionState_ClosedByPeer:
        case k_ESteamNetworkingConnectionState_ProblemDetectedLocally: {
            // R1 done: capture the reason into _lastDisconnectReason so the
            // state-change callback / subsystem can surface it.
            // GNS doesn't carry a structured reason; we infer:
            //   ProblemDetectedLocally -> ConnectionLost
            //   ClosedByPeer            -> ConnectionLost (peer walked away)
            // The wire-encoded reason (REJECT during handshake) is captured
            // separately in _handleHandshake() and overrides this.
            _lastDisconnectReason = DisconnectReason::ConnectionLost;
            setState(GnsConnectionState::Disconnecting);
            if (s_gns && _conn != k_HSteamNetConnection_Invalid) {
                s_gns->CloseConnection(_conn, 0, "peer closed", false);
            }
            {
                std::lock_guard<std::recursive_mutex> lk(connMapMutex);
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
// Wire format (host byte order, little-endian on x64):
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

// Read a little-endian uint32 from buf; returns bytes consumed.
static size_t readU32LE(const uint8_t* buf, uint32_t& out) {
    out =  static_cast<uint32_t>(buf[0])
        | (static_cast<uint32_t>(buf[1]) << 8)
        | (static_cast<uint32_t>(buf[2]) << 16)
        | (static_cast<uint32_t>(buf[3]) << 24);
    return 4;
}

void GnsConnection::_sendHello() {
    // R2: build the same R1 body bytes (HandshakeMsgType=Hello, version,
    // name) then seal via PacketCodec with msgType=kMsgTypeHandshake so
    // onRawData can demux by msgType instead of sniffing the first byte.
    uint8_t buf[1 + 4 + 1 + kHandshakeMaxNameLen] = {};
    size_t pos = 0;
    buf[pos++] = static_cast<uint8_t>(HandshakeMsgType::Hello);
    pos += appendU32LE(buf + pos, _protocolVersion);
    std::string name = _address;
    if (name.size() > kHandshakeMaxNameLen) name.resize(kHandshakeMaxNameLen);
    buf[pos++] = static_cast<uint8_t>(name.size());
    if (!name.empty()) {
        std::memcpy(buf + pos, name.data(), name.size());
        pos += name.size();
    }
    auto wire = PacketCodec::encode(
        buf, pos,
        kMsgTypeHandshake, kSchemaVersion,
        CHANNEL_RELIABLE,
        /*flags=*/0,
        nowMs(),
        /*compress=*/false);
    (void)_rawSend(wire.data(), static_cast<uint32_t>(wire.size()), CHANNEL_RELIABLE);
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
        if (_dataHandler) {
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