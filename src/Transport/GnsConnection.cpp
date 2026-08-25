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

#include <atomic>
#include <algorithm>
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

static std::recursive_mutex registryMutex;
static std::mutex pumpMutex;
static thread_local bool insidePump = false;

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
    owner->handleStatusChange(info->m_eOldState, info->m_info.m_eState);
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
        std::vector<std::pair<std::vector<uint8_t>, uint8_t>> sendOut;
        for (GnsConnection* owner : owners) {
            if (!owner->_faultCtl) continue;
            TransportFaultInterceptor* ic = owner->getFaultInterceptor();
            if (!ic || !ic->isEnabled()) continue;  // R5.4: skip if no profile
            std::vector<std::pair<std::vector<uint8_t>, uint8_t>> recvTmp;
            // dtSeconds for the rate-limit refill — the caller doesn't
            // supply one so we use a small constant (1ms). Tests that
            // care about exact refill behavior inject virtual time.
            ic->tick(maintenanceNowMs, 0.001, sendOut, recvTmp);
            (void)recvTmp;
        }
        for (auto& [bytes, channel] : sendOut) {
            // Find the owner that owns the frame — we don't have it
            // tagged here, so route via the global connMap() to the
            // single owner with a non-null controller for the bytes'
            // channel. For R5.4 we round-robin through owners and let
            // _rawSend handle it; each interceptor only owns one
            // connection's queue.
            for (GnsConnection* owner : owners) {
                if (owner->_faultCtl) {
                    owner->_rawSend(bytes.data(),
                                    static_cast<uint32_t>(bytes.size()),
                                    channel);
                    break;
                }
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
        std::vector<std::pair<std::vector<uint8_t>, uint8_t>> recvOut;
        for (GnsConnection* owner : owners) {
            if (!owner->_faultCtl) continue;
            TransportFaultInterceptor* ic = owner->getFaultInterceptor();
            if (!ic || !ic->isEnabled()) continue;  // R5.4: skip if no profile
            std::vector<std::pair<std::vector<uint8_t>, uint8_t>> sendTmp;
            ic->tick(maintenanceNowMs, 0.001, sendTmp, recvOut);
            (void)sendTmp;
        }
        for (auto& [bytes, channel] : recvOut) {
            (void)channel;
            // Find the right owner — the interceptor holds the recv
            // queue keyed by netId, and we lost that mapping when
            // tick() returned. Re-derive by scanning owners with a
            // non-null controller. For R5.4 (per-connection profiles)
            // this is a single match in practice.
            for (GnsConnection* owner : owners) {
                if (owner->_faultCtl) {
                    owner->onRawData(bytes.data(), bytes.size());
                    break;
                }
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
        len > UINT32_MAX ||
        (_state != GnsConnectionState::Connected &&
         _state != GnsConnectionState::Handshaking &&
         _state != GnsConnectionState::Ready)) return -1;
    const auto* wire = static_cast<const uint8_t*>(data);
    if (len <= kFrameMtu) {
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
        lastResult = _rawSend(frame.data(), static_cast<uint32_t>(frame.size()), channel);
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

void GnsConnection::disconnect(const char* reason) {
    if (!s_gns) return;
    if (_state == GnsConnectionState::Disconnected) return;

    setState(GnsConnectionState::Disconnecting);

    if (_conn != k_HSteamNetConnection_Invalid) {
        {
            std::lock_guard<std::recursive_mutex> lk(registryMutex);
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
