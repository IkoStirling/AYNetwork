#pragma once
// AYNetwork/Transport/AYNetwork/Transport/AYNetwork/Transport/GnsConnection.h - Thin wrapper around ISteamNetworkingSockets.
//
// R1 (2026-07-26): replaces the stub KcpConnection that AYConnection.cpp used.
// R1.A (2026-07-27): multi-connection support. Server-side GnsConnection
//                     instances may now represent either a parent (listen
//                     socket + N accepted children) or a child (single
//                     accepted client connection, no listen socket).
//                     The global status callback routes incoming connections
//                     through a per-listener factory registered by the subsystem,
//                     keeping GnsConnection unaware of the subsystem type.
//
// This wrapper deliberately exposes a tiny API surface — the rest of AYNetwork
// shouldn't know about Steam datagram structs. The SteamSDK identifiers
// (HSteamListenSocket, HSteamNetConnection, HSteamNetPollGroup) are kept
// opaque inside this translation unit so swapping transports later (e.g. ENet)
// only touches GnsConnection.cpp.
//
// Lifecycle:
//   initClient(addr, port)  -> connects via ConnectByIPAddress
//   initServer(port)        -> CreateListenSocketIP (parent)
//   pump()                  -> bounded shared callback/message dispatch
//   disconnect(reason)      -> CloseConnection
//
// Status callbacks (onStateChange, onData) are set via setHandlers() before
// update() is first called.

#include <AYCore.h>
#include <AYNetwork/INetwork.h>                // R1 done: DisconnectReason / HandshakeMsgType / kProtocolVersion
#include <AYNetwork/P2P.h>
#include <AYNetwork/Protocol/PacketAssembler.h>           // R2: receive-side reassembly
#include <AYNetwork/Protocol/AckPipeline.h>      // R4.1-B: CHANNEL_ACK pipeline
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

// Forward-decl GNS types so we don't pollute global namespace with the Steam
// headers. The full definitions are only pulled into GnsConnection.cpp.
struct ISteamNetworkingSockets;
typedef uint32_t HSteamListenSocket;
typedef uint32_t HSteamNetConnection;
typedef uint32_t HSteamNetPollGroup;

namespace ayt::net
{

// R5.4 (2026-08-25): fault-injection forward declarations. The full
// definitions live in src/Transport/. We need pointers in the header so
// the per-`GnsConnection` interceptor field can be a `unique_ptr`.
class TransportFaultController;
class TransportFaultInterceptor;

// =============================================================================
// GnsConnection state (mirrors AYConnection::ConnectionState)
// =============================================================================
enum class GnsConnectionState : uint8_t {
    Disconnected,
    Connecting,    // ConnectByIPAddress issued, awaiting FindingConnection / Connected
    Connected,     // GNS Connected, but handshake NOT yet completed
    Handshaking,   // GNS Connected, handshake in flight
    Ready,         // Handshake completed; full bidirectional app traffic allowed
    Disconnecting  // CloseConnection issued, awaiting Close
};

enum class GnsConnectionRole : uint8_t {
    None,
    Client,
    Listener,
    AcceptedServerPeer,
};

struct GnsPumpBudget {
    uint32_t maxMessages = 512;
    uint32_t maxBytes = 2u * 1024u * 1024u;
};

struct GnsPumpResult {
    uint32_t messages = 0;
    uint32_t bytes = 0;
    bool budgetExhausted = false;
};

// =============================================================================
// GnsConnection - one endpoint (client OR server-side listen socket OR
// server-side accepted client).
//
// A single GnsConnection owns at most ONE of:
//   - An HSteamNetConnection (client mode, post-ConnectByIPAddress),
//   - An HSteamListenSocket (server parent mode, post-CreateListenSocketIP),
//   - An HSteamNetConnection inherited from a server-side accept
//     (server child mode — no listen socket, just an adopted conn),
//   - Nothing (Disconnected).
//
// R1.A (2026-07-27): server mode with multiple clients is modelled as:
//   - One "server parent" GnsConnection that owns the listen socket.
//   - N "server child" GnsConnection instances, each owning exactly one
//     accepted client conn. AYNetworkSubSystem holds these children in a
//     vector and pumps them every frame.
//   The global status callback routes incoming connections through a
//   factory registered for the exact listener so
//   GnsConnection itself stays decoupled from the subsystem type.
// =============================================================================
class GnsConnection {
public:
    using StateHandler = std::function<void(GnsConnectionState oldState, GnsConnectionState newState)>;
    using DataHandler  = std::function<void(const uint8_t* data, size_t len)>;
    using PacketHandler = std::function<void(const PacketHeader& header, const uint8_t* body, size_t len)>;

    // R1.A: factory invoked by the global status callback when an incoming
    // connection arrives on a server listen socket. Returns a fresh
    // GnsConnection* the subsystem will own (must outlive the conn). The
    // factory is responsible for registering the connection with s_gns
    // (AcceptConnection + SetConnectionPollGroup) and adopting the handle.
    // Returning nullptr means "reject the incoming connection".
    using AdoptFactory = std::function<GnsConnection*(HSteamNetConnection incomingConn)>;
    using P2PAdoptFactory =
        std::function<GnsConnection*(HSteamNetConnection incomingConn,
                                     const PeerId& remotePeer)>;
    // Register per-listener rather than process-global routing.  This permits
    // multiple worlds/listeners in one process without last-writer-wins
    // callback corruption.
    static void setAdoptFactory(HSteamListenSocket listener, AdoptFactory factory);
    static void clearAdoptFactory(HSteamListenSocket listener);

    // Custom-signaling P2P routes are process-wide because standalone GNS
    // exposes one interface/identity per process. A normal game process owns
    // one local PeerId and may listen on several virtual ports.
    static bool setLocalP2PIdentity(const PeerId& localPeer);
    static void setP2PAdoptFactory(uint16_t virtualPort,
                                   std::shared_ptr<ISignalingTransport> signaling,
                                   P2PAdoptFactory factory);
    static void clearP2PAdoptFactory(uint16_t virtualPort);
    static bool receiveP2PSignal(const PeerId& sender,
                                 const void* data, size_t size,
                                 std::shared_ptr<ISignalingTransport> signaling);

    GnsConnection();
    ~GnsConnection();

    // Non-copyable: holds opaque GNS handles that don't survive copy.
    GnsConnection(const GnsConnection&) = delete;
    GnsConnection& operator=(const GnsConnection&) = delete;

    // ===== Init =====
    // initClient: starts async connect to (address, virtualPort).
    //   `virtualPort` is the GNS P2P port concept (not TCP/UDP port).
    void initClient(const char* address, uint16_t virtualPort);

    // initServer: starts listening for incoming IP connections on virtualPort.
    //   Subsequent incoming connections are handed off via setAdoptFactory().
    void initServer(uint16_t virtualPort);

    // Starts a GNS custom-signaling P2P connection. Returns false when the
    // identity/config/signaling channel is invalid or GNS rejects creation.
    bool initP2PClient(const PeerId& remotePeer,
                       const P2PConfig& config,
                       std::shared_ptr<ISignalingTransport> signaling);

    // R1.A: attach to an already-accepted GNS connection (server child path).
    // Used by the adopt factory: it AcceptConnection()s and SetConnectionPollGroup()s
    // the incoming conn, then calls this to bind it to the new GnsConnection.
    void adoptIncomingConnection(HSteamNetConnection conn);
    void adoptIncomingP2PConnection(HSteamNetConnection conn,
                                    const PeerId& remotePeer,
                                    uint16_t virtualPort);

    // ===== Per-frame =====
    // Production path: pump the shared GNS callback/receive queue exactly once
    // per network ingress and route messages by HSteamNetConnection.
    static GnsPumpResult pump(const GnsPumpBudget& budget = {});
    static bool isPumping();

    // Compatibility adapter for direct GnsConnection tests/tools.  Subsystems
    // must use pump() once instead of invoking update() on every connection.
    void update();

    // ===== Send / Close =====
    // channel: 0..3 (CHANNEL_RELIABLE/UNRELIABLE/FRAGMENTED/ACK)
    int  send(uint8_t channel, const void* data, size_t len);
    int  sendEncoded(uint8_t channel, const void* data, size_t len);
    // R4.1-B: seal with RequiresAck + seq prefix; optional callback when
    // the peer echoes kMsgTypeAppAck on CHANNEL_ACK.
    int  sendRequireAck(uint16_t msgType, uint8_t channel, const void* data, size_t len,
                        AckTracker::Callback onAck = nullptr);
    void disconnect(const char* reason = nullptr);

    // ===== State / info =====
    GnsConnectionState getState() const { return _state; }
    GnsConnectionRole  getRole() const { return _role; }
    bool               isConnected() const {
        // "Connected" means app can use the link. R1 done: with handshake
        // enabled (setProtocolVersion > 0), this is Ready. With handshake
        // disabled (legacy / version 0), GNS Connected == Ready.
        if (_protocolVersion == 0) {
            return _state == GnsConnectionState::Connected;
        }
        return _state == GnsConnectionState::Ready;
    }
    int  getPing() const;

    const char* getAddress() const { return _address.c_str(); }
    uint16_t    getPort() const { return _port; }
    bool        isP2P() const { return _isP2P; }
    const PeerId& getRemotePeerId() const { return _remotePeerId; }
    P2PConnectionInfo getP2PConnectionInfo(const PeerId& localPeer) const;

    // R1 done (2026-07-27): set the protocol version this endpoint
    // expects/announces. 0 disables the handshake (legacy loopback).
    // Must be called BEFORE initClient/initServer/adoptIncomingConnection
    // to take effect. Default = 0 (handshake skipped).
    void setProtocolVersion(uint32_t version) { _protocolVersion = version; }
    uint32_t getProtocolVersion() const { return _protocolVersion; }

    // R5.4 (2026-08-25): attach the fault controller. Called once by
    // NetworkSubSystem at startup. The interceptor is created lazily on
    // the first send/recv tick if the controller has a profile for this
    // netId. Safe to call with nullptr to detach (interceptor is
    // destroyed).
    void attachFaultController(TransportFaultController* ctl);
    TransportFaultController* getFaultController() const { return _faultCtl; }
    TransportFaultInterceptor* getFaultInterceptor(); // lazy-creates

    // R1 done: reason for the last disconnect (Unknown if none / not yet).
    // After a peer-initiated disconnect this carries the wire-encoded
    // DisconnectReason; for local-initiated disconnect it's Unknown.
    DisconnectReason getLastDisconnectReason() const { return _lastDisconnectReason; }

    // ===== Callback registration =====
    void onStateChange(StateHandler handler) { _stateHandler = std::move(handler); }
    void onData(DataHandler handler)         { _dataHandler  = std::move(handler); }
    void onPacket(PacketHandler handler)     { _packetHandler = std::move(handler); }

    // ===== Opaque GNS handles (for NetworkSubSystem bookkeeping) =====
    HSteamNetConnection getInnerConnection() const { return _conn; }
    HSteamListenSocket  getInnerListenSocket() const { return _listen; }

    // ===== Internal access for the file-local gns:: helpers in GnsConnection.cpp =====
    // Public so the gns_status_callback trampoline and the gns::init/shutdown
    // helpers can route through them. The symbols themselves are TU-private
    // (defined in GnsConnection.cpp only), so external code can't link them.
    static ISteamNetworkingSockets* s_gns;
    static HSteamNetPollGroup       s_pollGroup;

    // GNS state -> our state mapping. Called from gns_status_callback.
    void handleStatusChange(int oldGnsState, int newGnsState);

    // R1 done: receive-side handshake parser. Called from update() when
    // data arrives on a Handshaking/Connected conn. If the bytes look
    // like a Hello/Welcome/Reject, dispatches into _handleHandshake().
    // Otherwise forwards to the user data handler.
    void onRawData(const uint8_t* data, size_t len);

    // R2: low-overhead current-time helper used by PacketCodec::encode
    // AND by R6 callers (PacketAssembler::consume TTL clock,
    // NetworkTime, etc.) that need a logically-deterministic timestamp.
    // Public so the seam can drive every monotonic-time call site.
    static uint32_t nowMs();

    // R6 (2026-08-25): clock seam for determinism tests.
    //
    // nowMs() consults this optional override first. When non-null, the
    // override's returned microseconds are downshifted to a uint32 ms
    // timestamp so PacketHeader::timestampMs is stable across runs that
    // install the same override. Default null = production wall clock.
    //
    // Production code never sets this. Only tests use it; install/clear
    // happens in test setup/teardown, paired with clearNowOverride().
    //
    // The tick-rate convenience helper stamps logical time as
    // (serverTick * 1'000'000) / tickRate microseconds — equivalent to a
    // monotonic "logical now" derived from the server tick counter.
    using NowOverrideFn = std::function<uint64_t()>;
    static void setNowOverrideForTesting(NowOverrideFn fn);
    static void clearNowOverride();
    static void setNowOverrideForTickRate(uint32_t serverTick, uint32_t tickRate);
    static NowOverrideFn s_nowOverride;

    // Test seams for AckPipeline integration.
    size_t pendingAckCountForTesting() const { return _ackTracker.pendingCount(); }

    // R5.5 (2026-08-25): profiler hooks. pendingFragmentBytesForTesting is
    // the AYNetwork-side reassembly queue (PacketAssembler::pendingBytes);
    // getLastPumpBytes is the inbound byte count from the most recent
    // pump() iteration, cached so the profiler snapshot doesn't have to
    // re-run GNS. Cached values are refreshed by the next pump().
    uint32_t pendingFragmentBytesForTesting() const { return static_cast<uint32_t>(_assembler.pendingBytes()); }
    uint32_t getLastPumpBytes() const { return _lastPumpBytes; }

    // ===== R5.4 test seam: FakeTransport =====
    // Mirrors `ReplicationManager::setBroadcastSinkForTesting` (INetwork.h).
    // When a non-null `FakeTransportSender` is installed, `_rawSend` calls
    // the fake instead of `s_gns->SendMessageToConnection`. When a non-null
    // `FakeTransportReceiver` is installed, the receive pump calls the fake
    // receiver with each incoming message's raw payload, INSTEAD of running
    // `onRawData` (i.e. bypassing PacketCodec::decode → _packetHandler).
    // Pass nullptr to clear either side. Default (both null) = real GNS
    // transport. Either fake can be installed independently.
    //
    // The fake receiver IS expected to call back into `onRawData(data, len)`
    // if the test wants to exercise the decode path — the seam exists so
    // tests can interpose fault injectors without spinning up GNS.
    using FakeTransportSender = std::function<bool(const uint8_t* data, size_t len,
                                                   uint8_t channel)>;
    using FakeTransportReceiver = std::function<void(const uint8_t* data, size_t len,
                                                     uint8_t channel)>;
    void setFakeTransportSender(FakeTransportSender s)   { _fakeSender   = std::move(s); }
    void setFakeTransportReceiver(FakeTransportReceiver r) { _fakeReceiver = std::move(r); }
    bool hasFakeTransportSender() const   { return static_cast<bool>(_fakeSender); }
    bool hasFakeTransportReceiver() const { return static_cast<bool>(_fakeReceiver); }

    // R5.4 (2026-08-25): AYNetwork netId for this connection (assigned by
    // NetworkSubSystem when a GnsConnection is created / adopted). Used as
    // the fault-profile lookup key. 0 = unassigned (legacy connections).
    void    setNetId(uint32_t netId) { _netId = netId; }
    uint32_t getNetId() const        { return _netId; }

    // R5.5 (2026-08-25): profiler hooks. Installed by the subsystem at
    // construction time. The transport layer doesn't know about
    // ProfilerRegistry directly — it just calls these functions with
    // (msgType, bytes) after each _rawSend / onRawData that goes through
    // the transport layer's sealed-bytes seam. The subsystem's lambda
    // forwards to its ProfilerRegistry::recordSend/recordRecv(...).
    // Default null = no-op (cheap test paths).
    using ProfilerSendHook = std::function<void(uint16_t msgType, uint64_t bytes)>;
    using ProfilerRecvHook = std::function<void(uint16_t msgType, uint64_t bytes)>;
    void setProfilerSendHook(ProfilerSendHook hook) { _profilerSendHook = std::move(hook); }
    void setProfilerRecvHook(ProfilerRecvHook hook) { _profilerRecvHook = std::move(hook); }

private:
    void setState(GnsConnectionState newState);
    void runMaintenance(uint32_t monotonicNowMs);

    // R1 done: handshake state machine helpers.
    void _sendHello();
    void _sendWelcome();
    void _sendReject(DisconnectReason reason);
    void _handleHandshake(const uint8_t* data, size_t len);
    bool _isHandshaking() const { return _state == GnsConnectionState::Handshaking; }

    GnsConnectionState _state = GnsConnectionState::Disconnected;
    GnsConnectionRole  _role = GnsConnectionRole::None;

    std::string _address;
    uint16_t    _port = 0;
    bool        _isP2P = false;
    PeerId      _remotePeerId;

    HSteamListenSocket  _listen = 0;
    HSteamNetConnection _conn   = 0;

    StateHandler _stateHandler;
    DataHandler  _dataHandler;
    PacketHandler _packetHandler;

    // R1 done: 0 = no handshake (legacy). Non-zero enables HELLO/WELCOME.
    uint32_t _protocolVersion = 0;
    DisconnectReason _lastDisconnectReason = DisconnectReason::Unknown;

    // R2: per-connection reassembly state. Fragments are routed through
    // _assembler.consume() and the optional result is dispatched.
    PacketAssembler _assembler;
    // R6 C8 M-14 (2026-08-25): was uint32_t; pump() iterates owners and
    // send-path may run from multiple places. Atomic with relaxed memory
    // order is sufficient — pump()'s mutex provides the cross-thread
    // ordering for the high-level send path; fragment id only needs to
    // be unique within a connection's outgoing stream, not globally
    // ordered.
    std::atomic<uint32_t> _nextFragmentId{0};
    AckTracker _ackTracker;

    // R2: convenience for fragments that exceed MTU. ~1200 bytes fits
    // comfortably under typical internet MTUs (1500 minus IP+UDP+GNS
    // overhead).
    static constexpr size_t kFrameMtu = 1200;

    // R2: low-level GNS send wrapper. Called by send() and the handshake
    // helpers; maps channel -> GNS Reliable/Unreliable and reports EResult.
    int _rawSend(const uint8_t* data, uint32_t len, uint8_t channel);

    // R5.4 (2026-08-25): AYNetwork netId for fault-profile lookup. 0 =
    // unassigned. Set by NetworkSubSystem when the GnsConnection is
    // created / adopted.
    uint32_t _netId = 0;

    // R5.4 (2026-08-25): fault injection. `attachFaultController` is
    // called by NetworkSubSystem once at startup; the interceptor itself
    // is created on first use (and re-created if the netId changes).
    TransportFaultController* _faultCtl = nullptr;
    std::unique_ptr<TransportFaultInterceptor> _faultInterceptor;

    // R5.4 (2026-08-25): test seams (FakeTransport). When set, they
    // short-circuit _rawSend and the receive pump respectively.
    FakeTransportSender   _fakeSender;
    FakeTransportReceiver _fakeReceiver;

    // R5.5 (2026-08-25): inbound byte count cached from the most recent
    // pump() iteration. Refreshed by pump(); read by getLastPumpBytes().
    uint32_t _lastPumpBytes = 0;

    // R5.5 (2026-08-25): profiler hooks installed by AYNetworkSubSystem.
    ProfilerSendHook _profilerSendHook;
    ProfilerRecvHook _profilerRecvHook;
};

// =============================================================================
// GnsInit / GnsShutdown - module-level GNS lifecycle.
// =============================================================================
// Called once from NetworkSubSystem::initialize() and ::shutdown(). Subsequent
// calls are no-ops (ref-counted).
namespace gns
{
    bool init();
    void shutdown();
}

} // namespace ayt::net
