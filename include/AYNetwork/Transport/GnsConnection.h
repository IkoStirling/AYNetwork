#pragma once
// AYNetwork/Transport/AYNetwork/Transport/AYNetwork/Transport/GnsConnection.h - Thin wrapper around ISteamNetworkingSockets.
//
// R1 (2026-07-26): replaces the stub KcpConnection that AYConnection.cpp used.
// R1.A (2026-07-27): multi-connection support. Server-side GnsConnection
//                     instances may now represent either a parent (listen
//                     socket + N accepted children) or a child (single
//                     accepted client connection, no listen socket).
//                     The global status callback routes incoming connections
//                     through a static factory registered by the subsystem,
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
//   update()                -> poll ReceiveMessages / dispatch state changes
//   disconnect(reason)      -> CloseConnection
//
// Status callbacks (onStateChange, onData) are set via setHandlers() before
// update() is first called.

#include <AYCore.h>
#include <AYNetwork/INetwork.h>                // R1 done: DisconnectReason / HandshakeMsgType / kProtocolVersion
#include <AYNetwork/Protocol/PacketAssembler.h>           // R2: receive-side reassembly
#include <AYNetwork/Protocol/AckPipeline.h>      // R4.1-B: CHANNEL_ACK pipeline
#include <cstdint>
#include <functional>
#include <string>

// Forward-decl GNS types so we don't pollute global namespace with the Steam
// headers. The full definitions are only pulled into GnsConnection.cpp.
struct ISteamNetworkingSockets;
typedef uint32_t HSteamListenSocket;
typedef uint32_t HSteamNetConnection;
typedef uint32_t HSteamNetPollGroup;

namespace ayt::net
{

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
//   factory registered by the subsystem (see s_adoptFactory below) so
//   GnsConnection itself stays decoupled from the subsystem type.
// =============================================================================
class GnsConnection {
public:
    using StateHandler = std::function<void(GnsConnectionState oldState, GnsConnectionState newState)>;
    using DataHandler  = std::function<void(const uint8_t* data, size_t len)>;

    // R1.A: factory invoked by the global status callback when an incoming
    // connection arrives on a server listen socket. Returns a fresh
    // GnsConnection* the subsystem will own (must outlive the conn). The
    // factory is responsible for registering the connection with s_gns
    // (AcceptConnection + SetConnectionPollGroup) and adopting the handle.
    // Returning nullptr means "reject the incoming connection".
    using AdoptFactory = std::function<GnsConnection*(HSteamNetConnection incomingConn)>;
    static void setAdoptFactory(AdoptFactory factory) { s_adoptFactory = std::move(factory); }

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

    // R1.A: attach to an already-accepted GNS connection (server child path).
    // Used by the adopt factory: it AcceptConnection()s and SetConnectionPollGroup()s
    // the incoming conn, then calls this to bind it to the new GnsConnection.
    void adoptIncomingConnection(HSteamNetConnection conn);

    // ===== Per-frame =====
    // Pump GNS callbacks + drain receive queue. Must be called every frame.
    void update();

    // ===== Send / Close =====
    // channel: 0..3 (CHANNEL_RELIABLE/UNRELIABLE/FRAGMENTED/ACK)
    int  send(uint8_t channel, const void* data, size_t len);
    // R4.1-B: seal with RequiresAck + seq prefix; optional callback when
    // the peer echoes kMsgTypeAppAck on CHANNEL_ACK.
    int  sendRequireAck(uint16_t msgType, uint8_t channel, const void* data, size_t len,
                        AckTracker::Callback onAck = nullptr);
    void disconnect(const char* reason = nullptr);

    // ===== State / info =====
    GnsConnectionState getState() const { return _state; }
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

    // R1 done (2026-07-27): set the protocol version this endpoint
    // expects/announces. 0 disables the handshake (legacy loopback).
    // Must be called BEFORE initClient/initServer/adoptIncomingConnection
    // to take effect. Default = 0 (handshake skipped).
    void setProtocolVersion(uint32_t version) { _protocolVersion = version; }
    uint32_t getProtocolVersion() const { return _protocolVersion; }

    // R1 done: reason for the last disconnect (Unknown if none / not yet).
    // After a peer-initiated disconnect this carries the wire-encoded
    // DisconnectReason; for local-initiated disconnect it's Unknown.
    DisconnectReason getLastDisconnectReason() const { return _lastDisconnectReason; }

    // ===== Callback registration =====
    void onStateChange(StateHandler handler) { _stateHandler = std::move(handler); }
    void onData(DataHandler handler)         { _dataHandler  = std::move(handler); }

    // ===== Opaque GNS handles (for NetworkSubSystem bookkeeping) =====
    HSteamNetConnection getInnerConnection() const { return _conn; }
    HSteamListenSocket  getInnerListenSocket() const { return _listen; }

    // ===== Internal access for the file-local gns:: helpers in GnsConnection.cpp =====
    // Public so the gns_status_callback trampoline and the gns::init/shutdown
    // helpers can route through them. The symbols themselves are TU-private
    // (defined in GnsConnection.cpp only), so external code can't link them.
    static ISteamNetworkingSockets* s_gns;
    static HSteamNetPollGroup       s_pollGroup;
    static AdoptFactory             s_adoptFactory;

    // GNS state -> our state mapping. Called from gns_status_callback.
    void handleStatusChange(int oldGnsState, int newGnsState);

    // R1 done: receive-side handshake parser. Called from update() when
    // data arrives on a Handshaking/Connected conn. If the bytes look
    // like a Hello/Welcome/Reject, dispatches into _handleHandshake().
    // Otherwise forwards to the user data handler.
    void onRawData(const uint8_t* data, size_t len);

    // Test seams for AckPipeline integration.
    size_t pendingAckCountForTesting() const { return _ackTracker.pendingCount(); }

private:
    void setState(GnsConnectionState newState);

    // R1 done: handshake state machine helpers.
    void _sendHello();
    void _sendWelcome();
    void _sendReject(DisconnectReason reason);
    void _handleHandshake(const uint8_t* data, size_t len);
    bool _isHandshaking() const { return _state == GnsConnectionState::Handshaking; }

    GnsConnectionState _state = GnsConnectionState::Disconnected;

    std::string _address;
    uint16_t    _port = 0;

    HSteamListenSocket  _listen = 0;
    HSteamNetConnection _conn   = 0;

    StateHandler _stateHandler;
    DataHandler  _dataHandler;

    // R1 done: 0 = no handshake (legacy). Non-zero enables HELLO/WELCOME.
    uint32_t _protocolVersion = 0;
    DisconnectReason _lastDisconnectReason = DisconnectReason::Unknown;

    // R2: per-connection reassembly state. Fragments are routed through
    // _assembler.consume() and the optional result is dispatched.
    PacketAssembler _assembler;
    uint32_t _nextFragmentId = 0;
    AckTracker _ackTracker;

    // R2: convenience for fragments that exceed MTU. ~1200 bytes fits
    // comfortably under typical internet MTUs (1500 minus IP+UDP+GNS
    // overhead).
    static constexpr size_t kFrameMtu = 1200;

    // R2: low-level GNS send wrapper. Called by send() and the handshake
    // helpers; maps channel -> GNS Reliable/Unreliable and reports EResult.
    int _rawSend(const uint8_t* data, uint32_t len, uint8_t channel);

    // R2: low-overhead current-time helper used by PacketCodec::encode.
    static uint32_t nowMs();
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