// AYNetworkSubSystem.cpp - 网络子系统实现

#include <AYNetwork.h>
#include <AYNetwork/NetworkModule.h>
#include <AYGameLoop.h>
#include <AYGameLoop/SubSystemRegistry.h>
#include <AYNetwork/Transport/GnsConnection.h>
#include <AYNetwork/Protocol/PacketCodec.h>
#include <AYNetwork/RPC/RpcHandler.h>
#include <AYNetwork/Transport/NetConnectionImpl.h>
// R5.3 (2026-08-24): Replay wire-tap hooks. Use forward declaration + the
// foundation interface only so we don't pull NetworkReplayRecorderAdapter.h
// (which includes INetwork.h → would create a cycle).
#include <AYReplay/IReplayRecorder.h>
#include <AYNetwork/Replay/NetworkReplayTypes.h>
#include "TransportFaultController.h"    // R5.4 (2026-08-25)
#include <AYNetwork/Profiler/ProfilerRegistry.h>  // R5.5 (2026-08-25)
// R1.A (2026-07-27): pull in EResult + AcceptConnection signature. The
// GnsConnection.cpp TU-private includes are sufficient because s_gns is a
// fully-typed pointer in this TU — we just need the constants.
#include <steam/steamclientpublic.h>
#include <steam/isteamnetworkingsockets.h>
#include <algorithm>
#include <cstdio>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace ayt::net
{

// =============================================================================
// NetworkSubSystem - 子系统实现
// =============================================================================
class NetworkSubSystem : public INetworkSubSystem {
    struct ServerClientRecord {
        std::unique_ptr<GnsConnection> transport;
        std::unique_ptr<NetConnectionImpl> facade;
        bool extensionDisconnectNotified = false;
    };

    enum class DeferredControlType : uint8_t {
        None,
        Connect,
        Listen,
        Disconnect,
        Shutdown,
    };

    enum class DriverMode : uint8_t {
        Unknown,
        LegacyUpdate,
        Phased,
    };

    struct DeferredControl {
        DeferredControlType type = DeferredControlType::None;
        std::string address;
        uint16_t port = 0;
    };

public:
    const char* getName() const override { return "Network"; }
    const ::ayt::game::SubSystemDescriptor& getDescriptor() const override {
        static ::ayt::game::SubSystemDescriptor desc = {
            .name = "Network",
            .dependencies = {},  // 无依赖
            .basePriority = 100,  // 早期初始化
            .timeType = ::ayt::game::SubSystemDescriptor::TimeType::Real,
            .phases = ::ayt::game::phaseBit(::ayt::game::FramePhase::Ingress)
                    | ::ayt::game::phaseBit(::ayt::game::FramePhase::FixedPrePhysics)
                    | ::ayt::game::phaseBit(::ayt::game::FramePhase::Egress),
            .clock = ::ayt::game::ClockDomain::RealWall,
            .phasePriority = 100,
            .reads = {},
            .writes = {"Simulation.World"}
        };
        return desc;
    }

    bool initialize() override {
        if (_initialized) return true;
        // R1 (2026-07-26): bring up GNS once per process. Ref-counted, so
        // subsequent initialise/shutdown cycles are safe.
        if (!gns::init()) {
            ::printf("[Network] Failed to init GameNetworkingSockets\n");
            return false;
        }
        _initialized = true;
        _driverMode = DriverMode::Unknown;
        setupProfilerHooks();
        ::printf("[Network] Initialized (GNS ready)\n");
        return true;
    }

    void update(float deltaTime) override {
        if (_driverMode == DriverMode::Phased) return;
        _driverMode = DriverMode::LegacyUpdate;
        // P0 audit fix (2026-07-26): no longer empty.
        // R1 (2026-07-26): forward every per-frame callback to the active
        // GnsConnection. ReplicationManager tick stays here so its deltaTime
        // advances even when there's no active connection (matches the
        // expected GameLoop semantics).
        // R1.A (2026-07-27): pump client conn (if any), server parent (if
        // listening), AND all server children (accepted client connections).
        pumpTransport();
        _replicationManager.tick(deltaTime);
        _rpcHandler.tick(deltaTime);
        // R5.5 (2026-08-25): tick the profiler window + periodic dump.
        _profiler.tickWindow(_replicationManager.getServerTick());
        _profiler.dumpPeriodicIfDue();
        fireProfilerSinkIfSet();
    }

    void tick(::ayt::game::FramePhase phase,
              const ::ayt::game::FrameContext& context) override {
        if (_driverMode == DriverMode::LegacyUpdate) return;
        _driverMode = DriverMode::Phased;
        if (phase == ::ayt::game::FramePhase::Ingress) {
            _ingressTargetSimTick = context.simTick + 1;
            _stagedIngress = true;
            pumpTransport();
            _stagedIngress = false;
        } else if (phase == ::ayt::game::FramePhase::FixedPrePhysics) {
            // R5.2 (2026-08-24): drain queued client inputs first so the
            // gameplay-side application callback fires before any
            // replicated state arrives in the same tick. The order
            // matters: the prediction loop runs predict→apply→read
            // snapshot, and the snapshot deserialization (drainSimulationInbound)
            // delivers server-authoritative state last. consumeClientInputs
            // is server-side only; clients no-op.
            _replicationManager.consumeClientInputs(static_cast<uint32_t>(context.simTick));
            // Apply packets assigned to this simulation tick once. All data
            // observed at this frame's Ingress belongs before its first
            // catch-up tick and is never replayed by later catch-up ticks.
            drainSimulationInbound(context.simTick);
            _rpcHandler.tick(context.fixedDeltaTime);
        } else if (phase == ::ayt::game::FramePhase::Egress) {
            // Observe the completed World state when producing replication.
            _replicationManager.tick(context.deltaTime);
            // R5.3 (2026-08-24): flush replay recorder at end-of-tick so
            // every captured frame is durable before the next tick starts.
            if (auto* rec = _replicationManager.getReplayRecorder()) {
                rec->flush();
            }
            // R5.5 (2026-08-25): tick the profiler window + periodic dump
            // at end-of-tick so a snapshot pulled after Egress reflects
            // the just-completed serverTick's contributions.
            _profiler.tickWindow(_replicationManager.getServerTick());
            _profiler.dumpPeriodicIfDue();
            fireProfilerSinkIfSet();
        }
    }

    // Legacy direct-call compatibility. The staged loop uses tick() above and
    // never enters this adapter for an explicitly phased subsystem.
    void fixedUpdate(float fixedDeltaTime) override {
        (void)fixedDeltaTime;
    }

    void shutdown() override {
        if (!_initialized) return;
        if (GnsConnection::isPumping()) {
            _deferredControl = {DeferredControlType::Shutdown, {}, 0};
            return;
        }
        shutdownNow();
    }

    void shutdownNow() {
        disconnectNow();
        {
            std::lock_guard<std::mutex> lock(_simulationInboundMutex);
            _simulationInbound.clear();
            _simulationInboundBytes = 0;
        }
        _ingressTargetSimTick = 0;
        gns::shutdown();
        _initialized = false;
        _driverMode = DriverMode::Unknown;
        ::printf("[Network] Shutdown\n");
    }

    // ===== R1.A: server-side accept factory =====
    // Called from GnsConnection's global status callback when an incoming
    // connection arrives on _serverConn's listen socket. AcceptConnection +
    // SetConnectionPollGroup + adopt, then route through onMessage wiring.
    GnsConnection* adoptIncomingClient(HSteamNetConnection incoming) {
        if (!_serverConn) {
            ::fprintf(stderr, "[Network] incoming conn %u but server not listening\n", incoming);
            return nullptr;
        }
        if (_serverClients.size() >= _limits.maxConnections) {
            GnsConnection::s_gns->CloseConnection(
                incoming, 0, "server connection limit reached", false);
            ++_rejectedConnections;
            return nullptr;
        }
        // Accept the connection on GNS side first.
        EResult r = GnsConnection::s_gns->AcceptConnection(incoming);
        if (r != k_EResultOK) {
            ::fprintf(stderr, "[Network] AcceptConnection failed: %d\n", r);
            return nullptr;
        }
        // Create the server-child GnsConnection, install it in our list.
        auto child = std::make_unique<GnsConnection>();
        child->setProtocolVersion(_protocolVersion);
        child->adoptIncomingConnection(incoming);

        // R1.A: route onData to the registered message handler (if any).
        GnsConnection* rawChild = child.get();
        const uint32_t netId = allocateNetId();
        rawChild->setNetId(netId);                         // R5.4
        rawChild->attachFaultController(&_faultCtl);        // R5.4
        hookGnsConnection(rawChild);                        // R5.5
        auto netConn = std::make_unique<NetConnectionImpl>(rawChild, netId);
        NetConnectionImpl* netPtr = netConn.get();

        // Both public admission APIs participate in the same atomic gate.
        bool accepted = !_acceptCallback || _acceptCallback(netPtr);
        if (accepted && _extension) {
            accepted = _extension->onIncomingConnection(netPtr);
        }
        if (!accepted) {
            rawChild->disconnect("connection admission rejected");
            ++_rejectedConnections;
            return nullptr;
        }

        child->onPacket(makePacketHandler(netPtr));

        if (_connectionHandler) {
            rawChild->onStateChange([this, rawChild, netPtr](GnsConnectionState /*oldS*/,
                                                             GnsConnectionState newS) {
                const bool connected = (newS == GnsConnectionState::Ready)
                    || (newS == GnsConnectionState::Connected
                        && rawChild->getProtocolVersion() == 0);
                const DisconnectReason reason = connected
                    ? DisconnectReason::Unknown
                    : rawChild->getLastDisconnectReason();
                _connectionHandler(netPtr, connected, reason);
            });
            // If handshake finished before the handler was installed, synthesize
            // the connected callback so late-join rebroadcast is not missed.
            if (rawChild->isConnected()) {
                _connectionHandler(netPtr, true, DisconnectReason::Unknown);
            }
        } else if (_pendingConnHandler) {
            rawChild->onStateChange([this, rawChild](GnsConnectionState /*oldS*/,
                                                     GnsConnectionState newS) {
                const bool connected = (newS == GnsConnectionState::Ready)
                    || (newS == GnsConnectionState::Connected
                        && rawChild->getProtocolVersion() == 0);
                const DisconnectReason reason = connected
                    ? DisconnectReason::Unknown
                    : rawChild->getLastDisconnectReason();
                _pendingConnHandler(connected, reason);
            });
        }

        GnsConnection* raw = child.get();
        ServerClientRecord record;
        record.transport = std::move(child);
        record.facade = std::move(netConn);
        _serverClients.push_back(std::move(record));
        ::printf("[Network] accepted incoming client (now %zu clients)\n", _serverClients.size());
        return raw;
    }

    GnsConnection::PacketHandler makePacketHandler(NetConnection* from) {
        return [this, from](const PacketHeader& header, const uint8_t* body, size_t len) {
            dispatchIncoming(from, header, body, len);
        };
    }

    void dispatchIncoming(NetConnection* from, const PacketHeader& header,
                          const uint8_t* body, size_t len) {
        const uint8_t channel = header.channel;
        switch (header.msgType) {
            case kMsgTypeRpcRequest:
            case kMsgTypeRpcResponse:
            case kMsgTypeRpcReject:
            case kMsgTypeDelta:
            case kMsgTypeReplication:
            case kMsgTypeEntitySpawn:
            case kMsgTypeEntityDespawn:
                if (_stagedIngress) {
                    queueSimulationInbound(header.msgType,
                                           from != nullptr ? from->getId() : 0,
                                           body, len);
                } else {
                    applySimulationInbound(header.msgType, from,
                                           const_cast<uint8_t*>(body), len);
                }
                return;
            default:
                if (_messageHandlers[channel]) {
                    _messageHandlers[channel](from, channel, body, len);
                }
                return;
        }
    }

    struct PendingSimulationInbound {
        uint64_t targetSimTick = 0;
        uint16_t messageType = 0;
        uint32_t fromNetId = 0;
        std::vector<uint8_t> body;
    };

    void queueSimulationInbound(uint16_t messageType,
                                uint32_t fromNetId,
                                const uint8_t* body,
                                size_t bodySize) {
        std::lock_guard<std::mutex> lock(_simulationInboundMutex);
        if ((body == nullptr && bodySize != 0) ||
            bodySize > _limits.maxQueuedInboundBytes ||
            _simulationInbound.size() >= _limits.maxQueuedInboundMessages ||
            bodySize > _limits.maxQueuedInboundBytes - _simulationInboundBytes) {
            ++_droppedInboundMessages;
            return;
        }

        PendingSimulationInbound pending;
        pending.targetSimTick = _ingressTargetSimTick != 0
            ? _ingressTargetSimTick
            : 1;
        pending.messageType = messageType;
        pending.fromNetId = fromNetId;
        if (body != nullptr && bodySize != 0) {
            pending.body.assign(body, body + bodySize);
        }
        _simulationInboundBytes += pending.body.size();
        _simulationInbound.push_back(std::move(pending));
    }

    NetConnection* findConnectionById(uint32_t netId) const {
        if (netId == 0) return nullptr;
        if (_clientNetConn && _clientNetConn->getId() == netId) {
            return _clientNetConn.get();
        }
        for (const auto& connection : _serverClients) {
            if (connection.facade && connection.facade->getId() == netId) {
                return connection.facade.get();
            }
        }
        return nullptr;
    }

    void applySimulationInbound(uint16_t messageType,
                                NetConnection* from,
                                uint8_t* body,
                                size_t bodySize) {
        BitStream bodyStream(body, bodySize);
        bodyStream.resetForRead();
        // R5.3 (2026-08-24) Replay wire-tap: server-only inbound capture.
        // We pack payloads locally here so we don't need the adapter header
        // (which would cycle back through INetwork.h). The recorder is
        // opted-in; when not present, every call short-circuits.
        ayt::replay::IReplayRecorder* rec = _replicationManager.getReplayRecorder();
        const bool isAuth = _replicationManager.isAuthority();
        const uint32_t fromId = from ? from->getId() : 0u;
        switch (messageType) {
        case kMsgTypeRpcRequest:
            (void)_rpcHandler.onRpcRequest(bodyStream, from);
            if (rec && isAuth && body && bodySize > 0) {
                std::vector<uint8_t> hdr(10 + bodySize);
                hdr[0] = static_cast<uint8_t>(messageType & 0xFF);
                hdr[1] = static_cast<uint8_t>((messageType >> 8) & 0xFF);
                hdr[2] = static_cast<uint8_t>(fromId & 0xFF);
                hdr[3] = static_cast<uint8_t>((fromId >> 8) & 0xFF);
                hdr[4] = static_cast<uint8_t>((fromId >> 16) & 0xFF);
                hdr[5] = static_cast<uint8_t>((fromId >> 24) & 0xFF);
                const uint32_t blen = static_cast<uint32_t>(bodySize);
                hdr[6] = static_cast<uint8_t>(blen & 0xFF);
                hdr[7] = static_cast<uint8_t>((blen >> 8) & 0xFF);
                hdr[8] = static_cast<uint8_t>((blen >> 16) & 0xFF);
                hdr[9] = static_cast<uint8_t>((blen >> 24) & 0xFF);
                std::memcpy(hdr.data() + 10, body, bodySize);
                rec->recordEvent(_replicationManager.getServerTick(), ayt::net::replay::kEvtNet_RpcBatch,
                                 hdr.data(), hdr.size());
            }
            break;
        case kMsgTypeRpcResponse:
            (void)_rpcHandler.onRpcResponse(bodyStream, from);
            if (rec && isAuth && body && bodySize > 0) {
                std::vector<uint8_t> hdr(10 + bodySize);
                hdr[0] = static_cast<uint8_t>(messageType & 0xFF);
                hdr[1] = static_cast<uint8_t>((messageType >> 8) & 0xFF);
                hdr[2] = static_cast<uint8_t>(fromId & 0xFF);
                hdr[3] = static_cast<uint8_t>((fromId >> 8) & 0xFF);
                hdr[4] = static_cast<uint8_t>((fromId >> 16) & 0xFF);
                hdr[5] = static_cast<uint8_t>((fromId >> 24) & 0xFF);
                const uint32_t blen = static_cast<uint32_t>(bodySize);
                hdr[6] = static_cast<uint8_t>(blen & 0xFF);
                hdr[7] = static_cast<uint8_t>((blen >> 8) & 0xFF);
                hdr[8] = static_cast<uint8_t>((blen >> 16) & 0xFF);
                hdr[9] = static_cast<uint8_t>((blen >> 24) & 0xFF);
                std::memcpy(hdr.data() + 10, body, bodySize);
                rec->recordEvent(_replicationManager.getServerTick(), ayt::net::replay::kEvtNet_RpcBatch,
                                 hdr.data(), hdr.size());
            }
            break;
        case kMsgTypeRpcReject:
            (void)_rpcHandler.onRpcReject(bodyStream, from);
            if (rec && isAuth && body && bodySize > 0) {
                std::vector<uint8_t> hdr(10 + bodySize);
                hdr[0] = static_cast<uint8_t>(messageType & 0xFF);
                hdr[1] = static_cast<uint8_t>((messageType >> 8) & 0xFF);
                hdr[2] = static_cast<uint8_t>(fromId & 0xFF);
                hdr[3] = static_cast<uint8_t>((fromId >> 8) & 0xFF);
                hdr[4] = static_cast<uint8_t>((fromId >> 16) & 0xFF);
                hdr[5] = static_cast<uint8_t>((fromId >> 24) & 0xFF);
                const uint32_t blen = static_cast<uint32_t>(bodySize);
                hdr[6] = static_cast<uint8_t>(blen & 0xFF);
                hdr[7] = static_cast<uint8_t>((blen >> 8) & 0xFF);
                hdr[8] = static_cast<uint8_t>((blen >> 16) & 0xFF);
                hdr[9] = static_cast<uint8_t>((blen >> 24) & 0xFF);
                std::memcpy(hdr.data() + 10, body, bodySize);
                rec->recordEvent(_replicationManager.getServerTick(), ayt::net::replay::kEvtNet_RpcBatch,
                                 hdr.data(), hdr.size());
            }
            break;
        case kMsgTypeReplication:
        case kMsgTypeDelta:
        case kMsgTypeEntitySpawn:
        case kMsgTypeEntityDespawn:
            (void)_replicationManager.onReceive(messageType, bodyStream, from);
            break;
        case kMsgTypeClientInput: {
            // R5.2 (2026-08-24): per-connection client input frame. Pass the
            // raw body straight to ReplicationManager which delegates into
            // its PredictionManager (decoded by ClientInputCodec). The
            // manager is server-only; on a client this case drops silently.
            (void)_replicationManager.onClientInput(fromId, body, bodySize);
            // R5.3 (2026-08-24): record the authoritative input frame so
            // playback can re-evaluate prediction during v2 deterministic
            // replay. We re-derive inputSeq/serverTickAtSend by header
            // inspection when the codec length matches; otherwise we
            // record a fixed placeholder tick (serverTickAtSend = 0).
            if (rec && isAuth && body && bodySize > 0) {
                // ClientInputCodec::encode emits [u32 inputSeq][u32 serverTickAtSend][bytes].
                uint32_t inputSeq = 0, serverTickAtSend = 0;
                if (bodySize >= 8) {
                    std::memcpy(&inputSeq,        body + 0, 4);
                    std::memcpy(&serverTickAtSend, body + 4, 4);
                }
                std::vector<uint8_t> inBuf(16 + bodySize);
                std::memcpy(inBuf.data() +  0, &fromId,            4);
                std::memcpy(inBuf.data() +  4, &inputSeq,          4);
                std::memcpy(inBuf.data() +  8, &serverTickAtSend,  4);
                const uint32_t blen = static_cast<uint32_t>(bodySize);
                std::memcpy(inBuf.data() + 12, &blen,              4);
                std::memcpy(inBuf.data() + 16, body,               bodySize);
                rec->recordEvent(_replicationManager.getServerTick(), ayt::net::replay::kEvtNet_InputBatch,
                                 inBuf.data(), inBuf.size());
            }
            break;
        }
        default:
            break;
        }
    }

    void drainSimulationInbound(uint64_t simTick) {
        std::vector<PendingSimulationInbound> due;
        {
            std::lock_guard<std::mutex> lock(_simulationInboundMutex);
            while (!_simulationInbound.empty()
                   && _simulationInbound.front().targetSimTick <= simTick) {
                due.push_back(std::move(_simulationInbound.front()));
                _simulationInboundBytes -= due.back().body.size();
                _simulationInbound.pop_front();
            }
        }

        for (PendingSimulationInbound& pending : due) {
            NetConnection* from = findConnectionById(pending.fromNetId);
            if (pending.fromNetId != 0 && from == nullptr) continue;
            applySimulationInbound(pending.messageType, from,
                                   pending.body.data(), pending.body.size());
        }
    }

    void installInboundRoutes() {
        if (_clientConn && _clientNetConn) {
            _clientConn->onPacket(makePacketHandler(_clientNetConn.get()));
        }
    }

    uint32_t allocateNetId() {
        do {
            ++_nextNetId;
        } while (_nextNetId == INVALID_NET_ID);
        return _nextNetId;
    }

    void pumpTransport() {
        const GnsPumpBudget budget{
            _limits.maxPumpMessages,
            _limits.maxPumpBytes,
        };
        const GnsPumpResult result = GnsConnection::pump(budget);
        if (result.budgetExhausted) ++_pumpBudgetExhaustions;
        reapDisconnectedClients();
        processDeferredControl();
    }

    void reapDisconnectedClients() {
        auto it = _serverClients.begin();
        while (it != _serverClients.end()) {
            if (!it->transport ||
                it->transport->getState() == GnsConnectionState::Disconnected) {
                if (_extension && it->facade &&
                    !it->extensionDisconnectNotified) {
                    _extension->onConnectionDisconnected(it->facade.get());
                    it->extensionDisconnectNotified = true;
                }
                it = _serverClients.erase(it);
            } else {
                ++it;
            }
        }
    }

    void clearServerClients(const char* reason) {
        for (auto& record : _serverClients) {
            if (_extension && record.facade &&
                !record.extensionDisconnectNotified) {
                _extension->onConnectionDisconnected(record.facade.get());
                record.extensionDisconnectNotified = true;
            }
            if (record.transport) record.transport->disconnect(reason);
        }
        _serverClients.clear();
    }

    void processDeferredControl() {
        if (_deferredControl.type == DeferredControlType::None ||
            GnsConnection::isPumping()) {
            return;
        }
        DeferredControl control = std::move(_deferredControl);
        _deferredControl = {};
        switch (control.type) {
        case DeferredControlType::Connect:
            connectNow(control.address.c_str(), control.port);
            break;
        case DeferredControlType::Listen:
            listenNow(control.port);
            break;
        case DeferredControlType::Disconnect:
            disconnectNow();
            break;
        case DeferredControlType::Shutdown:
            shutdownNow();
            break;
        case DeferredControlType::None:
            break;
        }
    }

    // ===== 连接管理 =====
    void connect(const char* address, uint16_t port) override {
        if (GnsConnection::isPumping()) {
            _deferredControl = {
                DeferredControlType::Connect,
                address ? address : "",
                port,
            };
            return;
        }
        connectNow(address, port);
    }

    void connectNow(const char* address, uint16_t port) {
        if (_clientConn) {
            _clientConn->disconnect("superseded by connect()");
            _clientConn.reset();
            _clientNetConn.reset();
        }
        _clientConn = std::make_unique<GnsConnection>();
        _clientConn->setProtocolVersion(_protocolVersion);
        _clientConn->initClient(address, port);
        const uint32_t clientNetId = allocateNetId();
        _clientConn->setNetId(clientNetId);                    // R5.4
        _clientConn->attachFaultController(&_faultCtl);       // R5.4
        hookGnsConnection(_clientConn.get());                 // R5.5
        _clientNetConn = std::make_unique<NetConnectionImpl>(
            _clientConn.get(), clientNetId);
        _mode = ConnectionMode::Client;
        installInboundRoutes();
        if (_connectionHandler) {
            _clientConn->onStateChange([this](GnsConnectionState /*oldS*/, GnsConnectionState newS) {
                bool connected = (newS == GnsConnectionState::Ready) ||
                                 (newS == GnsConnectionState::Connected &&
                                  _clientConn->getProtocolVersion() == 0);
                DisconnectReason reason = connected
                    ? DisconnectReason::Unknown
                    : _clientConn->getLastDisconnectReason();
                _connectionHandler(_clientNetConn.get(), connected, reason);
            });
        }
    }

    void listen(uint16_t port) override {
        if (GnsConnection::isPumping()) {
            _deferredControl = {DeferredControlType::Listen, {}, port};
            return;
        }
        listenNow(port);
    }

    void listenNow(uint16_t port) {
        if (_serverConn) {
            _serverConn->disconnect("superseded by listen()");
            _serverConn.reset();
        }
        clearServerClients("superseded by listen()");

        _serverConn = std::make_unique<GnsConnection>();
        _serverConn->setProtocolVersion(_protocolVersion);
        _serverConn->initServer(port);
        // The listen conn is just a socket — no peer netId, no messages
        // flow through it. Hooking it would inject netId=0 records which
        // ProfilerRegistry::recordSend drops (early return at line 47).
        // Skip hookGnsConnection here; children hook themselves when accepted.
        const HSteamListenSocket listener = _serverConn->getInnerListenSocket();
        GnsConnection::setAdoptFactory(
            listener,
            [this](HSteamNetConnection incoming) -> GnsConnection* {
                return this->adoptIncomingClient(incoming);
            });
        // R4.1: listen() means "host is also a player" — design §6.6
        // distinguishes Server (dedicated) vs ListenServer (host). R4.0
        // collapsed these; R4.1 fixes the typo so ReplicationManager's
        // authority gate (which now accepts both via isAuthority()) sees
        // the right mode.
        _mode = ConnectionMode::ListenServer;
    }

    void disconnect() override {
        if (GnsConnection::isPumping()) {
            _deferredControl = {DeferredControlType::Disconnect, {}, 0};
            return;
        }
        disconnectNow();
    }

    void disconnectNow() {
        if (_clientConn) {
            _clientConn->disconnect("client disconnect");
            _clientConn.reset();
            _clientNetConn.reset();
        }
        if (_serverConn) {
            _serverConn->disconnect("server shutdown");
            _serverConn.reset();
        }
        clearServerClients("server shutdown");
        {
            std::lock_guard<std::mutex> lock(_simulationInboundMutex);
            _simulationInbound.clear();
            _simulationInboundBytes = 0;
        }
        _mode = ConnectionMode::Disconnected;
    }

    bool isConnected() const override {
        if (_clientConn && _clientConn->isConnected()) return true;
        for (auto& child : _serverClients) {
            if (child.transport && child.transport->isConnected()) return true;
        }
        return false;
    }

    bool isListening() const override {
        return _serverConn &&
            _serverConn->getInnerListenSocket() != k_HSteamListenSocket_Invalid;
    }

    ConnectionMode getMode() const override {
        return _mode;
    }

    void setProtocolVersion(uint32_t version) override {
        if (_clientConn || _serverConn || !_serverClients.empty()) {
            ::fprintf(stderr,
                "[Network] setProtocolVersion ignored while transport is active\n");
            return;
        }
        _protocolVersion = version;
    }

    uint32_t getProtocolVersion() const override { return _protocolVersion; }

    void setLimits(const NetworkLimits& limits) override {
        _limits.maxConnections = std::max(1u, limits.maxConnections);
        _limits.maxPumpMessages = std::max(1u, limits.maxPumpMessages);
        _limits.maxPumpBytes = std::max(1u, limits.maxPumpBytes);
        _limits.maxQueuedInboundMessages =
            std::max(1u, limits.maxQueuedInboundMessages);
        _limits.maxQueuedInboundBytes =
            std::max(1u, limits.maxQueuedInboundBytes);
    }

    NetworkLimits getLimits() const override { return _limits; }

    // ===== 消息发送 =====
    void send(uint8_t channel, const void* data, size_t size) override {
        if (_clientConn) {
            _clientConn->send(channel, data, size);
        }
    }

    void sendTo(NetConnection* conn, uint8_t channel, const void* data, size_t size) override {
        // R4.1: per-connection send. NetConnectionImpl::send forwards to the
        // wrapped GnsConnection::send which routes through the 4-channel
        // _rawSend switch (R4.0). Single-call indirection cost; identical
        // wire behavior to broadcast().
        if (!conn) return;
        conn->send(channel, data, size);
    }

    void broadcast(uint8_t channel, const void* data, size_t size) override {
        // R1.A: actually iterate all server children and send to each.
        for (auto& child : _serverClients) {
            if (child.transport && child.transport->isConnected()) {
                child.transport->send(channel, data, size);
            }
        }
    }

    void broadcastExcept(NetConnection* exclude, uint8_t channel, const void* data, size_t size) override {
        for (auto& record : _serverClients) {
            if (!record.facade || !record.facade->isConnected()) continue;
            if (exclude && record.facade.get() == exclude) continue;
            record.facade->send(channel, data, size);
        }
    }

    void sendEncoded(uint8_t channel, const void* data, size_t size) override {
        if (_clientConn) _clientConn->sendEncoded(channel, data, size);
    }

    void sendEncodedTo(NetConnection* conn, uint8_t channel,
                       const void* data, size_t size) override {
        if (!conn) return;
        if (_clientNetConn.get() == conn && _clientConn) {
            _clientConn->sendEncoded(channel, data, size);
            return;
        }
        for (auto& record : _serverClients) {
            if (record.facade.get() == conn && record.transport) {
                record.transport->sendEncoded(channel, data, size);
                return;
            }
        }
    }

    void broadcastEncoded(uint8_t channel, const void* data, size_t size) override {
        for (auto& record : _serverClients) {
            if (record.transport && record.transport->isConnected()) {
                record.transport->sendEncoded(channel, data, size);
            }
        }
    }

    // ===== 消息接收 =====
    void onMessage(uint8_t channel, MessageHandler handler) override {
        _messageHandlers[channel] = handler;
        installInboundRoutes();
    }

    // ===== 连接状态 =====
    void onConnectionChange(ConnectionHandler handler) override {
        _connectionHandler = handler;
        if (_clientConn) {
            _clientConn->onStateChange([this](GnsConnectionState oldS, GnsConnectionState newS) {
                if (_connectionHandler) {
                    // R1 done: surface DisconnectReason. connected=true ->
                    // Unknown. connected=false -> whatever the GnsConnection
                    // captured (UserQuit, ConnectionLost, etc.).
                    bool connected = (newS == GnsConnectionState::Ready) ||
                                     (newS == GnsConnectionState::Connected &&
                                      _clientConn->getProtocolVersion() == 0);
                    DisconnectReason reason = connected
                        ? DisconnectReason::Unknown
                        : _clientConn->getLastDisconnectReason();
                    _connectionHandler(nullptr, connected, reason);
                }
            });
        }
        _pendingConnHandler = [this](bool connected, DisconnectReason reason) {
            if (_connectionHandler) _connectionHandler(nullptr, connected, reason);
        };
    }

    // ===== 服务器专用 =====
    void setAcceptCallback(AcceptCallback callback) override {
        _acceptCallback = callback;
    }

    void kickConnection(NetConnection* conn, const char* reason) override {
        // R4.1: real impl — NetConnection::disconnect forwards through the
        // adapter to the underlying GnsConnection.
        if (!conn) return;
        conn->disconnect(reason ? reason : "kicked by server");
    }

    const std::vector<NetConnection*>& getConnections() override {
        _connections.clear();
        _connections.reserve(_serverClients.size());
        for (auto& record : _serverClients) {
            if (record.facade && record.facade->isConnected()) {
                _connections.push_back(record.facade.get());
            }
        }
        return _connections;
    }

    // ===== 查询 =====
    NetConnection* getConnection() const override {
        return _clientNetConn.get();
    }

    uint32_t getHostId() const override {
        return _hostId;
    }

    // ===== 扩展点 =====
    void setExtension(INetworkExtension* ext) override {
        _extension = ext;
        _replicationManager.setExtension(ext);
    }

    // ===== Replication =====
    ReplicationManager* getReplicationManager() override {
        return &_replicationManager;
    }

    // R5.3 (2026-08-24): opt-in replay recorder. Forwarded to the
    // replication manager (which guards by isAuthority()) and to the RPC
    // handler (which captures outbound RPCs).
    void setReplayRecorder(ayt::replay::IReplayRecorder* rec) override {
        _replicationManager.setReplayRecorder(rec);
        _rpcHandler.setReplayRecorder(rec);
    }
    ayt::replay::IReplayRecorder* getReplayRecorder() const override {
        return _replicationManager.getReplayRecorder();
    }

    // ===== R5.4 (2026-08-25) transport-fault profile =====
    void setTransportFaultProfile(uint32_t netId,
                                  const TransportFaultProfile& profile) override {
        _faultCtl.setProfile(netId, profile);
    }
    void clearTransportFaultProfile(uint32_t netId) override {
        _faultCtl.clearProfile(netId);
    }
    const TransportFaultProfile* getTransportFaultProfile(uint32_t netId) const override {
        return _faultCtl.getProfile(netId);
    }

    // ===== R5.5 (2026-08-25) bandwidth / connection profiler =====
    void getProfilerSnapshot(ProfilerSnapshot& out, uint32_t netId) override {
        _profiler.snapshotFor(netId, out);
    }
    void getProfilerSnapshots(std::vector<ProfilerSnapshot>& out) override {
        _profiler.snapshotAll(out);
    }
    void setProfilerSinkForTesting(
        std::function<void(const ProfilerSnapshot&)> sink) override {
        _profiler.setSink(std::move(sink));
    }
    void setProfilerDumpInterval(uint32_t ticks) override {
        _profiler.setDumpEveryTicks(ticks);
    }

    // Wire the profiler hooks into ReplicationManager, RpcHandler, and any
    // GnsConnections that exist at the time of call. New connections built
    // later (connectNow / adoptIncomingClient) call hookGnsConnection()
    // inline so their bytes count toward the same per-connection state.
    //
    // Called once from initialize(). Idempotent — replacing a hook is fine.
    void setupProfilerHooks() {
        // ReplicationManager: (connNetId, msgType, bytes, ghostNetId)
        _replicationManager.setProfilerSendHook(
            [this](uint32_t connNetId, uint16_t msgType, uint64_t bytes, uint32_t ghostNetId) {
                _profiler.recordSend(connNetId, msgType, bytes, ghostNetId);
            });

        // RpcHandler: (connNetId, msgType, bytes) — no ghost association.
        _rpcHandler.setProfilerRpcHook(
            [this](uint32_t connNetId, uint16_t msgType, uint64_t bytes) {
                _profiler.recordSend(connNetId, msgType, bytes, /*ghostNetId=*/ 0);
            });

        // GnsConnection (already created before initialize): client conn,
        // server listen conn, server children.
        if (_clientConn) hookGnsConnection(_clientConn.get());
        if (_serverConn) hookGnsConnection(_serverConn.get());
        for (auto& rec : _serverClients) {
            if (rec.transport) hookGnsConnection(rec.transport.get());
        }

        // ConnAccessor — the registry needs a way to ask "who owns netId X"
        // when building the live-status row of a snapshot. Return a pointer
        // to whichever GnsConnection carries the given netId (server child,
        // client conn, or server listen conn). Null when not found.
        _profiler.setConnAccessorForTesting(
            [this](uint32_t connNetId, const GnsConnection*& out) {
                if (_clientConn && _clientConn->getNetId() == connNetId) {
                    out = _clientConn.get();
                    return;
                }
                if (_serverConn && _serverConn->getNetId() == connNetId) {
                    out = _serverConn.get();
                    return;
                }
                for (auto& rec : _serverClients) {
                    if (rec.transport && rec.transport->getNetId() == connNetId) {
                        out = rec.transport.get();
                        return;
                    }
                }
                out = nullptr;
            });
    }

    // Attach the profiler hooks to a single GnsConnection (send + recv).
    // Uses the connection's own netId so the registry can bucket counters.
    void hookGnsConnection(GnsConnection* conn) {
        if (!conn) return;
        const uint32_t netId = conn->getNetId();
        conn->setProfilerSendHook(
            [this, netId](uint16_t msgType, uint64_t bytes) {
                _profiler.recordSend(netId, msgType, bytes, /*ghostNetId=*/ 0);
            });
        conn->setProfilerRecvHook(
            [this, netId](uint16_t msgType, uint64_t bytes) {
                _profiler.recordRecv(netId, msgType, bytes, /*ghostNetId=*/ 0);
            });
    }

    // Build a snapshot vector (one entry per active connection) and fan it
    // out to the test sink if one was registered. Called after tickWindow
    // + dumpPeriodicIfDue so the snapshot reflects the just-completed tick.
    void fireProfilerSinkIfSet() {
        std::vector<ProfilerSnapshot> snaps;
        _profiler.snapshotAll(snaps);
        for (const auto& s : snaps) {
            _profiler.fireSink(s);
        }
    }

    // ===== R4.0 RPC =====
    RpcHandler* getRpcHandler() override {
        return &_rpcHandler;
    }

private:
    ConnectionMode _mode = ConnectionMode::Disconnected;
    bool _initialized = false;
    DriverMode _driverMode = DriverMode::Unknown;
    bool _connected = false;
    uint32_t _hostId = 0;

    NetConnection* _connection = nullptr;
    std::vector<NetConnection*> _connections;

    MessageHandler _messageHandlers[256];
    ConnectionHandler _connectionHandler;
    AcceptCallback _acceptCallback;
    INetworkExtension* _extension = nullptr;

    // R1.A: subsystem owns one client conn (or none), one server parent
    // (listen socket, or none), and a list of server children (accepted clients).
    std::unique_ptr<GnsConnection> _clientConn;
    std::unique_ptr<NetConnectionImpl> _clientNetConn;
    std::unique_ptr<GnsConnection> _serverConn;
    std::vector<ServerClientRecord> _serverClients;
    uint32_t _nextNetId = 0;
    uint32_t _protocolVersion = kProtocolVersion;
    NetworkLimits _limits{};
    DeferredControl _deferredControl{};
    uint64_t _rejectedConnections = 0;
    uint64_t _pumpBudgetExhaustions = 0;
    uint64_t _droppedInboundMessages = 0;

    // Pending handler captured at onConnectionChange time so adoptIncomingClient()
    // can install it on each new server child.
    std::function<void(bool, DisconnectReason)> _pendingConnHandler;

    ReplicationManager _replicationManager{this};
    // R4.0 (2026-07-29): mirrors _replicationManager ownership. Routed by
    // onMessage via PacketHeader.msgType envelope kind.
    RpcHandler _rpcHandler{this};
    // R5.4 (2026-08-25): per-connection fault-profile store. Owns the
    // RNG map keyed by netId. GnsConnection instances pull profiles
    // through it on every send/recv tick.
    TransportFaultController _faultCtl;

    // R5.5 (2026-08-25): bandwidth / connection profiler. Owns the
    // per-connection counters and the periodic stderr dump machinery.
    // Pull API goes through the four override methods above; the hook
    // for ReplicationManager / RpcHandler / GnsConnection is wired in
    // setupProfilerHooks() called from the constructor.
    ProfilerRegistry _profiler;
    std::mutex _simulationInboundMutex;
    std::deque<PendingSimulationInbound> _simulationInbound;
    size_t _simulationInboundBytes = 0;
    uint64_t _ingressTargetSimTick = 0;
    bool _stagedIngress = false;
};

// 注册宏 — may be stripped from static libs; callers should also invoke
// registerNetworkSubSystem() explicitly (Editor Play, tests).
REGISTER_SUBSYSTEM(NetworkSubSystem, {}, 100);

INetworkSubSystem* findRegisteredNetworkSubSystem()
{
    auto* system = ::ayt::game::SubSystemRegistry::instance().findSubSystem("Network");
    return dynamic_cast<INetworkSubSystem*>(system);
}

void registerNetworkSubSystem()
{
    static bool registered = false;
    if (registered) {
        return;
    }
    registered = true;
    if (findRegisteredNetworkSubSystem() != nullptr) {
        return;
    }
    ::ayt::game::IGameLoop::instance().registerSubSystem(new NetworkSubSystem());
}

#if defined(AYNETWORK_BUILD_TESTS)
INetworkSubSystem* createNetworkSubSystemForTest() {
    return new NetworkSubSystem();
}
#endif

} // namespace ayt::net
