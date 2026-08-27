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
#include <bit>
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
        ConnectP2P,
        ListenP2P,
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
        if (_p2pSignaling) _p2pSignaling->stop();
        _p2pSignaling.reset();
        _p2pConfigured = false;
        _p2pConfig = {};
        // R6 C6: single-thread; no lock.
        _simulationInbound.clear();
        _simulationInboundBytes = 0;
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
    GnsConnection* adoptIncomingClient(HSteamNetConnection incoming,
                                       const PeerId* remotePeer = nullptr,
                                       uint16_t p2pVirtualPort = 0) {
        if (!_serverConn && !_p2pListening) {
            ::fprintf(stderr, "[Network] incoming conn %u but server not listening\n", incoming);
            return nullptr;
        }
        // A PeerId names one active engine session. A reconnect can arrive
        // before GNS reports the old path closed (notably after NAT/interface
        // changes), so replace the stale route deterministically instead of
        // retaining two authoritative connections for the same peer.
        if (remotePeer) {
            for (auto it = _serverClients.begin(); it != _serverClients.end();) {
                if (it->transport && it->transport->isP2P() &&
                    it->transport->getRemotePeerId() == *remotePeer) {
                    it->transport->disconnect("superseded P2P peer session",
                                              DisconnectReason::Kicked);
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
        if (remotePeer) {
            child->adoptIncomingP2PConnection(incoming, *remotePeer, p2pVirtualPort);
        } else {
            child->adoptIncomingConnection(incoming);
        }

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
            rawChild->disconnect("connection admission rejected",
                                 DisconnectReason::Kicked);
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
                if (!connected && newS != GnsConnectionState::Disconnected) return;
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
                if (!connected && newS != GnsConnectionState::Disconnected) return;
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
            case kMsgTypeClientInput:
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
        // R6 C6: single-thread; no lock.
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
                    inputSeq = static_cast<uint32_t>(body[0]) |
                               (static_cast<uint32_t>(body[1]) << 8) |
                               (static_cast<uint32_t>(body[2]) << 16) |
                               (static_cast<uint32_t>(body[3]) << 24);
                    serverTickAtSend = static_cast<uint32_t>(body[4]) |
                                       (static_cast<uint32_t>(body[5]) << 8) |
                                       (static_cast<uint32_t>(body[6]) << 16) |
                                       (static_cast<uint32_t>(body[7]) << 24);
                }
                std::vector<uint8_t> inBuf(16 + bodySize);
                auto writeU32 = [&inBuf](size_t offset, uint32_t value) {
                    inBuf[offset + 0] = static_cast<uint8_t>(value);
                    inBuf[offset + 1] = static_cast<uint8_t>(value >> 8);
                    inBuf[offset + 2] = static_cast<uint8_t>(value >> 16);
                    inBuf[offset + 3] = static_cast<uint8_t>(value >> 24);
                };
                writeU32(0, fromId);
                writeU32(4, inputSeq);
                writeU32(8, serverTickAtSend);
                const uint32_t blen = static_cast<uint32_t>(bodySize);
                writeU32(12, blen);
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
        // R6 C6: single-thread; no lock. _simulationInbound deque is drained
        // in FIFO order so the due[] list is itself deterministic.
        while (!_simulationInbound.empty()
               && _simulationInbound.front().targetSimTick <= simTick) {
            due.push_back(std::move(_simulationInbound.front()));
            _simulationInboundBytes -= due.back().body.size();
            _simulationInbound.pop_front();
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
        if (_p2pSignaling && _p2pSignaling->isRunning()) {
            (void)_p2pSignaling->poll(
                [this](const PeerId& sender, const void* data, size_t size) {
                    (void)GnsConnection::receiveP2PSignal(
                        sender, data, size, _p2pSignaling);
                },
                _limits.maxPumpMessages);
        }
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

    void clearServerClients(const char* reason,
                            DisconnectReason code = DisconnectReason::HostShutdown) {
        for (auto& record : _serverClients) {
            if (_extension && record.facade &&
                !record.extensionDisconnectNotified) {
                _extension->onConnectionDisconnected(record.facade.get());
                record.extensionDisconnectNotified = true;
            }
            if (record.transport) record.transport->disconnect(reason, code);
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
        case DeferredControlType::ConnectP2P:
            (void)connectP2PNow(PeerId{std::move(control.address)});
            break;
        case DeferredControlType::ListenP2P:
            (void)listenP2PNow();
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
    bool configureP2P(const P2PConfig& config,
                      std::shared_ptr<ISignalingTransport> signaling) override {
        if (!_initialized || !config.isValid() || !signaling ||
            _clientConn || _serverConn || !_serverClients.empty() || _p2pListening ||
            GnsConnection::isPumping()) {
            return false;
        }
        if (!signaling->start(config.localPeerId)) return false;
        if (!GnsConnection::setLocalP2PIdentity(config.localPeerId)) {
            signaling->stop();
            return false;
        }
        if (_p2pSignaling && _p2pSignaling != signaling) _p2pSignaling->stop();
        _p2pConfig = config;
        _p2pSignaling = std::move(signaling);
        _p2pConfigured = true;
        return true;
    }

    bool listenP2P() override {
        if (GnsConnection::isPumping()) {
            _deferredControl = {DeferredControlType::ListenP2P, {}, 0};
            return _p2pConfigured;
        }
        return listenP2PNow();
    }

    bool listenP2PNow() {
        if (!_p2pConfigured || !_p2pSignaling || !_p2pSignaling->isRunning()) return false;
        if (_serverConn) {
            _serverConn->disconnect("superseded by listenP2P()");
            _serverConn.reset();
        }
        clearServerClients("superseded by listenP2P()");
        if (_p2pListening) {
            GnsConnection::clearP2PAdoptFactory(_p2pConfig.virtualPort);
        }
        if (!GnsConnection::setP2PAdoptFactory(
            _p2pConfig, _p2pSignaling,
            [this](HSteamNetConnection incoming, const PeerId& remotePeer) {
                return adoptIncomingClient(incoming, &remotePeer, _p2pConfig.virtualPort);
            })) return false;
        _p2pListening = true;
        _mode = ConnectionMode::ListenServer;
        return true;
    }

    bool connectP2P(const PeerId& remotePeer) override {
        if (!remotePeer.isValid()) return false;
        if (GnsConnection::isPumping()) {
            _deferredControl = {
                DeferredControlType::ConnectP2P,
                remotePeer.value,
                _p2pConfig.virtualPort,
            };
            return _p2pConfigured;
        }
        return connectP2PNow(remotePeer);
    }

    bool connectP2PNow(const PeerId& remotePeer) {
        if (!_p2pConfigured || !_p2pSignaling || !_p2pSignaling->isRunning() ||
            !remotePeer.isValid() || remotePeer == _p2pConfig.localPeerId) {
            return false;
        }
        if (_clientConn) {
            _clientConn->disconnect("superseded by connectP2P()");
            _clientConn.reset();
            _clientNetConn.reset();
        }
        auto transport = std::make_unique<GnsConnection>();
        transport->setProtocolVersion(_protocolVersion);
        if (!transport->initP2PClient(remotePeer, _p2pConfig, _p2pSignaling)) return false;
        const uint32_t clientNetId = allocateNetId();
        transport->setNetId(clientNetId);
        transport->attachFaultController(&_faultCtl);
        hookGnsConnection(transport.get());
        _clientConn = std::move(transport);
        _clientNetConn = std::make_unique<NetConnectionImpl>(_clientConn.get(), clientNetId);
        _mode = _p2pListening ? ConnectionMode::ListenServer : ConnectionMode::Client;
        installInboundRoutes();
        if (_connectionHandler) {
            _clientConn->onStateChange([this](GnsConnectionState, GnsConnectionState newState) {
                const bool connected = (newState == GnsConnectionState::Ready) ||
                    (newState == GnsConnectionState::Connected &&
                     _clientConn->getProtocolVersion() == 0);
                if (!connected && newState != GnsConnectionState::Disconnected) return;
                const DisconnectReason reason = connected
                    ? DisconnectReason::Unknown : _clientConn->getLastDisconnectReason();
                _connectionHandler(_clientNetConn.get(), connected, reason);
            });
        }
        return true;
    }

    bool isP2PConfigured() const override { return _p2pConfigured; }
    PeerId getLocalPeerId() const override {
        return _p2pConfigured ? _p2pConfig.localPeerId : PeerId{};
    }

    P2PConnectionInfo getP2PConnectionInfo(NetConnection* connection) const override {
        if (!_p2pConfigured) return {};
        if (!connection && _clientConn) {
            return _clientConn->getP2PConnectionInfo(_p2pConfig.localPeerId);
        }
        for (const auto& record : _serverClients) {
            if (record.facade.get() == connection && record.transport) {
                return record.transport->getP2PConnectionInfo(_p2pConfig.localPeerId);
            }
        }
        return {};
    }

    std::vector<P2PPeerInfo> getP2PPeers() const override {
        std::vector<P2PPeerInfo> peers;
        const auto append = [this, &peers](const GnsConnection* transport,
                                           const NetConnection* facade,
                                           bool remoteIsHost) {
            if (!transport || !transport->isP2P() ||
                transport->getState() == GnsConnectionState::Disconnected) {
                return;
            }
            const auto connection =
                transport->getP2PConnectionInfo(_p2pConfig.localPeerId);
            if (!connection.remotePeerId.isValid()) return;

            P2PPeerInfo peer;
            peer.peerId = connection.remotePeerId;
            peer.connectionId = facade ? facade->getId() : transport->getNetId();
            peer.path = connection.path;
            peer.remoteAddress = connection.remoteAddress;
            peer.pingMs = connection.pingMs;
            peer.isSessionHost = remoteIsHost;
            switch (transport->getState()) {
            case GnsConnectionState::Ready:
                peer.state = P2PPeerState::Ready;
                break;
            case GnsConnectionState::Disconnecting:
                peer.state = P2PPeerState::Disconnecting;
                break;
            case GnsConnectionState::Connected:
                peer.state = transport->getProtocolVersion() == 0
                    ? P2PPeerState::Ready : P2PPeerState::Connecting;
                break;
            default:
                peer.state = P2PPeerState::Connecting;
                break;
            }
            peers.push_back(std::move(peer));
        };

        append(_clientConn.get(), _clientNetConn.get(), !_p2pListening);
        for (const auto& record : _serverClients) {
            append(record.transport.get(), record.facade.get(), false);
        }
        std::sort(peers.begin(), peers.end(),
                  [](const P2PPeerInfo& lhs, const P2PPeerInfo& rhs) {
                      if (lhs.peerId.value != rhs.peerId.value) {
                          return lhs.peerId.value < rhs.peerId.value;
                      }
                      return lhs.connectionId < rhs.connectionId;
                  });
        return peers;
    }

    P2PSessionInfo getP2PSessionInfo() const override {
        P2PSessionInfo session;
        if (!_p2pConfigured) return session;

        session.state = P2PSessionState::Idle;
        session.localPeerId = _p2pConfig.localPeerId;
        session.virtualPort = _p2pConfig.virtualPort;
        if (_p2pListening) {
            session.role = P2PSessionRole::Host;
            session.hostPeerId = _p2pConfig.localPeerId;
            session.state = P2PSessionState::Hosting;
        } else if (_clientConn && _clientConn->isP2P()) {
            session.role = P2PSessionRole::Client;
            session.hostPeerId = _clientConn->getRemotePeerId();
            session.state = P2PSessionState::Connecting;
        }

        for (const auto& peer : getP2PPeers()) {
            if (peer.state == P2PPeerState::Ready) ++session.readyPeerCount;
        }
        if (session.readyPeerCount != 0) session.state = P2PSessionState::Active;
        return session;
    }

    NetConnection* findP2PPeer(const PeerId& peer) const override {
        if (!peer.isValid()) return nullptr;
        if (_clientConn && _clientNetConn && _clientConn->isP2P() &&
            _clientConn->getState() != GnsConnectionState::Disconnected &&
            _clientConn->getRemotePeerId() == peer) {
            return _clientNetConn.get();
        }
        for (const auto& record : _serverClients) {
            if (record.transport && record.facade && record.transport->isP2P() &&
                record.transport->getState() != GnsConnectionState::Disconnected &&
                record.transport->getRemotePeerId() == peer) {
                return record.facade.get();
            }
        }
        return nullptr;
    }

    bool disconnectP2PPeer(const PeerId& peer, const char* reason) override {
        if (!peer.isValid()) return false;
        if (_clientConn && _clientConn->isP2P() &&
            _clientConn->getState() != GnsConnectionState::Disconnected &&
            _clientConn->getRemotePeerId() == peer) {
            _clientConn->disconnect(reason ? reason : "left P2P session",
                                    DisconnectReason::UserQuit);
            return true;
        }
        for (auto& record : _serverClients) {
            if (record.transport && record.transport->isP2P() &&
                record.transport->getState() != GnsConnectionState::Disconnected &&
                record.transport->getRemotePeerId() == peer) {
                record.transport->disconnect(reason ? reason : "removed from P2P session",
                                             DisconnectReason::Kicked);
                return true;
            }
        }
        return false;
    }

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
        if (_p2pListening) {
            GnsConnection::clearP2PAdoptFactory(_p2pConfig.virtualPort);
            _p2pListening = false;
        }
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
                if (!connected && newS != GnsConnectionState::Disconnected) return;
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
        if (_p2pListening) {
            GnsConnection::clearP2PAdoptFactory(_p2pConfig.virtualPort);
            _p2pListening = false;
        }
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
        if (_p2pListening) {
            GnsConnection::clearP2PAdoptFactory(_p2pConfig.virtualPort);
            _p2pListening = false;
        }
        if (_clientConn) {
            _clientConn->disconnect("client disconnect", DisconnectReason::UserQuit);
            _clientConn.reset();
            _clientNetConn.reset();
        }
        if (_serverConn) {
            _serverConn->disconnect("server shutdown");
            _serverConn.reset();
        }
        clearServerClients("server shutdown");
        // R6 C6: single-thread; no lock.
        _simulationInbound.clear();
        _simulationInboundBytes = 0;
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
        return _p2pListening || (_serverConn &&
            _serverConn->getInnerListenSocket() != k_HSteamListenSocket_Invalid);
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
                    if (!connected && newS != GnsConnectionState::Disconnected) return;
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
        for (auto& record : _serverClients) {
            if (record.facade.get() == conn && record.transport) {
                record.transport->disconnect(reason ? reason : "kicked by server",
                                             DisconnectReason::Kicked);
                return;
            }
        }
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

    // R6 C9 (2026-08-25): state-equal hashing. FNV-1a 64-bit over the
    // observable, post-decode state — replication objects (bytewise), the
    // sorted set of pending RPC callIds, the current server tick, and the
    // per-connection acknowledged-sequence cursors. HashKind::StatePlusProfiler
    // also folds in profiler counter totals so tests can opt into stricter
    // assertions when profiling is exercised in the same run.
    uint64_t computeStateHash(HashKind kind) override {
        return computeStateHashInternal(kind == HashKind::StatePlusProfiler);
    }

    // R6 C9 (2026-08-25): override the virtual test seam declared on
    // INetworkSubSystem — see `interface/AYNetwork/INetwork.h`.
    ReplicationManager* getReplicationManagerForTesting() override {
        return &_replicationManager;
    }

    // R6.5-3 (2026-08-25): replay-pump bridge. The recorded event arrives
    // in the same byte layout that `NetworkReplayRecorderAdapter` wrote
    // (see interface/AYNetwork/Replay/NetworkReplayRecorderAdapter.cpp
    // for the per-event prefix sizes). The bridge strips the adapter
    // prefix and routes the remaining body to the matching live seam:
    //
    //   kEvtNet_InitialFullSnapshot → _replicationManager.onReceive(kMsgTypeReplication, ...)
    //   kEvtNet_DeltaSnapshot       → _replicationManager.onReceive(kMsgTypeDelta, ...)
    //   kEvtNet_Spawn               → _replicationManager.onReceive(kMsgTypeEntitySpawn, ...)
    //   kEvtNet_Despawn             → _replicationManager.onReceive(kMsgTypeEntityDespawn, ...)
    //   kEvtNet_InputBatch          → _replicationManager.onClientInput(connectionId, body, len)
    //   kEvtNet_RpcBatch            → _rpcHandler.onRpcRequest / onRpcResponse / onRpcReject
    //                                  (demuxed on the recorded messageType)
    //   kEvtNet_AuthorityChange     → _replicationManager.setObjectProxyKind(netId, newKind)
    //
    // `from` is null — replay events originate from the recording, not a
    // live GnsConnection. Predicted-input records and Snapshot/Deltahave
    // no transport peer, so we use nullptr here.
    void tickRecordedEvent(uint32_t eventType,
                           const uint8_t* payload,
                           size_t size) override {
        if (!payload && size != 0) return;
        using ayt::net::replay::kEvtNet_AuthorityChange;
        using ayt::net::replay::kEvtNet_Checkpoint;
        using ayt::net::replay::kEvtNet_DeltaSnapshot;
        using ayt::net::replay::kEvtNet_Despawn;
        using ayt::net::replay::kEvtNet_InitialFullSnapshot;
        using ayt::net::replay::kEvtNet_InputBatch;
        using ayt::net::replay::kEvtNet_RpcBatch;
        using ayt::net::replay::kEvtNet_Spawn;

        switch (eventType) {
        case kEvtNet_Checkpoint: {
            // [u32 count], then count entries of
            // [u32 netId][u32 bodySize][replication body].
            if (size < 4) return;
            auto readU32 = [](const uint8_t* p) {
                return static_cast<uint32_t>(p[0]) |
                       (static_cast<uint32_t>(p[1]) << 8) |
                       (static_cast<uint32_t>(p[2]) << 16) |
                       (static_cast<uint32_t>(p[3]) << 24);
            };
            const uint32_t count = readU32(payload);
            size_t offset = 4;
            for (uint32_t i = 0; i < count; ++i) {
                if (offset > size || size - offset < 8) return;
                const uint32_t bodySize = readU32(payload + offset + 4);
                offset += 8;
                if (bodySize > size - offset) return;
                BitStream stream(const_cast<uint8_t*>(payload + offset), bodySize);
                stream.resetForRead();
                (void)_replicationManager.onReceive(kMsgTypeReplication, stream, nullptr);
                offset += bodySize;
            }
            return;
        }
        case kEvtNet_InitialFullSnapshot: {
            // Prefix: [u32 connId][u8 frameFlags]; body follows.
            if (size < 5) return;
            const uint8_t* body = payload + 5;
            const size_t   bodySize = size - 5;
            BitStream stream(const_cast<uint8_t*>(body), bodySize);
            stream.resetForRead();
            (void)_replicationManager.onReceive(kMsgTypeReplication, stream, nullptr);
            return;
        }
        case kEvtNet_DeltaSnapshot: {
            if (size < 5) return;
            const uint8_t* body = payload + 5;
            const size_t   bodySize = size - 5;
            BitStream stream(const_cast<uint8_t*>(body), bodySize);
            stream.resetForRead();
            (void)_replicationManager.onReceive(kMsgTypeDelta, stream, nullptr);
            return;
        }
        case kEvtNet_Spawn: {
            // Prefix: [u32 connId][u32 netId][u64 schemaHash]; body follows.
            if (size < 16) return;
            const uint8_t* body = payload + 16;
            const size_t   bodySize = size - 16;
            BitStream stream(const_cast<uint8_t*>(body), bodySize);
            stream.resetForRead();
            (void)_replicationManager.onReceive(kMsgTypeEntitySpawn, stream, nullptr);
            return;
        }
        case kEvtNet_Despawn: {
            // Prefix: [u32 connId][u32 netId]; no body.
            if (size < 8) return;
            const uint32_t netId =
                static_cast<uint32_t>(payload[4])        |
                (static_cast<uint32_t>(payload[5]) <<  8) |
                (static_cast<uint32_t>(payload[6]) << 16) |
                (static_cast<uint32_t>(payload[7]) << 24);
            uint8_t netIdBuf[4] = {
                static_cast<uint8_t>(netId        & 0xFF),
                static_cast<uint8_t>((netId >>  8) & 0xFF),
                static_cast<uint8_t>((netId >> 16) & 0xFF),
                static_cast<uint8_t>((netId >> 24) & 0xFF),
            };
            BitStream stream(netIdBuf, sizeof(netIdBuf));
            stream.resetForRead();
            (void)_replicationManager.onReceive(kMsgTypeEntityDespawn, stream, nullptr);
            return;
        }
        case kEvtNet_InputBatch: {
            // Prefix: [u32 connId][u32 inputSeq][u32 serverTickAtSend][u32 payloadLen]; body follows.
            if (size < 16) return;
            const uint32_t connId =
                static_cast<uint32_t>(payload[0])        |
                (static_cast<uint32_t>(payload[1]) <<  8) |
                (static_cast<uint32_t>(payload[2]) << 16) |
                (static_cast<uint32_t>(payload[3]) << 24);
            const uint32_t declaredLen =
                static_cast<uint32_t>(payload[12])        |
                (static_cast<uint32_t>(payload[13]) <<  8) |
                (static_cast<uint32_t>(payload[14]) << 16) |
                (static_cast<uint32_t>(payload[15]) << 24);
            const size_t avail = (size > 16) ? (size - 16) : 0;
            if (declaredLen > avail) return;
            (void)_replicationManager.onClientInput(connId, payload + 16, declaredLen);
            return;
        }
        case kEvtNet_RpcBatch: {
            // Prefix: [u16 messageType][u32 connId][u32 bodyLen]; body follows.
            if (size < 10) return;
            const uint16_t msgType =
                static_cast<uint16_t>(payload[0])        |
                (static_cast<uint16_t>(payload[1]) <<  8);
            const uint32_t declaredLen =
                static_cast<uint32_t>(payload[6])        |
                (static_cast<uint32_t>(payload[7]) <<  8) |
                (static_cast<uint32_t>(payload[8]) << 16) |
                (static_cast<uint32_t>(payload[9]) << 24);
            const size_t avail = (size > 10) ? (size - 10) : 0;
            if (declaredLen > avail) return;
            const size_t take = declaredLen;
            // Build a BitStream over the recorded body and route to the
            // matching RPC handler. The body is captured as a side buffer
            // because the decoder's payload pointer outlives this scope.
            std::vector<uint8_t> body(take);
            if (take > 0) std::memcpy(body.data(), payload + 10, take);
            BitStream stream(body.data(), body.size());
            stream.resetForRead();
            switch (msgType) {
            case kMsgTypeRpcRequest:
                (void)_rpcHandler.onRpcRequest(stream, nullptr);
                return;
            case kMsgTypeRpcResponse:
                (void)_rpcHandler.onRpcResponse(stream, nullptr);
                return;
            case kMsgTypeRpcReject:
                (void)_rpcHandler.onRpcReject(stream, nullptr);
                return;
            default:
                // Unknown RPC envelope kind — drop silently. R7 may surface
                // a counter for diagnostics.
                return;
            }
        }
        case kEvtNet_AuthorityChange: {
            // Prefix: [u32 netId][u8 oldKind][u8 newKind][u32 connId][u32 reserved].
            if (size < 14) return;
            const uint32_t netId =
                static_cast<uint32_t>(payload[0])        |
                (static_cast<uint32_t>(payload[1]) <<  8) |
                (static_cast<uint32_t>(payload[2]) << 16) |
                (static_cast<uint32_t>(payload[3]) << 24);
            const uint8_t newKind = payload[5];
            const uint32_t connectionId =
                static_cast<uint32_t>(payload[6])        |
                (static_cast<uint32_t>(payload[7]) <<  8) |
                (static_cast<uint32_t>(payload[8]) << 16) |
                (static_cast<uint32_t>(payload[9]) << 24);
            _replicationManager.setObjectProxyKind(
                netId, static_cast<ProxyKind>(newKind), connectionId);
            return;
        }
        default:
            // Foundation events (SessionBegin/SessionEnd/TextMarker) and
            // future adapter event types in the AYNetwork range are
            // intentionally dropped here — they are session-level markers,
            // not per-tick state mutations.
            return;
        }
    }

    // R6.5-3 (2026-08-25): RNG seed install seam. The v2 replay player
    // exposes `ReplayFileHeader.randomSeed` so a deterministic playback
    // can latch the seed for downstream RNG consumers (RngApi, fault
    // controller, RPC tie-break). Today this is a stub field; R7 will
    // route through `TransportFaultController::setSessionSeed` and
    // surface a public accessor.
    void installReplayRngSeed(uint64_t seed) override {
        _replayRngSeed = seed;
        _replayRngSeedSet = true;
        _faultCtl.setSessionSeed(seed);
    }

    // R6.5-3 (2026-08-25): read-back accessors for the bridge state. The
    // tests assert the recorded seed round-trips through the subsystem
    // without going through a live RNG yet. `override` matches the
    // virtuals on INetworkSubSystem.
    bool     hasReplayRngSeed() const override { return _replayRngSeedSet; }
    uint64_t getReplayRngSeed() const override { return _replayRngSeed; }

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

        // R6 C5 H-09: forward the live server tick to RpcHandler so
        // outbound RPC replay events carry the canonical tick (was 0).
        _rpcHandler.setReplayTickSource(
            [this]() { return _replicationManager.getServerTick(); });

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
    P2PConfig _p2pConfig{};
    std::shared_ptr<ISignalingTransport> _p2pSignaling;
    bool _p2pConfigured = false;
    bool _p2pListening = false;
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
    // R6 C6 (2026-08-25): single-thread model — the over-defensive
    // _simulationInboundMutex has been removed. The inbound queue is
    // drained on the main thread only.
    std::deque<PendingSimulationInbound> _simulationInbound;
    size_t _simulationInboundBytes = 0;
    uint64_t _ingressTargetSimTick = 0;
    bool _stagedIngress = false;

    // R6 C9 (2026-08-25): state-equal hashing — see computeStateHash().
    // Implemented out-of-line below the class so the member functions it
    // calls stay free of FNV mix details. Iteration is in sorted-key order
    // so the hash is bitwise stable across runs / compilers.
    uint64_t computeStateHashInternal(bool includeProfiler);

    // R6.5-3 (2026-08-25): replay-pump RNG seed stub. Captured via
    // installReplayRngSeed() so the recorded seed can be inspected by
    // tests; full propagation into TransportFaultController /
    // prediction RNG is R7+.
    uint64_t _replayRngSeed    = 0;
    bool     _replayRngSeedSet = false;
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

// =============================================================================
// R6 C9 (2026-08-25): state-equal hash helper. Out-of-line to keep the
// class body focused on subsystem state. FNV-1a 64-bit (offset basis
// 0xCBF29CE484222325; prime 0x100000001B3). Order-independent on the keys
// it iterates (per-connection cursors are summed in netId order so the
// hash is bitwise stable across runs).
// =============================================================================
namespace {

constexpr uint64_t kFnv1aOffsetBasis = 0xCBF29CE484222325ULL;
constexpr uint64_t kFnv1aPrime        = 0x100000001B3ULL;

inline void fnv1aMix(uint64_t& h, const void* data, size_t bytes) {
    const uint8_t* p = static_cast<const uint8_t*>(data);
    for (size_t i = 0; i < bytes; ++i) {
        h ^= static_cast<uint64_t>(p[i]);
        h *= kFnv1aPrime;
    }
}

inline void fnv1aMixU32(uint64_t& h, uint32_t v) {
    fnv1aMix(h, &v, sizeof(v));
}

inline void fnv1aMixU64(uint64_t& h, uint64_t v) {
    fnv1aMix(h, &v, sizeof(v));
}

} // anonymous namespace

uint64_t NetworkSubSystem::computeStateHashInternal(bool includeProfiler) {
    uint64_t h = kFnv1aOffsetBasis;

    // Server tick — fold the timeline first so two recordings of
    // identical play at different baseline ticks differ.
    fnv1aMixU32(h, _replicationManager.getServerTick());
    // Server tick rate (double; bit-cast to uint64 for stable folding
    // across compilers). Even though the value is configured at
    // startup, two recordings that installed different rates would
    // diverge here.
    fnv1aMixU64(h, std::bit_cast<uint64_t>(_replicationManager.getServerTickRate()));

    // Replication objects: pull the bytewise snapshot for every registered
    // netId in sorted order. The records are bitwise-stable because the
    // BitStream write paths use round-half-up (R6 C7) and the iteration
    // here is sorted.
    std::vector<uint32_t> netIds = _replicationManager.knownNetIdsForHash();
    std::sort(netIds.begin(), netIds.end());
    for (uint32_t n : netIds) {
        fnv1aMixU32(h, n);
        std::vector<uint8_t> bytes;
        if (_replicationManager.serializeObjectForHash(n, bytes)) {
            fnv1aMix(h, bytes.data(), bytes.size());
        }
    }

    // Pending RPC call ids (sorted).
    std::vector<uint64_t> rpcKeys = _rpcHandler.pendingCallIdsForHash();
    std::sort(rpcKeys.begin(), rpcKeys.end());
    for (uint64_t k : rpcKeys) fnv1aMixU64(h, k);

    // Per-connection ack cursors (sorted by netId).
    std::vector<uint32_t> ackKeys = _replicationManager.ackedSeqKeysForHash();
    std::sort(ackKeys.begin(), ackKeys.end());
    for (uint32_t n : ackKeys) {
        fnv1aMixU32(h, n);
        fnv1aMixU32(h, _replicationManager.ackedSeqForHash(n));
    }

    // Profiler counter totals (opt-in).
    if (includeProfiler) {
        std::vector<uint32_t> pkeys = _profiler.connectionKeysForHash();
        std::sort(pkeys.begin(), pkeys.end());
        for (uint32_t n : pkeys) {
            fnv1aMixU32(h, n);
            fnv1aMixU64(h, _profiler.cumulativeSendForHash(n));
            fnv1aMixU64(h, _profiler.cumulativeRecvForHash(n));
        }
    }

    return h;
}

} // namespace ayt::net
