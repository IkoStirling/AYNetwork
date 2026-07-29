#pragma once
// RpcHandler.h - R4.0 RPC dispatcher for AYNetwork
//
// R4.0 (2026-07-29): Server / Client / Multicast RPC, parameter
// (de)serialization via the R3.2 16-WireTypeId dispatch, server-side
// validator hook on IMethodInfo::validate. Wire schemaVersion = 1
// (R3.x receivers ignore 0x0010..0x0012 envelope slots and drop
// the frame silently, same as R3.2 nested WireTypeId 12..15).
//
// See design.md §13 R4 checklist and §10 Phase 4 for the full spec.

#include <IAYNetwork.h>
#include <ayreflect/IReflect.h>

#include <atomic>
#include <cstdint>
#include <cstddef>
#include <functional>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace ayt::net
{

// ===== R4.0 reject reason codes (RpcReject body) =====
enum class RpcRejectReason : uint8_t {
    UnknownMethod = 0,
    ValidatorDeny = 1,
    ParseFail     = 2,
    NotAuthority  = 3,
};

// ===== RpcSerializer thin wrappers =====
//
// R4.0: writeRpcArgs / readRpcArgs wrap ReflectSerializer::writeWireValue /
// readWireValue (the R3.2 16-WireTypeId recursive dispatch). Tests pin
// round-trip correctness without depending on RPC plumbing.
//
// writeRpcResponse / readRpcResponse handle the optional return value.
// writeRpcReject / readRpcReject carry the (callId, reason) pair.
class RpcSerializer {
public:
    // ===== Request body (RpcCallFrame) =====
    // Wire: [u8 rpcKind][u16 methodHash][u64 callId][u8 argCount][u8 reserved]
    // [arg record × argCount]
    // Each arg record: [u16 argNameHash][u8 wireTypeId][writeWireValue(...)]
    //
    // `methodInfo`: IMethodInfo* whose getParamCount() must equal argCount.
    // `args`: pointer table of length argCount; each slot is a raw byte
    // buffer holding the corresponding arg value (i.e. `*(T*)args[i]`).
    //
    // Returns true if all writes succeeded. False on truncated stream or
    // a WireTypeId resolution failure (caller side).
    static bool writeRpcArgs(BitStream& s,
                             const ayt::reflect::ITypeInfo* /*returnType*/,
                             const ayt::reflect::IMethodInfo* methodInfo,
                             ayt::reflect::RpcKind rpcKind,
                             uint16_t methodHash,
                             uint64_t callId,
                             const void* const* args,
                             size_t argCount);

    // Read the inverse of writeRpcArgs. On success, populates
    // `methodInfoOut` (resolved by methodHash against the registered
    // methods table), `callIdOut`, `rpcKindOut`, and writes arg bytes
    // into a freshly allocated `argBuf` buffer (caller frees after
    // invoking). Returns false on truncated / unknown-method / wire-
    // type-resolve failure.
    static bool readRpcArgs(BitStream& s,
                            const std::unordered_map<uint16_t, const ayt::reflect::IMethodInfo*>& methodsByHash,
                            ayt::reflect::RpcKind& rpcKindOut,
                            uint16_t& methodHashOut,
                            uint64_t& callIdOut,
                            const ayt::reflect::IMethodInfo*& methodInfoOut,
                            std::vector<uint8_t>& argBufOut);

    // ===== Response body =====
    // Wire: [u64 callId][u8 hasReturn][u8 returnWid][return bytes]
    static bool writeRpcResponse(BitStream& s,
                                 const ayt::reflect::ITypeInfo* returnType,
                                 uint64_t callId,
                                 const void* returnValue);
    static bool readRpcResponse(BitStream& s,
                                uint64_t& callIdOut,
                                bool& hasReturnOut,
                                WireTypeId& returnWidOut,
                                std::vector<uint8_t>& returnBufOut);

    // ===== Reject body =====
    // Wire: [u64 callId][u8 reason]
    static bool writeRpcReject(BitStream& s, uint64_t callId, RpcRejectReason reason);
    static bool readRpcReject(BitStream& s, uint64_t& callIdOut, RpcRejectReason& reasonOut);
};

// ===== RpcHandler =====
//
// One per AYNetworkSubSystem (mirrors ReplicationManager ownership).
// Mirrors the R3.x ReplicationManager test-seam surface:
//   - setModeForTesting(mode) — forces Server/Client without an
//     INetworkSubSystem, lets unit tests drive RPC paths.
//   - setBroadcastSinkForTesting(sink) — captures every sealed frame
//     the dispatcher would have sent on the wire. Production code
//     never sets this.
//
// Wire envelope routing (envelope msgType → handler):
//   0x0010 kMsgTypeRpcRequest  → onRpcRequest
//   0x0011 kMsgTypeRpcResponse → onRpcResponse
//   0x0012 kMsgTypeRpcReject   → onRpcReject
//
// Channels: all RPC envelopes default to CHANNEL_RELIABLE; per-RPC
// `IMethodInfo::isUnreliable()` overrides to CHANNEL_UNRELIABLE.
// RpcResponse / RpcReject always RELIABLE — caller must receive them.
//
// callId: 64-bit caller-side random; matched by Response / Reject.
// The pending map keeps a callback slot per callId for the duration of
// the call (auto-cleaned on Response/Reject or after RpcDefaultTimeoutMs).
class RpcHandler {
public:
    explicit RpcHandler(INetworkSubSystem* network);
    ~RpcHandler();

    // ===== Registration =====
    //
    // Binds an instance method as an RPC handler. Looks up
    // `ITypeInfo*(typeName)` and `IMethodInfo*(methodName)`, verifies
    // the method's getRpcKind() != RpcKind::None, and stores the
    // binding under methodHash (FNV-1a-32 of methodName low-16).
    //
    // `obj` must outlive the RpcHandler OR until unregisterMethod().
    // For stateless handlers pass a singleton.
    //
    // Returns false if type/method not registered, the method's
    // RpcKind is None, or `obj` is null.
    bool registerMethod(const char* typeName, const char* methodName, void* obj);
    void unregisterMethod(const char* typeName, const char* methodName);

    // ===== Outbound call sites =====
    //
    // The caller passes `args` as a pointer table of length `argCount`,
    // each slot a raw byte buffer holding the argument's value
    // (i.e. memcpy from the source into args[i]).
    //
    // `argNames` carries an optional name per arg — used for the
    // on-wire nameHash discriminator. nullptr falls back to positional
    // indices (slower but correct).
    //
    // On success returns true and assigns the assigned callId to
    // `outCallId` for caller-side tracking (pairs with Response).
    //
    // Note: callServer / callClient / callMulticast are not yet wired
    // up to an INetworkSubSystem in R4.0 — the unit tests exercise the
    // pure (de)serialize + invoke paths. The full broker (sendTo,
    // broadcast) is plumbing that lands with R4.1 Interest Management.
    bool callServer(const char* typeName, const char* methodName,
                    const void* const* args, const char* const* argNames,
                    size_t argCount, uint64_t& outCallId);
    bool callClient(uint32_t targetNetId, const char* typeName, const char* methodName,
                    const void* const* args, const char* const* argNames,
                    size_t argCount, uint64_t& outCallId);
    bool callMulticast(const char* typeName, const char* methodName,
                       const void* const* args, const char* const* argNames,
                       size_t argCount, uint64_t& outCallId);

    // ===== Inbound handlers =====
    //
    // Wired into AYNetworkSubSystem::update: when a sealed frame's
    // PacketHeader.msgType matches kMsgTypeRpc*, the body bytes are
    // handed to one of these methods. They are public for tests.
    bool onRpcRequest(BitStream& body, NetConnection* from);
    bool onRpcResponse(BitStream& body, NetConnection* from);
    bool onRpcReject(BitStream& body, NetConnection* from);

    // ===== Pending call tracking =====
    //
    // Outbound callers can register a callback that fires when the
    // matching Response or Reject arrives. Auto-cleaned when matched.
    using RpcCallback = std::function<void(bool /*accepted*/, const void* /*returnValueOrNull*/)>;
    void registerPending(uint64_t callId, RpcCallback cb) {
        std::lock_guard<std::mutex> lk(_pendingCallsMutex);
        _pendingCalls[callId] = std::move(cb);
    }
    bool hasPending(uint64_t callId) const {
        std::lock_guard<std::mutex> lk(_pendingCallsMutex);
        return _pendingCalls.find(callId) != _pendingCalls.end();
    }
    size_t pendingCount() const {
        std::lock_guard<std::mutex> lk(_pendingCallsMutex);
        return _pendingCalls.size();
    }

    // ===== Test seams =====
    void setModeForTesting(ConnectionMode mode) { _forcedMode = mode; }
    ConnectionMode getEffectiveMode() const {
        if (_forcedMode != ConnectionMode::Disconnected) return _forcedMode;
        return _network ? _network->getMode() : ConnectionMode::Disconnected;
    }
    using BroadcastSink = std::function<void(uint8_t channel, const void* data, size_t size)>;
    void setBroadcastSinkForTesting(BroadcastSink sink) { _broadcastSink = std::move(sink); }

    // ===== Registry access (for tests + downstream RpcSerializer callers) =====
    //
    // Map of methodHash(FNV-1a-32 low-16) → IMethodInfo for every
    // registered RPC. Built once by registerMethod; mutated by
    // unregisterMethod. Read-only outside the class.
    const std::unordered_map<uint16_t, const ayt::reflect::IMethodInfo*>& methodsByHash() const {
        return _methodsByHash;
    }

private:
    // Resolve (methodName → methodHash), find IMethodInfo binding. Internal
    // helper used by callServer / callClient / callMulticast and inbound handlers.
    bool resolveMethod(const char* typeName, const char* methodName,
                       const ayt::reflect::IMethodInfo*& outInfo,
                       void*& outObj,
                       uint16_t& outHash) const;

    // Send helper used by both callXxx paths and outbound Response/Reject.
    bool emit(uint8_t channel, uint16_t envelopeMsgType, const BitStream& body);

    INetworkSubSystem* _network = nullptr;
    INetworkExtension* _extension = nullptr;
    ConnectionMode _forcedMode = ConnectionMode::Disconnected;
    BroadcastSink  _broadcastSink = nullptr;

    std::atomic<uint64_t> _nextCallId{1};
    std::unordered_map<uint16_t, const ayt::reflect::IMethodInfo*> _methodsByHash;
    std::unordered_map<uint16_t, void*>                            _objsByHash;

    mutable std::mutex                       _pendingCallsMutex;
    std::unordered_map<uint64_t, RpcCallback> _pendingCalls;
};

} // namespace ayt::net
