// RpcHandler.cpp - R4.0 RPC dispatcher implementation
//
// R4.0 (2026-07-29): Routes envelope msgType 0x0010/0x0011/0x0012
// between AYNetworkSubSystem and AYReflect-decorated methods on
// registered instances. Parameter (de)serialization reuses the R3.2
// writeWireValue / readWireValue recursive 16-WireTypeId dispatch.
//
// See design.md §13 R4 checklist and design.md §10 Phase 4.

#include <RPC/RpcHandler.h>

#include <Replication/ReflectSerializer.h>
#include <Protocol/PacketCodec.h>
#include <Transport/GnsConnection.h>

#include <ayreflect/IReflect.h>
#include <ayreflect/ReflectRegistry.h>

#include <cstring>
#include <cstdio>
#include <chrono>
#include <condition_variable>
#include <atomic>
#include <queue>
#include <thread>
#include <vector>

namespace ayt::net
{

namespace {

// FNV-1a 32-bit hash of a C-string. Returns the low 16 bits so the
// resulting slot fits in the wire's two-byte methodHash / fieldNameHash
// discriminator. Reuses the same hash as ReflectSerializer::hashFieldName
// (R3.0) — collisions are tolerable since Reflection annotations are
// closed and small.
inline uint16_t fnv1a16(const char* s) {
    if (!s) return 0;
    uint32_t h = 0x811C9DC5u;
    while (*s) {
        h ^= static_cast<uint8_t>(*s++);
        h *= 16777619u;
    }
    return static_cast<uint16_t>(h & 0xFFFFu);
}

} // anonymous namespace

// =============================================================================
// R4.1-B: RpcAsyncPool — worker threads invoke isAsync RPCs; completions are
// drained on the network/game thread via RpcHandler::tick().
// =============================================================================
class RpcAsyncPool {
public:
    struct Job {
        const ayt::reflect::IMethodInfo* methodInfo = nullptr;
        void* obj = nullptr;
        std::vector<uint8_t> argBuf;
        uint64_t callId = 0;
        NetConnection* from = nullptr;
    };

    struct Completion {
        uint64_t callId = 0;
        NetConnection* from = nullptr;
        const ayt::reflect::ITypeInfo* returnType = nullptr;
        std::vector<uint8_t> returnBuf;
        bool hasReturn = false;
    };

    void start(size_t workerCount) {
        if (! _workers.empty()) return;
        _stop.store(false);
        if (workerCount == 0) workerCount = 1;
        _workers.reserve(workerCount);
        for (size_t i = 0; i < workerCount; ++i) {
            _workers.emplace_back([this]() { workerLoop(); });
        }
    }

    void shutdown() {
        _stop.store(true);
        _jobCv.notify_all();
        for (std::thread& t : _workers) {
            if (t.joinable()) t.join();
        }
        _workers.clear();
        {
            std::lock_guard<std::mutex> lk(_jobMutex);
            while (!_jobs.empty()) _jobs.pop();
        }
        {
            std::lock_guard<std::mutex> lk(_completionMutex);
            while (!_completions.empty()) _completions.pop();
        }
    }

    void submit(Job job) {
        {
            std::lock_guard<std::mutex> lk(_jobMutex);
            _jobs.push(std::move(job));
        }
        _jobCv.notify_one();
    }

    bool tryPopCompletion(Completion& out) {
        std::lock_guard<std::mutex> lk(_completionMutex);
        if (_completions.empty()) return false;
        out = std::move(_completions.front());
        _completions.pop();
        return true;
    }

private:
    static bool buildArgPtrTable(const ayt::reflect::IMethodInfo* methodInfo,
                                 const std::vector<uint8_t>& argBuf,
                                 std::vector<const void*>& argPtrsOut) {
        if (!methodInfo) return false;
        const size_t argCount = methodInfo->getParamCount();
        argPtrsOut.resize(argCount);
        size_t offset = 0;
        for (size_t i = 0; i < argCount; ++i) {
            const ayt::reflect::ITypeInfo* argType = methodInfo->getParamType(i);
            const size_t argSize = argType ? argType->getSize() : 0;
            if (offset + argSize > argBuf.size()) return false;
            argPtrsOut[i] = argBuf.data() + offset;
            offset += argSize;
        }
        return true;
    }

    void workerLoop() {
        while (true) {
            Job job;
            {
                std::unique_lock<std::mutex> lk(_jobMutex);
                _jobCv.wait(lk, [this]() { return _stop.load() || !_jobs.empty(); });
                if (_stop.load() && _jobs.empty()) return;
                job = std::move(_jobs.front());
                _jobs.pop();
            }

            Completion completion;
            completion.callId = job.callId;
            completion.from = job.from;
            if (!job.methodInfo || !job.obj) {
                completion.hasReturn = false;
            } else {
                std::vector<const void*> argPtrs;
                if (!buildArgPtrTable(job.methodInfo, job.argBuf, argPtrs)) {
                    completion.hasReturn = false;
                } else {
                    const void* retPtr = job.methodInfo->invoke(job.obj, argPtrs.data());
                    completion.returnType = job.methodInfo->getReturnType();
                    if (completion.returnType != nullptr && retPtr != nullptr) {
                        const size_t retSize = completion.returnType->getSize();
                        completion.returnBuf.resize(retSize);
                        std::memcpy(completion.returnBuf.data(), retPtr, retSize);
                        completion.hasReturn = true;
                    }
                }
            }

            {
                std::lock_guard<std::mutex> lk(_completionMutex);
                _completions.push(std::move(completion));
            }
        }
    }

    std::vector<std::thread> _workers;
    std::queue<Job> _jobs;
    std::queue<Completion> _completions;
    std::mutex _jobMutex;
    std::mutex _completionMutex;
    std::condition_variable _jobCv;
    std::atomic<bool> _stop{false};
};

// =============================================================================
// RpcSerializer implementation
// =============================================================================

bool RpcSerializer::writeRpcArgs(BitStream& s,
                                 const ayt::reflect::ITypeInfo* /*returnType*/,
                                 const ayt::reflect::IMethodInfo* methodInfo,
                                 ayt::reflect::RpcKind rpcKind,
                                 uint16_t methodHash,
                                 uint64_t callId,
                                 const void* const* args,
                                 size_t argCount) {
    if (!methodInfo) return false;
    const size_t methodParamCount = methodInfo->getParamCount();
    if (methodParamCount != argCount) return false;

    // RpcCallFrame header
    s.writeUInt8(static_cast<uint8_t>(rpcKind));
    s.writeUInt16(methodHash);
    // callId written as 8 little-endian bytes (BitStream has no u64 helper).
    for (int i = 0; i < 8; ++i) s.writeUInt8(static_cast<uint8_t>(callId >> (i * 8)));
    s.writeUInt8(static_cast<uint8_t>(argCount));
    s.writeUInt8(0); // reserved

    for (size_t i = 0; i < argCount; ++i) {
        const char* argName = methodInfo->getParamName(i);
        uint16_t    nameHash = fnv1a16(argName ? argName : "");
        // Fallback: when argName is empty (default IMethodInfo impl),
        // hash positional index so the slot is still unique across args.
        if (nameHash == 0) {
            nameHash = static_cast<uint16_t>(fnv1a16("arg") ^ static_cast<uint16_t>(i));
        }
        s.writeUInt16(nameHash);

        const ayt::reflect::ITypeInfo* argType = methodInfo->getParamType(i);
        WireTypeId wid;
        if (!ReflectSerializer::resolveWireTypeId(argType, wid)) {
            // Unknown arg type — can't serialize, but frame already started.
            // Wrap an error into the byte stream: pad with explicit garbage
            // wid=255 (WireTypeId cast to uint8_t) so the receiver's
            // deserialize fails. R4.0 receivers see a parse fail and drop.
            s.writeUInt8(255);
            return false;
        }
        s.writeUInt8(static_cast<uint8_t>(wid));
        if (!ReflectSerializer::writeWireValue(s, wid, argType, args ? args[i] : nullptr)) {
            return false;
        }
    }
    return true;
}

bool RpcSerializer::readRpcArgs(BitStream& s,
                                const std::unordered_map<uint16_t, const ayt::reflect::IMethodInfo*>& methodsByHash,
                                ayt::reflect::RpcKind& rpcKindOut,
                                uint16_t& methodHashOut,
                                uint64_t& callIdOut,
                                const ayt::reflect::IMethodInfo*& methodInfoOut,
                                std::vector<uint8_t>& argBufOut) {
    rpcKindOut   = ayt::reflect::RpcKind::None;
    methodHashOut = 0;
    callIdOut    = 0;
    methodInfoOut = nullptr;
    argBufOut.clear();

    // RpcCallFrame header
    if (s.getBitPosition() + 8 > s.getBitCount()) return false;
    rpcKindOut   = static_cast<ayt::reflect::RpcKind>(s.readUInt8());
    if (s.getBitPosition() + 16 > s.getBitCount()) return false;
    methodHashOut = s.readUInt16();
    if (s.getBitPosition() + 64 > s.getBitCount()) return false;
    callIdOut = 0;
    for (int i = 0; i < 8; ++i) {
        callIdOut |= (static_cast<uint64_t>(s.readUInt8()) << (i * 8));
    }
    if (s.getBitPosition() + 16 > s.getBitCount()) return false;
    const uint8_t argCount = s.readUInt8();
    (void)s.readUInt8(); // reserved

    auto it = methodsByHash.find(methodHashOut);
    if (it == methodsByHash.end()) { methodInfoOut = nullptr; return false; }
    methodInfoOut = it->second;
    if (!methodInfoOut) return false;
    if (methodInfoOut->getParamCount() != argCount) return false;

    // Reuse R3.2 readWireValue for each arg. The arg buffer is a
    // contiguous blob where each arg occupies `getParamType(i)->getSize()`
    // bytes (size known up-front because R3.0/3.1 fields are
    // trivially-copyable types; struct args arrived in R3.2 with size
    // returned from ITypeInfo). Allocation strategy: sum sizes,
    // allocate once, then point each `args[i]` slot at the
    // appropriate offset.
    //
    // For R4.0 simplicity we instead allocate one buffer per arg and
    // concatenate — the args[] table that's handed to invoke() then
    // points into the heap-allocated argBuf vector.
    argBufOut.clear();
    argBufOut.reserve(256); // typical small RPC
    std::vector<const void*> argPtrs;
    argPtrs.reserve(argCount);
    for (size_t i = 0; i < argCount; ++i) {
        if (s.getBitPosition() + 16 > s.getBitCount()) return false;
        (void)s.readUInt16(); // argNameHash — accepted blindly; the
                              // receiver already trusts the registered
                              // methods' IMethodInfo* signature to
                              // disambiguate which position the value
                              // lands in.
        if (s.getBitPosition() + 8 > s.getBitCount()) return false;
        const uint8_t widByte = s.readUInt8();
        WireTypeId wid = static_cast<WireTypeId>(widByte);
        const ayt::reflect::ITypeInfo* argType = methodInfoOut->getParamType(i);
        if (widByte == 255) return false; // sender signaled parse fail
        // readWireValue mutates `&argBufOut[slotStart]`. We grow the
        // buffer and align writes to the type's size.
        const size_t argSize = argType ? argType->getSize() : 0;
        const size_t slotStart = argBufOut.size();
        argBufOut.resize(slotStart + argSize + sizeof(uint64_t) /*alignment pad*/);
        if (!ReflectSerializer::readWireValue(s, wid, argType, &argBufOut[slotStart])) {
            return false;
        }
        argPtrs.push_back(&argBufOut[slotStart]);
    }
    return true;
}

bool RpcSerializer::writeRpcResponse(BitStream& s,
                                     const ayt::reflect::ITypeInfo* returnType,
                                     uint64_t callId,
                                     const void* returnValue) {
    for (int i = 0; i < 8; ++i) s.writeUInt8(static_cast<uint8_t>(callId >> (i * 8)));
    const bool hasReturn = (returnType != nullptr && returnValue != nullptr);
    s.writeUInt8(hasReturn ? 1 : 0);
    if (!hasReturn) return true;
    WireTypeId wid;
    if (!ReflectSerializer::resolveWireTypeId(returnType, wid)) return false;
    s.writeUInt8(static_cast<uint8_t>(wid));
    return ReflectSerializer::writeWireValue(s, wid, returnType, returnValue);
}

bool RpcSerializer::readRpcResponse(BitStream& s,
                                    uint64_t& callIdOut,
                                    bool& hasReturnOut,
                                    WireTypeId& returnWidOut,
                                    std::vector<uint8_t>& returnBufOut) {
    callIdOut    = 0;
    hasReturnOut = false;
    returnWidOut = WireTypeId::Bool;
    returnBufOut.clear();

    if (s.getBitPosition() + 8 > s.getBitCount()) return false;
    callIdOut = 0;
    for (int i = 0; i < 8; ++i) {
        callIdOut |= (static_cast<uint64_t>(s.readUInt8()) << (i * 8));
    }
    if (s.getBitPosition() + 8 > s.getBitCount()) return false;
    hasReturnOut = (s.readUInt8() != 0);
    if (!hasReturnOut) return true;
    if (s.getBitPosition() + 8 > s.getBitCount()) return false;
    returnWidOut = static_cast<WireTypeId>(s.readUInt8());
    // Allocate space for the return value — caller already knows the type
    // via the registered IMethodInfo::getReturnType(). We can't know the
    // size here without the registry; embed returnBufOut sizing in the
    // caller. For R4.0 simplicity we read into a placeholder — caller
    // uses the unwrapped bytes directly.
    returnBufOut.resize(256);
    return ReflectSerializer::readWireValue(s, returnWidOut, nullptr /*fallback*/, &returnBufOut[0]);
}

bool RpcSerializer::writeRpcReject(BitStream& s, uint64_t callId, RpcRejectReason reason) {
    for (int i = 0; i < 8; ++i) s.writeUInt8(static_cast<uint8_t>(callId >> (i * 8)));
    s.writeUInt8(static_cast<uint8_t>(reason));
    return true;
}

bool RpcSerializer::readRpcReject(BitStream& s, uint64_t& callIdOut, RpcRejectReason& reasonOut) {
    callIdOut = 0;
    reasonOut = RpcRejectReason::UnknownMethod;
    if (s.getBitPosition() + 16 > s.getBitCount()) return false;
    callIdOut = 0;
    for (int i = 0; i < 8; ++i) {
        callIdOut |= (static_cast<uint64_t>(s.readUInt8()) << (i * 8));
    }
    if (s.getBitPosition() + 8 > s.getBitCount()) return false;
    reasonOut = static_cast<RpcRejectReason>(s.readUInt8());
    return true;
}

// =============================================================================
// RpcHandler implementation
// =============================================================================

RpcHandler::RpcHandler(INetworkSubSystem* network)
    : _network(network)
{
    _asyncPool = std::make_unique<RpcAsyncPool>();
    _asyncPool->start(2);
}

RpcHandler::~RpcHandler() {
    if (_asyncPool) _asyncPool->shutdown();
    // Locked access for the destructor: trivial since the handler's owner
    // (AYNetworkSubSystem) is single-threaded by design. Pending callbacks
    // are dropped; clients must drain their own state before destroying
    // the subsystem.
    _pendingCalls.clear();
}

bool RpcHandler::resolveMethod(const char* typeName, const char* methodName,
                               const ayt::reflect::IMethodInfo*& outInfo,
                               void*& outObj,
                               uint16_t& outHash) const {
    outInfo = nullptr;
    outObj  = nullptr;
    outHash = 0;
    if (!typeName || !methodName) return false;

    // Locate the ITypeInfo* by string name in the global registry.
    auto& reg = ayt::reflect::TypeRegistryImpl::instance();
    auto* typeInfo = reg.findType(typeName);
    if (!typeInfo) return false;
    auto* methodInfo = typeInfo->findMethod(methodName);
    if (!methodInfo) return false;
    if (methodInfo->getRpcKind() == ayt::reflect::RpcKind::None) return false;

    const uint16_t h = fnv1a16(methodName);
    outInfo = methodInfo;
    outHash = h;
    // Outbound callServer/callClient/callMulticast only need registry
    // metadata to serialize args — the receiver's registerMethod() binds
    // the handler object. Requiring a local _objsByHash entry on the caller
    // made client→server RPC impossible (E2E ServerRpcHappyPath).
    auto it = _objsByHash.find(h);
    outObj = (it != _objsByHash.end()) ? it->second : nullptr;
    return true;
}

bool RpcHandler::registerMethod(const char* typeName, const char* methodName, void* obj) {
    if (!typeName || !methodName || !obj) return false;

    auto& reg = ayt::reflect::TypeRegistryImpl::instance();
    auto* typeInfo = reg.findType(typeName);
    if (!typeInfo) return false;
    auto* methodInfo = typeInfo->findMethod(methodName);
    if (!methodInfo) return false;
    if (methodInfo->getRpcKind() == ayt::reflect::RpcKind::None) return false;

    const uint16_t h = fnv1a16(methodName);
    if (_methodsByHash.find(h) != _methodsByHash.end()) {
        // Duplicate registration — leave the existing binding (callers
        // must call unregisterMethod first). Returning false surfaces the
        // misuse loudly in tests.
        return false;
    }
    _methodsByHash[h] = methodInfo;
    _objsByHash[h]    = obj;
    return true;
}

void RpcHandler::unregisterMethod(const char* typeName, const char* methodName) {
    if (!typeName || !methodName) return;
    const uint16_t h = fnv1a16(methodName);
    _methodsByHash.erase(h);
    _objsByHash.erase(h);
}

bool RpcHandler::emit(uint8_t channel, uint16_t envelopeMsgType, const BitStream& body,
                      NetConnection* target) {
    if (!_network && !_broadcastSink) return false;
    std::vector<uint8_t> sealed = PacketCodec::encode(
        static_cast<const uint8_t*>(body.getData()), body.getSize(),
        envelopeMsgType, kSchemaVersion,
        channel, /*flags=*/ 0, /*timestampMs=*/ 0, /*compress=*/ false);
    if (_broadcastSink) {
        _broadcastSink(channel, sealed.data(), sealed.size());
        return true;
    }
    if (!_network) return false;
    if (target) {
        _network->sendTo(target, channel, sealed.data(), sealed.size());
        return true;
    }
    const ConnectionMode mode = getEffectiveMode();
    if (mode == ConnectionMode::Client) {
        _network->send(channel, sealed.data(), sealed.size());
        return true;
    }
    _network->broadcast(channel, sealed.data(), sealed.size());
    return true;
}

NetConnection* RpcHandler::findNetConnectionById(uint32_t netId) const {
    if (!_network || netId == 0) return nullptr;
    for (NetConnection* conn : _network->getConnections()) {
        if (conn && conn->getId() == netId) return conn;
    }
    return nullptr;
}

void RpcHandler::tick(float /*deltaTime*/) {
    drainAsyncRpcCompletions();
    expirePendingCalls();
}

bool RpcHandler::buildArgPtrTable(const ayt::reflect::IMethodInfo* methodInfo,
                                  const std::vector<uint8_t>& argBuf,
                                  std::vector<const void*>& argPtrsOut) const {
    if (!methodInfo) return false;
    const size_t argCount = methodInfo->getParamCount();
    argPtrsOut.resize(argCount);
    size_t offset = 0;
    for (size_t i = 0; i < argCount; ++i) {
        const ayt::reflect::ITypeInfo* argType = methodInfo->getParamType(i);
        const size_t argSize = argType ? argType->getSize() : 0;
        if (offset + argSize > argBuf.size()) return false;
        argPtrsOut[i] = argBuf.data() + offset;
        offset += argSize;
    }
    return true;
}

void RpcHandler::sendRpcResponse(uint64_t callId, NetConnection* from,
                                 const ayt::reflect::IMethodInfo* methodInfo,
                                 const void* retPtr) {
    if (!methodInfo) return;
    BitStream respBody;
    if (methodInfo->getReturnType() != nullptr) {
        RpcSerializer::writeRpcResponse(respBody, methodInfo->getReturnType(), callId, retPtr);
    } else {
        RpcSerializer::writeRpcResponse(respBody, nullptr, callId, nullptr);
    }
    emit(CHANNEL_RELIABLE, kMsgTypeRpcResponse, respBody, from);
}

void RpcHandler::drainAsyncRpcCompletions() {
    if (!_asyncPool) return;
    RpcAsyncPool::Completion completion;
    while (_asyncPool->tryPopCompletion(completion)) {
        if (completion.hasReturn && completion.returnType && !completion.returnBuf.empty()) {
            BitStream respBody;
            RpcSerializer::writeRpcResponse(respBody, completion.returnType, completion.callId,
                                            completion.returnBuf.data());
            emit(CHANNEL_RELIABLE, kMsgTypeRpcResponse, respBody, completion.from);
        } else {
            BitStream respBody;
            RpcSerializer::writeRpcResponse(respBody, nullptr, completion.callId, nullptr);
            emit(CHANNEL_RELIABLE, kMsgTypeRpcResponse, respBody, completion.from);
        }
    }
}

uint32_t RpcHandler::computeRetryBackoffMs(uint32_t retryCount) const {
    if (retryCount == 0 || _retryBaseMs == 0) return _retryBaseMs;
    const uint32_t shift = retryCount - 1;
    if (shift >= 31) return _retryBaseMs * 0x80000000u;
    return _retryBaseMs << shift;
}

bool RpcHandler::emitRequestBody(uint8_t channel, uint16_t envelopeMsgType,
                                 const std::vector<uint8_t>& body, uint32_t targetNetId) {
    if (body.empty()) return false;
    BitStream bs;
    bs.writeBits(body.data(), body.size() * 8);
    NetConnection* target = findNetConnectionById(targetNetId);
    if (!target && targetNetId == 0 && _network) {
        const std::vector<NetConnection*>& conns = _network->getConnections();
        if (!conns.empty()) target = conns.front();
    }
    return emit(channel, envelopeMsgType, bs, target);
}

void RpcHandler::registerPendingWithRetry(uint64_t callId, RpcCallback cb,
                                          uint8_t channel, uint16_t envelopeMsgType,
                                          const BitStream& requestBody, uint32_t targetNetId) {
    PendingEntry entry;
    entry.cb = std::move(cb);
    entry.retryEnabled = true;
    entry.retriesLeft = _maxRetries;
    entry.retry.channel = channel;
    entry.retry.envelopeMsgType = envelopeMsgType;
    entry.retry.targetNetId = targetNetId;
    entry.retry.body.assign(
        static_cast<const uint8_t*>(requestBody.getData()),
        static_cast<const uint8_t*>(requestBody.getData()) + requestBody.getSize());
    entry.phase = PendingPhase::AwaitingResponse;
    entry.deadline = std::chrono::steady_clock::now() +
                     std::chrono::milliseconds(_pendingTimeoutMs);
    std::lock_guard<std::mutex> lk(_pendingCallsMutex);
    _pendingCalls[callId] = std::move(entry);
}

void RpcHandler::expirePendingCalls() {
    const auto now = std::chrono::steady_clock::now();
    std::vector<uint64_t> due;
    {
        std::lock_guard<std::mutex> lk(_pendingCallsMutex);
        due.reserve(_pendingCalls.size());
        for (const auto& kv : _pendingCalls) {
            if (now >= kv.second.deadline) due.push_back(kv.first);
        }
    }

    for (uint64_t callId : due) {
        enum class PendingAction : uint8_t { None, Resend, Fail };
        PendingAction action = PendingAction::None;
        PendingRetryPayload resendPayload;
        RpcCallback failCb;

        {
            std::lock_guard<std::mutex> lk(_pendingCallsMutex);
            auto it = _pendingCalls.find(callId);
            if (it == _pendingCalls.end()) continue;
            PendingEntry& entry = it->second;
            if (now < entry.deadline) continue;

            if (entry.phase == PendingPhase::Backoff) {
                if (entry.retryEnabled) {
                    resendPayload = entry.retry;
                    action = PendingAction::Resend;
                }
                entry.phase = PendingPhase::AwaitingResponse;
                entry.deadline = std::chrono::steady_clock::now() +
                                 std::chrono::milliseconds(_pendingTimeoutMs);
            } else if (entry.retryEnabled && entry.retriesLeft > 0) {
                entry.retriesLeft--;
                entry.retryCount++;
                entry.phase = PendingPhase::Backoff;
                entry.deadline = std::chrono::steady_clock::now() +
                                 std::chrono::milliseconds(computeRetryBackoffMs(entry.retryCount));
            } else {
                failCb = std::move(entry.cb);
                _pendingCalls.erase(it);
                action = PendingAction::Fail;
            }
        }

        if (action == PendingAction::Resend) {
            (void)emitRequestBody(resendPayload.channel, resendPayload.envelopeMsgType,
                                  resendPayload.body, resendPayload.targetNetId);
        } else if (action == PendingAction::Fail && failCb) {
            failCb(false, nullptr);
        }
    }
}

bool RpcHandler::callServer(const char* typeName, const char* methodName,
                            const void* const* args, const char* const* argNames,
                            size_t argCount, uint64_t& outCallId) {
    outCallId = 0;
    const ayt::reflect::IMethodInfo* methodInfo = nullptr;
    void* obj = nullptr;
    uint16_t hash = 0;
    if (!resolveMethod(typeName, methodName, methodInfo, obj, hash)) return false;
    if (!methodInfo) return false;
    if (methodInfo->getRpcKind() != ayt::reflect::RpcKind::Server) return false;

    outCallId = _nextCallId.fetch_add(1, std::memory_order_relaxed);
    BitStream body;
    (void)argNames; // R4.0: rely on IMethodInfo::getParamName; argNames
                    // is a future extension point for ad-hoc naming
                    // that conflicts with methodInfo metadata.
    if (!RpcSerializer::writeRpcArgs(body, nullptr,
                                     methodInfo,
                                     ayt::reflect::RpcKind::Server,
                                     hash, outCallId, args, argCount)) {
        return false;
    }
    const uint8_t channel = methodInfo->isUnreliable() ? CHANNEL_UNRELIABLE : CHANNEL_RELIABLE;
    return emit(channel, kMsgTypeRpcRequest, body);
}

bool RpcHandler::callServerWithCallback(const char* typeName, const char* methodName,
                                        const void* const* args, const char* const* argNames,
                                        size_t argCount, uint64_t& outCallId, RpcCallback cb) {
    outCallId = 0;
    const ayt::reflect::IMethodInfo* methodInfo = nullptr;
    void* obj = nullptr;
    uint16_t hash = 0;
    if (!resolveMethod(typeName, methodName, methodInfo, obj, hash)) return false;
    if (!methodInfo) return false;
    if (methodInfo->getRpcKind() != ayt::reflect::RpcKind::Server) return false;

    outCallId = _nextCallId.fetch_add(1, std::memory_order_relaxed);
    BitStream body;
    (void)argNames;
    if (!RpcSerializer::writeRpcArgs(body, nullptr,
                                     methodInfo,
                                     ayt::reflect::RpcKind::Server,
                                     hash, outCallId, args, argCount)) {
        return false;
    }
    const uint8_t channel = methodInfo->isUnreliable() ? CHANNEL_UNRELIABLE : CHANNEL_RELIABLE;
    if (!emit(channel, kMsgTypeRpcRequest, body)) return false;
    if (cb) registerPendingWithRetry(outCallId, std::move(cb), channel, kMsgTypeRpcRequest, body, 0);
    return true;
}

bool RpcHandler::callClient(uint32_t targetNetId, const char* typeName, const char* methodName,
                            const void* const* args, const char* const* argNames,
                            size_t argCount, uint64_t& outCallId) {
    outCallId = 0;
    const ayt::reflect::IMethodInfo* methodInfo = nullptr;
    void* obj = nullptr;
    uint16_t hash = 0;
    if (!resolveMethod(typeName, methodName, methodInfo, obj, hash)) return false;
    if (!methodInfo) return false;
    if (methodInfo->getRpcKind() != ayt::reflect::RpcKind::Client) return false;

    outCallId = _nextCallId.fetch_add(1, std::memory_order_relaxed);
    BitStream body;
    (void)argNames;
    if (!RpcSerializer::writeRpcArgs(body, nullptr, methodInfo,
                                     ayt::reflect::RpcKind::Client,
                                     hash, outCallId, args, argCount)) {
        return false;
    }
    const uint8_t channel = methodInfo->isUnreliable() ? CHANNEL_UNRELIABLE : CHANNEL_RELIABLE;
    NetConnection* target = findNetConnectionById(targetNetId);
    if (!target && targetNetId == 0 && _network) {
        const std::vector<NetConnection*>& conns = _network->getConnections();
        if (!conns.empty()) target = conns.front();
    }
    if (!target) {
        // R4.0 test seam: loopback sink exercises Client RPC without a live conn.
        if (_broadcastSink) return emit(channel, kMsgTypeRpcRequest, body);
        return false;
    }
    return emit(channel, kMsgTypeRpcRequest, body, target);
}

bool RpcHandler::callClientWithCallback(uint32_t targetNetId, const char* typeName, const char* methodName,
                                        const void* const* args, const char* const* argNames,
                                        size_t argCount, uint64_t& outCallId, RpcCallback cb) {
    outCallId = 0;
    const ayt::reflect::IMethodInfo* methodInfo = nullptr;
    void* obj = nullptr;
    uint16_t hash = 0;
    if (!resolveMethod(typeName, methodName, methodInfo, obj, hash)) return false;
    if (!methodInfo) return false;
    if (methodInfo->getRpcKind() != ayt::reflect::RpcKind::Client) return false;

    outCallId = _nextCallId.fetch_add(1, std::memory_order_relaxed);
    BitStream body;
    (void)argNames;
    if (!RpcSerializer::writeRpcArgs(body, nullptr, methodInfo,
                                     ayt::reflect::RpcKind::Client,
                                     hash, outCallId, args, argCount)) {
        return false;
    }
    const uint8_t channel = methodInfo->isUnreliable() ? CHANNEL_UNRELIABLE : CHANNEL_RELIABLE;
    NetConnection* target = findNetConnectionById(targetNetId);
    if (!target && targetNetId == 0 && _network) {
        const std::vector<NetConnection*>& conns = _network->getConnections();
        if (!conns.empty()) target = conns.front();
    }
    if (!target) {
        if (_broadcastSink) {
            if (!emit(channel, kMsgTypeRpcRequest, body)) return false;
            if (cb) registerPendingWithRetry(outCallId, std::move(cb), channel, kMsgTypeRpcRequest, body, targetNetId);
            return true;
        }
        return false;
    }
    if (!emit(channel, kMsgTypeRpcRequest, body, target)) return false;
    if (cb) registerPendingWithRetry(outCallId, std::move(cb), channel, kMsgTypeRpcRequest, body, targetNetId);
    return true;
}

bool RpcHandler::callMulticast(const char* typeName, const char* methodName,
                               const void* const* args, const char* const* argNames,
                               size_t argCount, uint64_t& outCallId) {
    outCallId = 0;
    const ayt::reflect::IMethodInfo* methodInfo = nullptr;
    void* obj = nullptr;
    uint16_t hash = 0;
    if (!resolveMethod(typeName, methodName, methodInfo, obj, hash)) return false;
    if (!methodInfo) return false;
    if (methodInfo->getRpcKind() != ayt::reflect::RpcKind::Multicast) return false;

    outCallId = _nextCallId.fetch_add(1, std::memory_order_relaxed);
    BitStream body;
    (void)argNames;
    if (!RpcSerializer::writeRpcArgs(body, nullptr, methodInfo,
                                     ayt::reflect::RpcKind::Multicast,
                                     hash, outCallId, args, argCount)) {
        return false;
    }
    const uint8_t channel = methodInfo->isUnreliable() ? CHANNEL_UNRELIABLE : CHANNEL_RELIABLE;
    return emit(channel, kMsgTypeRpcRequest, body);
}

bool RpcHandler::onRpcRequest(BitStream& body, NetConnection* from) {
    // Callers may hand us a stream still positioned after writes (pure
    // unit tests build request bodies inline). Inbound handlers always
    // read from the start of the RpcCallFrame payload.
    body.resetForRead();

    ayt::reflect::RpcKind rpcKind;
    uint16_t methodHash;
    uint64_t callId;
    const ayt::reflect::IMethodInfo* methodInfo = nullptr;
    std::vector<uint8_t> argBuf;

    if (!RpcSerializer::readRpcArgs(body, _methodsByHash,
                                    rpcKind, methodHash, callId,
                                    methodInfo, argBuf)) {
        // Unknown method or parse fail → reject (best-effort) and report failure.
        BitStream rejectBody;
        RpcSerializer::writeRpcReject(rejectBody,
            (callId != 0 ? callId : 0),
            (methodInfo == nullptr ? RpcRejectReason::UnknownMethod : RpcRejectReason::ParseFail));
        (void)emit(CHANNEL_RELIABLE, kMsgTypeRpcReject, rejectBody, from);
        return false;
    }

    if (!methodInfo) {
        // readRpcArgs returned true-ish layout but no method binding —
        // treat as UnknownMethod.
        BitStream rejectBody;
        RpcSerializer::writeRpcReject(rejectBody, callId, RpcRejectReason::UnknownMethod);
        (void)emit(CHANNEL_RELIABLE, kMsgTypeRpcReject, rejectBody, from);
        return false;
    }

    // Authority gate: Server RPCs are handled only on authority endpoints;
    // Client RPCs only on client endpoints. Mis-directed frames are rejected.
    const ConnectionMode mode = getEffectiveMode();
    if (rpcKind == ayt::reflect::RpcKind::Server) {
        if (mode != ConnectionMode::Server && mode != ConnectionMode::ListenServer) {
            BitStream rejectBody;
            RpcSerializer::writeRpcReject(rejectBody, callId, RpcRejectReason::NotAuthority);
            (void)emit(CHANNEL_RELIABLE, kMsgTypeRpcReject, rejectBody, from);
            return false;
        }
    } else if (rpcKind == ayt::reflect::RpcKind::Client) {
        if (mode != ConnectionMode::Client) {
            BitStream rejectBody;
            RpcSerializer::writeRpcReject(rejectBody, callId, RpcRejectReason::NotAuthority);
            (void)emit(CHANNEL_RELIABLE, kMsgTypeRpcReject, rejectBody, from);
            return false;
        }
    }

    // Bind the receiver instance for the (methodHash) binding.
    auto objIt = _objsByHash.find(methodHash);
    if (objIt == _objsByHash.end() || objIt->second == nullptr) {
        BitStream rejectBody;
        RpcSerializer::writeRpcReject(rejectBody, callId, RpcRejectReason::UnknownMethod);
        (void)emit(CHANNEL_RELIABLE, kMsgTypeRpcReject, rejectBody, from);
        return false;
    }
    void* obj = objIt->second;

    if (!methodInfo->validate(obj)) {
        BitStream rejectBody;
        RpcSerializer::writeRpcReject(rejectBody, callId, RpcRejectReason::ValidatorDeny);
        (void)emit(CHANNEL_RELIABLE, kMsgTypeRpcReject, rejectBody, from);
        return false;
    }

    // Deserialize the arg bytes into a call-ready args[] table. The
    // per-position layout in argBuf is determined by the order written
    // by RpcSerializer::writeRpcArgs — argBuf[0..argSize0) is arg 0,
    // argBuf[argSize0..argSize0+argSize1) is arg 1, etc.
    const size_t argCount = methodInfo->getParamCount();
    std::vector<const void*> argPtrs(argCount);
    if (!buildArgPtrTable(methodInfo, argBuf, argPtrs)) {
        BitStream rejectBody;
        RpcSerializer::writeRpcReject(rejectBody, callId, RpcRejectReason::ParseFail);
        (void)emit(CHANNEL_RELIABLE, kMsgTypeRpcReject, rejectBody, from);
        return false;
    }

    if (methodInfo->isAsync()) {
        if (!_asyncPool) {
            BitStream rejectBody;
            RpcSerializer::writeRpcReject(rejectBody, callId, RpcRejectReason::UnknownMethod);
            (void)emit(CHANNEL_RELIABLE, kMsgTypeRpcReject, rejectBody, from);
            return false;
        }
        RpcAsyncPool::Job job;
        job.methodInfo = methodInfo;
        job.obj = obj;
        job.argBuf = std::move(argBuf);
        job.callId = callId;
        job.from = from;
        _asyncPool->submit(std::move(job));
        return true;
    }

    const void* retPtr = methodInfo->invoke(obj, argPtrs.data());
    sendRpcResponse(callId, from, methodInfo, retPtr);

    (void)rpcKind;
    return true;
}

bool RpcHandler::onRpcResponse(BitStream& body, NetConnection* from) {
    body.resetForRead();
    uint64_t callId;
    bool hasReturn;
    WireTypeId returnWid;
    std::vector<uint8_t> returnBuf;
    if (!RpcSerializer::readRpcResponse(body, callId, hasReturn, returnWid, returnBuf)) {
        return false;
    }
    RpcCallback cb;
    {
        std::lock_guard<std::mutex> lk(_pendingCallsMutex);
        auto it = _pendingCalls.find(callId);
        if (it == _pendingCalls.end()) return false;
        cb = std::move(it->second.cb);
        _pendingCalls.erase(it);
    }
    if (cb) cb(true, returnBuf.empty() ? nullptr : returnBuf.data());
    (void)from;
    return true;
}

bool RpcHandler::onRpcReject(BitStream& body, NetConnection* from) {
    body.resetForRead();
    uint64_t callId;
    RpcRejectReason reason;
    if (!RpcSerializer::readRpcReject(body, callId, reason)) return false;
    RpcCallback cb;
    {
        std::lock_guard<std::mutex> lk(_pendingCallsMutex);
        auto it = _pendingCalls.find(callId);
        if (it == _pendingCalls.end()) return false;
        cb = std::move(it->second.cb);
        _pendingCalls.erase(it);
    }
    (void)reason;
    if (cb) cb(false, nullptr);
    (void)from;
    return true;
}

} // namespace ayt::net
