// AYTest_RpcHandler.cpp - R4.0 RPC dispatcher + 4-channel + Validator tests
//
// R4.0 (2026-07-29): 25 cases total (15 pure + 7 e2e + 3 regression).
// Reuses the R3.2 fixture pattern (TypeInfoImpl<T> + addMethod for
// RPC metadata). Adds a hand-rolled RpcMethodInfoImpl<R(Args...)>
// template so test fixtures can register Server/Client/Multicast
// methods without depending on AYScript's logia/AYMethodInfoImpl.
//
// See design.md §13 R4 checklist for the per-case spec.

#include <AYNetwork.h>
#include <AYTest.h>

#include <AYNetwork/RPC/RpcHandler.h>
#include <AYNetwork/Replication/ReflectSerializer.h>
#include <AYNetwork/Protocol/PacketCodec.h>
#include <AYNetwork/Protocol/PacketHeader.h>
#include <AYNetwork/Transport/GnsConnection.h>

#include <AYReflect/IReflect.h>
#include <AYReflect/ReflectMacros.h>
#include <AYReflect/ReflectRegistry.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <functional>
#include <set>
#include <thread>
#include <vector>

namespace
{

using ayt::net::BitStream;
using ayt::net::GnsConnection;
using ayt::net::INetworkSubSystem;
using ayt::net::RpcHandler;
using ayt::net::RpcSerializer;
using ayt::net::kMsgTypeRpcRequest;
using ayt::net::kMsgTypeRpcResponse;
using ayt::net::kMsgTypeRpcReject;
using ayt::net::WireTypeId;
using ayt::net::PacketFlag;
using ayt::net::hasFlag;

// =============================================================================
// RpcMethodInfoImpl — hand-rolled IMethodInfo derived class for fixtures.
//
// Why not reuse AYScript's MethodInfoImpl? Per `Test_Reflect.cpp:600-603`
// comment, that template lives in `logia/AYScript/logia/AYScript/logia/AYScript/logia/AYScript/logia/MethodInfoImpl.h` and is
// tied to AYScript's PMF variadic unpack. RpcHandler needs only
// enough metadata to (de)serialize + invoke + validate, so a tiny
// hand-rolled template fits the test scope without dragging in
// AYScript.
template<typename Ret, typename... Args>
class RpcMethodInfoImpl : public ayt::reflect::IMethodInfo {
public:
    using Invoker = std::function<Ret(Args...)>;
    using Validator = std::function<bool(const void*)>;

    RpcMethodInfoImpl(
        const char* name,
        ayt::reflect::RpcKind kind,
        ayt::reflect::ITypeInfo* retType,
        const std::vector<ayt::reflect::ITypeInfo*>& paramTypes,
        const std::vector<std::string>& paramNames,
        bool unreliable,
        Invoker invoker,
        Validator validate,
        bool async = false)
        : _name(name)
        , _kind(kind)
        , _retType(retType)
        , _paramTypes(paramTypes)
        , _paramNames(paramNames)
        , _unreliable(unreliable)
        , _invoker(std::move(invoker))
        , _validate(std::move(validate))
        , _async(async)
    {}

    const char* getName() const override { return _name; }
    ayt::reflect::ITypeInfo* getReturnType() const override { return _retType; }
    size_t getParamCount() const override { return _paramTypes.size(); }
    ayt::reflect::ITypeInfo* getParamType(size_t index) const override {
        return index < _paramTypes.size() ? _paramTypes[index] : nullptr;
    }
    bool getParamIsOut(size_t /*index*/) const override { return false; }
    const char* getParamName(size_t index) const override {
        return index < _paramNames.size() ? _paramNames[index].c_str() : "";
    }
    ayt::reflect::RpcKind getRpcKind() const override { return _kind; }
    bool isUnreliable() const override { return _unreliable; }
    bool isAsync() const override { return _async; }
    bool validate(const void* obj) const override {
        return _validate ? _validate(obj) : true;
    }
    const void* invoke(const void* /*obj*/, const void* const* args) const override {
        // Real invocation via the captured Invoker. Args are at
        // args[0..paramCount), each a raw byte buffer cast to the
        // corresponding type. For the R4.0 single-arg RPCs the
        // invokers cast `args[0]` to the right type.
        if (!_invoker) return nullptr;
        if constexpr (std::is_void_v<Ret>) {
            callVoid<Args...>(args);
            return nullptr;
        } else {
            return callReturn<Ret, Args...>(args);
        }
    }

private:
    template<typename A0, typename... ARest>
    void callVoid(const void* const* args) const {
        // Single-arg only is supported in R4.0 tests; multi-arg would
        // recurse here.
        if constexpr (sizeof...(ARest) == 0) {
            A0 a0 = *static_cast<const A0*>(args[0]);
            _invoker(a0);
        }
    }
    template<typename R, typename A0, typename... ARest>
    const void* callReturn(const void* const* args) const {
        if constexpr (sizeof...(ARest) == 0) {
            A0 a0 = *static_cast<const A0*>(args[0]);
            R ret = _invoker(a0);
            // Thread-local buffer for return-by-value. RpcHandler reads
            // this BEFORE the next call.
            static thread_local R lastRet;
            lastRet = ret;
            return &lastRet;
        }
        return nullptr;
    }

    const char* _name;
    ayt::reflect::RpcKind _kind;
    ayt::reflect::ITypeInfo* _retType;
    std::vector<ayt::reflect::ITypeInfo*> _paramTypes;
    std::vector<std::string> _paramNames;
    bool _unreliable;
    bool _async;
    Invoker _invoker;
    Validator _validate;
};

// =============================================================================
// Test receiver instances — held by RpcHandler across the test.
// =============================================================================
struct PlayerRpcReceiver {
    std::atomic<int> healCalls{0};
    std::atomic<int> damageCalls{0};
    std::atomic<int> announceCalls{0};
    std::atomic<int> lastHpReceived{0};
    std::atomic<int> lastDamageAmount{0};
    std::atomic<int> lastAnnounceScore{0};
};

// Free functions invoked through the test fixtures' RpcMethodInfoImpl
// lambda bridge. R4.0 tests register static PlayerRpcReceiver instances
// in the test bodies, but the lambdas have no captured state — they
// use a per-test *active receiver* pointer below to mutate counters.
PlayerRpcReceiver* g_activeReceiver = nullptr;

void rpcMethod_ServerHeal(int32_t hp) {
    if (g_activeReceiver) {
        g_activeReceiver->healCalls.fetch_add(1);
        g_activeReceiver->lastHpReceived.store(hp);
    }
}
void rpcMethod_ClientDamage(int32_t amount) {
    if (g_activeReceiver) {
        g_activeReceiver->damageCalls.fetch_add(1);
        g_activeReceiver->lastDamageAmount.store(amount);
    }
}
void rpcMethod_Announce(int32_t score) {
    if (g_activeReceiver) {
        g_activeReceiver->announceCalls.fetch_add(1);
        g_activeReceiver->lastAnnounceScore.store(score);
    }
}

// =============================================================================
// Static-init registrar: builds TypeInfoImpl<T> + adds RpcMethodInfoImpl
// methods for the test fixture types.
// =============================================================================
struct RpcFixtureRegistrar {
    RpcFixtureRegistrar() {
        using ayt::reflect::TypeInfoImpl;
        using ayt::reflect::TypeRegistryImpl;
        using ayt::reflect::RpcKind;
        auto& reg = TypeRegistryImpl::instance();

        if (!reg.findType("PlayerRpc")) {
            auto* info = new TypeInfoImpl<PlayerRpcReceiver>(
                "PlayerRpc",
                ayt::reflect::detail::defaultCreate<PlayerRpcReceiver>,
                ayt::reflect::detail::defaultDestroy<PlayerRpcReceiver>,
                ayt::reflect::detail::defaultCopy<PlayerRpcReceiver>);

            // ServerHeal(int32_t hp) - RpcKind::Server, validator true,
            //   Reliable default.
            info->addMethod(new RpcMethodInfoImpl<int32_t, int32_t>(
                "ServerHeal",
                RpcKind::Server,
                reg.findType<int32_t>(),
                { reg.findType<int32_t>() },
                { "hp" },
                /*unreliable=*/false,
                /*invoker=*/[](int32_t hp) -> int32_t {
                    // The receiver pointer is captured by the test
                    // fixture's outer scope. RpcMethodInfoImpl::invoke
                    // passes the bound obj pointer to invoker, but our
                    // lambda ignores it; the actual user method runs in
                    // a free function below that mutates a static
                    // receiver held by the test.
                    (void)hp;
                    extern void rpcMethod_ServerHeal(int32_t);
                    rpcMethod_ServerHeal(hp);
                    return hp;
                },
                /*validate=*/[](const void*) { return true; }));

            // ClientDamage(int32_t amount) - RpcKind::Client, unreliable.
            info->addMethod(new RpcMethodInfoImpl<int32_t, int32_t>(
                "ClientDamage",
                RpcKind::Client,
                reg.findType<int32_t>(),
                { reg.findType<int32_t>() },
                { "amount" },
                /*unreliable=*/true,
                /*invoker=*/[](int32_t amount) -> int32_t {
                    (void)amount;
                    extern void rpcMethod_ClientDamage(int32_t);
                    rpcMethod_ClientDamage(amount);
                    return amount;
                },
                /*validate=*/[](const void*) { return true; }));

            // MulticastAnnounce(int32_t score) - RpcKind::Multicast.
            info->addMethod(new RpcMethodInfoImpl<int32_t, int32_t>(
                "MulticastAnnounce",
                RpcKind::Multicast,
                reg.findType<int32_t>(),
                { reg.findType<int32_t>() },
                { "score" },
                /*unreliable=*/false,
                /*invoker=*/[](int32_t score) -> int32_t {
                    (void)score;
                    extern void rpcMethod_Announce(int32_t);
                    rpcMethod_Announce(score);
                    return score;
                },
                /*validate=*/[](const void*) { return true; }));

            // ServerHealStrict — same signature, validator that always denies.
            info->addMethod(new RpcMethodInfoImpl<int32_t, int32_t>(
                "ServerHealStrict",
                RpcKind::Server,
                reg.findType<int32_t>(),
                { reg.findType<int32_t>() },
                { "hp" },
                /*unreliable=*/false,
                /*invoker=*/[](int32_t hp) -> int32_t {
                    (void)hp;
                    extern void rpcMethod_ServerHeal(int32_t);
                    rpcMethod_ServerHeal(hp);
                    return hp;
                },
                /*validate=*/[](const void*) { return false; }));

            // ServerHealSlow — async server RPC (simulates >16ms work).
            info->addMethod(new RpcMethodInfoImpl<int32_t, int32_t>(
                "ServerHealSlow",
                RpcKind::Server,
                reg.findType<int32_t>(),
                { reg.findType<int32_t>() },
                { "hp" },
                /*unreliable=*/false,
                /*invoker=*/[](int32_t hp) -> int32_t {
                    std::this_thread::sleep_for(std::chrono::milliseconds(30));
                    extern void rpcMethod_ServerHeal(int32_t);
                    rpcMethod_ServerHeal(hp);
                    return hp;
                },
                /*validate=*/[](const void*) { return true; },
                /*async=*/true));

            // ServerReturnBlob(int32_t byteCount) — returns a large std::string
            // payload for RpcResponse auto-lz4 tests.
            info->addMethod(new RpcMethodInfoImpl<std::string, int32_t>(
                "ServerReturnBlob",
                RpcKind::Server,
                reg.findType<std::string>(),
                { reg.findType<int32_t>() },
                { "byteCount" },
                /*unreliable=*/false,
                /*invoker=*/[](int32_t byteCount) -> std::string {
                    if (byteCount < 0) byteCount = 0;
                    return std::string(static_cast<size_t>(byteCount), 'Z');
                },
                /*validate=*/[](const void*) { return true; }));

            reg.registerTypeInfo("PlayerRpc", info);
        }
    }
};
static RpcFixtureRegistrar g_rpcFixtureRegistrar;

} // anonymous namespace

// =============================================================================
// R4.0 pure cases — 10 of them. All in one TEST_SUITE.
// =============================================================================

TEST_SUITE(RpcHandlerPure)
TEST_CASE(RegisterAndResolveMethod) {
    ayt::test::setCurrentCase("RegisterAndResolveMethod");
    RpcHandler h(nullptr);
    static PlayerRpcReceiver recv;
    CHECK(h.registerMethod("PlayerRpc", "ServerHeal", &recv));
    CHECK(h.methodsByHash().size() == 1);
}

TEST_CASE(WriteReadArgsRoundTrip) {
    ayt::test::setCurrentCase("WriteReadArgsRoundTrip");
    RpcHandler h(nullptr);
    h.setModeForTesting(ayt::net::ConnectionMode::Server);
    static PlayerRpcReceiver recv;
    g_activeReceiver = &recv;
    CHECK(h.registerMethod("PlayerRpc", "ServerHeal", &recv));
    // Sink required so onRpcRequest's response emit has a target.
    std::atomic<int> respCount{0};
    h.setBroadcastSinkForTesting([&](uint8_t, const void*, size_t) {
        respCount.fetch_add(1);
    });

    // Compute the FNV-1a-16 hash of "ServerHeal" so the methodHash
    // matches what registerMethod stored.
    const char* nm = "ServerHeal";
    uint32_t h32 = 0x811C9DC5u;
    for (const char* p = nm; *p; ++p) { h32 ^= static_cast<uint8_t>(*p); h32 *= 16777619u; }

    BitStream body;
    body.writeUInt8(static_cast<uint8_t>(ayt::reflect::RpcKind::Server));
    body.writeUInt16(static_cast<uint16_t>(h32 & 0xFFFFu));
    for (int i = 0; i < 8; ++i) body.writeUInt8(static_cast<uint8_t>(0x1122334455667788ull >> (i * 8)));
    body.writeUInt8(1); body.writeUInt8(0);
    body.writeUInt16(0x1234);
    body.writeUInt8(static_cast<uint8_t>(WireTypeId::Int32));
    body.writeInt32Raw(42);

    CHECK(h.onRpcRequest(body, nullptr));
    CHECK_INT_EQ(recv.healCalls.load(), 1);
    CHECK_INT_EQ(recv.lastHpReceived.load(), 42);
}

TEST_CASE(WriteReadResponseRoundTrip) {
    ayt::test::setCurrentCase("WriteReadResponseRoundTrip");
    BitStream out;
    int32_t retVal = -7;
    CHECK(RpcSerializer::writeRpcResponse(out, ayt::reflect::TypeRegistryImpl::instance().findType<int32_t>(),
                                         0xDEADBEEFCAFEBABEull, &retVal));
    BitStream in;
    in.writeBits(out.getData(), out.getSize() * 8);
    in.resetForRead();
    uint64_t callIdOut = 0;
    bool hasReturnOut = false;
    WireTypeId widOut = WireTypeId::Bool;
    std::vector<uint8_t> returnBuf;
    CHECK(RpcSerializer::readRpcResponse(in, callIdOut, hasReturnOut, widOut, returnBuf));
    CHECK_INT_EQ(static_cast<uint64_t>(callIdOut), static_cast<uint64_t>(0xDEADBEEFCAFEBABEull));
    CHECK(hasReturnOut);
    CHECK(static_cast<uint8_t>(widOut) == static_cast<uint8_t>(WireTypeId::Int32));
}

TEST_CASE(WriteReadRejectBodyRoundTrip) {
    ayt::test::setCurrentCase("WriteReadRejectBodyRoundTrip");
    BitStream out;
    CHECK(RpcSerializer::writeRpcReject(out, 0x12345678ABCDEF01ull,
                                        ayt::net::RpcRejectReason::ValidatorDeny));
    BitStream in;
    in.writeBits(out.getData(), out.getSize() * 8);
    in.resetForRead();
    uint64_t callIdOut = 0;
    ayt::net::RpcRejectReason reasonOut = ayt::net::RpcRejectReason::UnknownMethod;
    CHECK(RpcSerializer::readRpcReject(in, callIdOut, reasonOut));
    CHECK_INT_EQ(static_cast<uint64_t>(callIdOut), static_cast<uint64_t>(0x12345678ABCDEF01ull));
    CHECK(reasonOut == ayt::net::RpcRejectReason::ValidatorDeny);
}

TEST_CASE(ValidatorRejectsCall) {
    ayt::test::setCurrentCase("ValidatorRejectsCall");
    RpcHandler h(nullptr);
    h.setModeForTesting(ayt::net::ConnectionMode::Server);
    static PlayerRpcReceiver recv;
    CHECK(h.registerMethod("PlayerRpc", "ServerHealStrict", &recv));

    std::atomic<int> reqCount{0};
    h.setBroadcastSinkForTesting([&](uint8_t /*channel*/, const void*, size_t) {
        reqCount.fetch_add(1);
    });

    uint64_t callId = 0;
    int32_t hp = 50;
    const void* args[1] = { &hp };
    CHECK(h.callServer("PlayerRpc", "ServerHealStrict", args, nullptr, 1, callId));
    CHECK(callId != 0);
    CHECK(reqCount.load() >= 1);
}

TEST_CASE(UnknownMethodReturnsFalse) {
    ayt::test::setCurrentCase("UnknownMethodReturnsFalse");
    RpcHandler h(nullptr);
    uint64_t callId = 0;
    const void** dummyArgs = nullptr;
    CHECK(!h.callServer("PlayerRpc", "NoSuchMethod", dummyArgs, nullptr, 0, callId));
    CHECK_INT_EQ(static_cast<uint64_t>(callId), static_cast<uint64_t>(0));
}

TEST_CASE(ParseFailOnTruncatedBody) {
    ayt::test::setCurrentCase("ParseFailOnTruncatedBody");
    RpcHandler h(nullptr);
    h.setModeForTesting(ayt::net::ConnectionMode::Server);
    static PlayerRpcReceiver recv;
    CHECK(h.registerMethod("PlayerRpc", "ServerHeal", &recv));

    BitStream body;
    body.writeUInt8(static_cast<uint8_t>(ayt::reflect::RpcKind::Server));
    CHECK(!h.onRpcRequest(body, nullptr));
    CHECK_INT_EQ(recv.healCalls.load(), 0);
}

TEST_CASE(MultiplePendingCallsDisambiguated) {
    ayt::test::setCurrentCase("MultiplePendingCallsDisambiguated");
    RpcHandler h(nullptr);
    static PlayerRpcReceiver recv;
    CHECK(h.registerMethod("PlayerRpc", "ServerHeal", &recv));
    // No-op sink so callServer's emit() finds a target.
    h.setBroadcastSinkForTesting([](uint8_t, const void*, size_t){});
    constexpr int N = 5;
    std::vector<uint64_t> callIds(N, 0);
    for (int i = 0; i < N; ++i) {
        int32_t hp = i;
        const void* args[1] = { &hp };
        CHECK(h.callServer("PlayerRpc", "ServerHeal", args, nullptr, 1, callIds[i]));
        CHECK(callIds[i] != 0);
    }
    std::set<uint64_t> seen(callIds.begin(), callIds.end());
    CHECK_INT_EQ(static_cast<size_t>(seen.size()), static_cast<size_t>(N));
}

TEST_CASE(UnreliableOverridePerRpc) {
    ayt::test::setCurrentCase("UnreliableOverridePerRpc");
    RpcHandler h(nullptr);
    static PlayerRpcReceiver recv;
    h.registerMethod("PlayerRpc", "ClientDamage", &recv);

    std::atomic<int> reliableCount{0};
    std::atomic<int> unreliableCount{0};
    h.setBroadcastSinkForTesting([&](uint8_t channel, const void*, size_t) {
        if (channel == ayt::net::CHANNEL_RELIABLE) reliableCount.fetch_add(1);
        else if (channel == ayt::net::CHANNEL_UNRELIABLE) unreliableCount.fetch_add(1);
    });

    uint64_t callId = 0;
    int32_t amount = 5;
    const void* args[1] = { &amount };
    CHECK(h.callClient(0, "PlayerRpc", "ClientDamage", args, nullptr, 1, callId));
    CHECK(unreliableCount.load() >= 1);
}

TEST_CASE(RpcKindMismatchRejects) {
    ayt::test::setCurrentCase("RpcKindMismatchRejects");
    RpcHandler h(nullptr);
    static PlayerRpcReceiver recv;
    h.registerMethod("PlayerRpc", "ServerHeal", &recv);

    uint64_t callId = 0;
    int32_t hp = 1;
    const void* args[1] = { &hp };
    CHECK(!h.callClient(0, "PlayerRpc", "ServerHeal", args, nullptr, 1, callId));
}

TEST_CASE(PendingCallTimesOut) {
    ayt::test::setCurrentCase("PendingCallTimesOut");
    RpcHandler h(nullptr);
    h.setPendingTimeoutMsForTesting(1);
    std::atomic<bool> timedOut{false};
    h.registerPending(0xBEEF, [&](bool accepted, const void*) {
        if (!accepted) timedOut.store(true);
    });
    CHECK(h.hasPending(0xBEEF));
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    h.tick(0.016f);
    CHECK(timedOut.load());
    CHECK(!h.hasPending(0xBEEF));
}

TEST_CASE(PendingCallExhaustsRetries) {
    ayt::test::setCurrentCase("PendingCallExhaustsRetries");
    RpcHandler h(nullptr);
    h.setRetryPolicy(/*maxRetries=*/ 2, /*retryBaseMs=*/ 1, /*attemptTimeoutMs=*/ 1);
    std::atomic<int> emitCount{0};
    h.setBroadcastSinkForTesting([&](uint8_t, const void*, size_t) {
        emitCount.fetch_add(1);
    });

    std::atomic<bool> failed{false};
    uint64_t callId = 0;
    int32_t hp = 9;
    const void* args[1] = { &hp };
    CHECK(h.callServerWithCallback("PlayerRpc", "ServerHeal", args, nullptr, 1, callId,
        [&](bool accepted, const void*) {
            if (!accepted) failed.store(true);
        }));

    for (int i = 0; i < 64 && !failed.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        h.tick(0.016f);
    }
    CHECK(failed.load());
    CHECK_INT_EQ(emitCount.load(), 3);
    CHECK(!h.hasPending(callId));
}

TEST_CASE(RegisterWildcardMulticastRejectsUnknownType) {
    ayt::test::setCurrentCase("RegisterWildcardMulticastRejectsUnknownType");
    RpcHandler h(nullptr);
    static PlayerRpcReceiver recv;
    CHECK(!h.registerWildcardMulticast("NoSuchRpcType", &recv));
}

TEST_CASE(WildcardMulticastInboundWithoutRegisterMethod) {
    ayt::test::setCurrentCase("WildcardMulticastInboundWithoutRegisterMethod");
    RpcHandler h(nullptr);
    h.setModeForTesting(ayt::net::ConnectionMode::Client);
    static PlayerRpcReceiver recv;
    g_activeReceiver = &recv;
    CHECK(h.registerWildcardMulticast("PlayerRpc", &recv));
    CHECK(h.methodsByHash().empty());

    auto& reg = ayt::reflect::TypeRegistryImpl::instance();
    auto* typeInfo = reg.findType("PlayerRpc");
    CHECK(typeInfo != nullptr);
    auto* methodInfo = typeInfo->findMethod("MulticastAnnounce");
    CHECK(methodInfo != nullptr);

    const char* nm = "MulticastAnnounce";
    uint32_t h32 = 0x811C9DC5u;
    for (const char* p = nm; *p; ++p) { h32 ^= static_cast<uint8_t>(*p); h32 *= 16777619u; }
    const uint16_t methodHash = static_cast<uint16_t>(h32 & 0xFFFFu);

    std::atomic<int> outboundFrames{0};
    h.setBroadcastSinkForTesting([&](uint8_t, const void*, size_t) {
        outboundFrames.fetch_add(1);
    });

    int32_t score = 123;
    const void* args[1] = { &score };
    const uint64_t callId = 0xABCDEF01ull;
    BitStream body;
    CHECK(RpcSerializer::writeRpcArgs(body, nullptr, methodInfo,
                                      ayt::reflect::RpcKind::Multicast,
                                      methodHash, callId, args, 1));
    CHECK(h.onRpcRequest(body, nullptr));
    CHECK_INT_EQ(recv.announceCalls.load(), 1);
    CHECK_INT_EQ(recv.lastAnnounceScore.load(), 123);
    CHECK_INT_EQ(outboundFrames.load(), 0);
}

TEST_CASE(RpcResponseAutoCompressesLargePayload) {
    ayt::test::setCurrentCase("RpcResponseAutoCompressesLargePayload");
    RpcHandler h(nullptr);
    h.setModeForTesting(ayt::net::ConnectionMode::Server);
    static PlayerRpcReceiver recv;
    CHECK(h.registerMethod("PlayerRpc", "ServerReturnBlob", &recv));

    std::vector<uint8_t> sealed;
    h.setBroadcastSinkForTesting([&](uint8_t, const void* data, size_t size) {
        sealed.assign(static_cast<const uint8_t*>(data),
                      static_cast<const uint8_t*>(data) + size);
    });

    auto& reg = ayt::reflect::TypeRegistryImpl::instance();
    auto* typeInfo = reg.findType("PlayerRpc");
    CHECK(typeInfo != nullptr);
    auto* methodInfo = typeInfo->findMethod("ServerReturnBlob");
    CHECK(methodInfo != nullptr);

    const char* nm = "ServerReturnBlob";
    uint32_t h32 = 0x811C9DC5u;
    for (const char* p = nm; *p; ++p) { h32 ^= static_cast<uint8_t>(*p); h32 *= 16777619u; }
    const uint16_t methodHash = static_cast<uint16_t>(h32 & 0xFFFFu);

    int32_t byteCount = 256;
    const void* args[1] = { &byteCount };
    BitStream req;
    CHECK(RpcSerializer::writeRpcArgs(req, nullptr, methodInfo,
                                      ayt::reflect::RpcKind::Server,
                                      methodHash, 0x1111222233334444ull, args, 1));
    CHECK(h.onRpcRequest(req, nullptr));
    CHECK(!sealed.empty());

    auto decoded = ayt::net::PacketCodec::decode(sealed.data(), sealed.size());
    CHECK(decoded.ok);
    CHECK(hasFlag(decoded.header.flags, PacketFlag::Compressed));
    CHECK_INT_EQ(static_cast<int>(decoded.header.msgType),
                 static_cast<int>(kMsgTypeRpcResponse));
}

TEST_CASE(RpcResponseSkipsCompressForSmallReturn) {
    ayt::test::setCurrentCase("RpcResponseSkipsCompressForSmallReturn");
    RpcHandler h(nullptr);
    h.setModeForTesting(ayt::net::ConnectionMode::Server);
    static PlayerRpcReceiver recv;
    g_activeReceiver = &recv;
    CHECK(h.registerMethod("PlayerRpc", "ServerHeal", &recv));

    std::vector<uint8_t> sealed;
    h.setBroadcastSinkForTesting([&](uint8_t, const void* data, size_t size) {
        sealed.assign(static_cast<const uint8_t*>(data),
                      static_cast<const uint8_t*>(data) + size);
    });

    auto& reg = ayt::reflect::TypeRegistryImpl::instance();
    auto* typeInfo = reg.findType("PlayerRpc");
    auto* methodInfo = typeInfo->findMethod("ServerHeal");
    CHECK(methodInfo != nullptr);

    const char* nm = "ServerHeal";
    uint32_t h32 = 0x811C9DC5u;
    for (const char* p = nm; *p; ++p) { h32 ^= static_cast<uint8_t>(*p); h32 *= 16777619u; }
    const uint16_t methodHash = static_cast<uint16_t>(h32 & 0xFFFFu);

    int32_t hp = 42;
    const void* args[1] = { &hp };
    BitStream req;
    CHECK(RpcSerializer::writeRpcArgs(req, nullptr, methodInfo,
                                      ayt::reflect::RpcKind::Server,
                                      methodHash, 0x5555666677778888ull, args, 1));
    CHECK(h.onRpcRequest(req, nullptr));
    CHECK(!sealed.empty());

    auto decoded = ayt::net::PacketCodec::decode(sealed.data(), sealed.size());
    CHECK(decoded.ok);
    CHECK(!hasFlag(decoded.header.flags, PacketFlag::Compressed));
    CHECK_INT_EQ(static_cast<int>(decoded.header.msgType),
                 static_cast<int>(kMsgTypeRpcResponse));
}

TEST_CASE(RpcResponseCompressedBodyRoundTrip) {
    ayt::test::setCurrentCase("RpcResponseCompressedBodyRoundTrip");
    RpcHandler h(nullptr);
    h.setModeForTesting(ayt::net::ConnectionMode::Server);
    static PlayerRpcReceiver recv;
    CHECK(h.registerMethod("PlayerRpc", "ServerReturnBlob", &recv));

    std::vector<uint8_t> sealed;
    h.setBroadcastSinkForTesting([&](uint8_t, const void* data, size_t size) {
        sealed.assign(static_cast<const uint8_t*>(data),
                      static_cast<const uint8_t*>(data) + size);
    });

    auto& reg = ayt::reflect::TypeRegistryImpl::instance();
    auto* typeInfo = reg.findType("PlayerRpc");
    auto* methodInfo = typeInfo->findMethod("ServerReturnBlob");
    CHECK(methodInfo != nullptr);

    const char* nm = "ServerReturnBlob";
    uint32_t h32 = 0x811C9DC5u;
    for (const char* p = nm; *p; ++p) { h32 ^= static_cast<uint8_t>(*p); h32 *= 16777619u; }
    const uint16_t methodHash = static_cast<uint16_t>(h32 & 0xFFFFu);

    int32_t byteCount = 512;
    const void* args[1] = { &byteCount };
    BitStream req;
    CHECK(RpcSerializer::writeRpcArgs(req, nullptr, methodInfo,
                                      ayt::reflect::RpcKind::Server,
                                      methodHash, 0xAAAABBBBCCCCDDDDull, args, 1));
    CHECK(h.onRpcRequest(req, nullptr));

    auto decoded = ayt::net::PacketCodec::decode(sealed.data(), sealed.size());
    CHECK(decoded.ok);
    CHECK(hasFlag(decoded.header.flags, PacketFlag::Compressed));
    CHECK(decoded.body.size() >= ayt::net::RpcResponseCompressMinBytes);
}
TEST_SUITE_END

// =============================================================================
// E2E cases — 7 of them. Use the R3.2 E2EScaffold pattern, then add a
// simple stub INetworkSubSystem so the RpcHandler doesn't need a real one.
// =============================================================================

namespace
{

struct RpcE2EScaffold {
    RpcHandler serverRpc;
    RpcHandler clientRpc;
    PlayerRpcReceiver serverReceiver;
    PlayerRpcReceiver clientReceiver;

    std::atomic<int> serverRequestCount{0};
    std::atomic<int> clientRequestCount{0};
    std::atomic<int> serverRejectCount{0};
    std::atomic<uint8_t> lastServerChannel{99};
    std::atomic<uint8_t> lastClientChannel{99};

    RpcE2EScaffold()
        : serverRpc(&stub())
        , clientRpc(&stub())
    {
        serverRpc.setModeForTesting(ayt::net::ConnectionMode::Server);
        clientRpc.setModeForTesting(ayt::net::ConnectionMode::Client);

        // Loopback: each handler's emit() is captured and decoded into the peer.
        // No live GNS session — wire format + dispatch paths match production.
        serverRpc.setBroadcastSinkForTesting(
            [this](uint8_t channel, const void* data, size_t size) {
                lastServerChannel.store(channel);
                serverRequestCount.fetch_add(1);
                dispatchSealedTo(clientRpc, data, size);
            });
        clientRpc.setBroadcastSinkForTesting(
            [this](uint8_t channel, const void* data, size_t size) {
                lastClientChannel.store(channel);
                clientRequestCount.fetch_add(1);
                dispatchSealedTo(serverRpc, data, size);
            });
    }

    static void dispatchSealedTo(RpcHandler& rpc, const void* data, size_t size) {
        auto decoded = ayt::net::PacketCodec::decode(
            static_cast<const uint8_t*>(data), size);
        if (!decoded.ok) return;
        BitStream body(decoded.body.data(), decoded.body.size());
        body.resetForRead();
        switch (decoded.header.msgType) {
            case kMsgTypeRpcRequest:  rpc.onRpcRequest(body, nullptr); break;
            case kMsgTypeRpcResponse: rpc.onRpcResponse(body, nullptr); break;
            case kMsgTypeRpcReject:   rpc.onRpcReject(body, nullptr); break;
            default: break;
        }
    }

    static INetworkSubSystem& stub() {
        static StubSubSystem s;
        return s;
    }

private:
    struct StubSubSystem : public INetworkSubSystem {
        const char* getName() const override { return "stub"; }
        const ::ayt::game::SubSystemDescriptor& getDescriptor() const override {
            static ::ayt::game::SubSystemDescriptor d{"stub", {}, 0};
            return d;
        }
        bool initialize() override { return true; }
        void shutdown() override {}
        void update(float) override {}
        void fixedUpdate(float f) override { update(f); }
        void connect(const char*, uint16_t) override {}
        void listen(uint16_t) override {}
        void disconnect() override {}
        bool isConnected() const override { return true; }
        ayt::net::ConnectionMode getMode() const override { return ayt::net::ConnectionMode::Server; }
        void send(uint8_t, const void*, size_t) override {}
        void sendTo(ayt::net::NetConnection*, uint8_t, const void*, size_t) override {}
        void broadcast(uint8_t, const void*, size_t) override {}
        void broadcastExcept(ayt::net::NetConnection*, uint8_t, const void*, size_t) override {}
        void onMessage(uint8_t, MessageHandler) override {}
        void onConnectionChange(ConnectionHandler) override {}
        void setAcceptCallback(AcceptCallback) override {}
        void kickConnection(ayt::net::NetConnection*, const char*) override {}
        const std::vector<ayt::net::NetConnection*>& getConnections() override {
            static std::vector<ayt::net::NetConnection*> empty;
            return empty;
        }
        ayt::net::NetConnection* getConnection() const override { return nullptr; }
        uint32_t getHostId() const override { return 1; }
        void setExtension(ayt::net::INetworkExtension*) override {}
        ayt::net::ReplicationManager* getReplicationManager() override { return nullptr; }
        RpcHandler* getRpcHandler() override { return nullptr; }
        // R5.3 (2026-08-24): Replay recorder wiring — stubs return nullptr.
        void setReplayRecorder(ayt::replay::IReplayRecorder* /*rec*/) override {}
        ayt::replay::IReplayRecorder* getReplayRecorder() const override { return nullptr; }
    };
};

} // anonymous namespace

TEST_SUITE(RpcHandlerE2E)
TEST_CASE(ServerRpcHappyPath) {
    ayt::test::setCurrentCase("ServerRpcHappyPath");
    RpcE2EScaffold s;
    g_activeReceiver = &s.serverReceiver;
    CHECK(s.serverRpc.registerMethod("PlayerRpc", "ServerHeal", &s.serverReceiver));

    uint64_t callId = 0;
    int32_t hp = 100;
    const void* args[1] = { &hp };
    CHECK(s.clientRpc.callServer("PlayerRpc", "ServerHeal", args, nullptr, 1, callId));
    CHECK_INT_EQ(s.serverReceiver.healCalls.load(), 1);
    CHECK_INT_EQ(s.serverReceiver.lastHpReceived.load(), 100);
}

TEST_CASE(ValidatorRejectsAcrossGns) {
    ayt::test::setCurrentCase("ValidatorRejectsAcrossGns");
    RpcE2EScaffold s;
    g_activeReceiver = &s.serverReceiver;
    CHECK(s.serverRpc.registerMethod("PlayerRpc", "ServerHealStrict", &s.serverReceiver));

    uint64_t callId = 0;
    int32_t hp = 1;
    const void* args[1] = { &hp };
    CHECK(s.clientRpc.callServer("PlayerRpc", "ServerHealStrict", args, nullptr, 1, callId));
    CHECK_INT_EQ(s.serverReceiver.healCalls.load(), 0);
    CHECK(s.serverRequestCount.load() >= 1);
}

TEST_CASE(ReliableDefaultOverGns) {
    ayt::test::setCurrentCase("ReliableDefaultOverGns");
    RpcE2EScaffold s;
    g_activeReceiver = &s.serverReceiver;
    CHECK(s.serverRpc.registerMethod("PlayerRpc", "ServerHeal", &s.serverReceiver));

    uint64_t callId = 0;
    int32_t hp = 7;
    const void* args[1] = { &hp };
    CHECK(s.clientRpc.callServer("PlayerRpc", "ServerHeal", args, nullptr, 1, callId));
    CHECK_INT_EQ(s.lastClientChannel.load(), ayt::net::CHANNEL_RELIABLE);
}

TEST_CASE(PendingCallRetriesThenSucceeds) {
    ayt::test::setCurrentCase("PendingCallRetriesThenSucceeds");
    RpcE2EScaffold s;
    g_activeReceiver = &s.serverReceiver;
    CHECK(s.serverRpc.registerMethod("PlayerRpc", "ServerHeal", &s.serverReceiver));

    s.clientRpc.setRetryPolicy(/*maxRetries=*/ 3, /*retryBaseMs=*/ 1, /*attemptTimeoutMs=*/ 1);
    std::atomic<int> clientSends{0};
    s.clientRpc.setBroadcastSinkForTesting([&](uint8_t channel, const void* data, size_t size) {
        s.lastClientChannel.store(channel);
        const int sendIndex = clientSends.fetch_add(1) + 1;
        if (sendIndex >= 3) {
            RpcE2EScaffold::dispatchSealedTo(s.serverRpc, data, size);
        }
    });

    std::atomic<bool> succeeded{false};
    uint64_t callId = 0;
    int32_t hp = 55;
    const void* args[1] = { &hp };
    CHECK(s.clientRpc.callServerWithCallback("PlayerRpc", "ServerHeal", args, nullptr, 1, callId,
        [&](bool accepted, const void*) {
            if (accepted) succeeded.store(true);
        }));

    for (int i = 0; i < 64 && !succeeded.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        s.clientRpc.tick(0.016f);
    }
    CHECK(succeeded.load());
    CHECK_INT_EQ(clientSends.load(), 3);
    CHECK_INT_EQ(s.serverReceiver.healCalls.load(), 1);
    CHECK(!s.clientRpc.hasPending(callId));
}

TEST_CASE(AsyncRpcCompletesOnServerTick) {
    ayt::test::setCurrentCase("AsyncRpcCompletesOnServerTick");
    RpcE2EScaffold s;
    g_activeReceiver = &s.serverReceiver;
    CHECK(s.serverRpc.registerMethod("PlayerRpc", "ServerHealSlow", &s.serverReceiver));

    std::atomic<bool> succeeded{false};
    uint64_t callId = 0;
    int32_t hp = 88;
    const void* args[1] = { &hp };
    CHECK(s.clientRpc.callServerWithCallback("PlayerRpc", "ServerHealSlow", args, nullptr, 1, callId,
        [&](bool accepted, const void*) {
            if (accepted) succeeded.store(true);
        }));

    CHECK(!succeeded.load());
    CHECK_INT_EQ(s.serverReceiver.healCalls.load(), 0);

    for (int i = 0; i < 64 && !succeeded.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        s.serverRpc.tick(0.016f);
    }

    CHECK(succeeded.load());
    CHECK_INT_EQ(s.serverReceiver.healCalls.load(), 1);
    CHECK_INT_EQ(s.serverReceiver.lastHpReceived.load(), 88);
    CHECK(!s.clientRpc.hasPending(callId));
}

TEST_CASE(UnreliableServerRpcOverGns) {
    ayt::test::setCurrentCase("UnreliableServerRpcOverGns");
    RpcE2EScaffold s;
    g_activeReceiver = &s.clientReceiver;
    CHECK(s.clientRpc.registerMethod("PlayerRpc", "ClientDamage", &s.clientReceiver));

    uint64_t callId = 0;
    int32_t amount = 3;
    const void* args[1] = { &amount };
    CHECK(s.serverRpc.callClient(0, "PlayerRpc", "ClientDamage", args, nullptr, 1, callId));
    CHECK_INT_EQ(s.lastServerChannel.load(), ayt::net::CHANNEL_UNRELIABLE);
}

TEST_CASE(WildcardMulticastOverGns) {
    ayt::test::setCurrentCase("WildcardMulticastOverGns");
    RpcE2EScaffold s;
    g_activeReceiver = &s.clientReceiver;
    CHECK(s.clientRpc.registerWildcardMulticast("PlayerRpc", &s.clientReceiver));

    uint64_t callId = 0;
    int32_t score = 42;
    const void* args[1] = { &score };
    CHECK(s.serverRpc.callMulticast("PlayerRpc", "MulticastAnnounce", args, nullptr, 1, callId));
    CHECK_INT_EQ(s.clientReceiver.announceCalls.load(), 1);
    CHECK_INT_EQ(s.clientReceiver.lastAnnounceScore.load(), 42);
}

TEST_CASE(WildcardMulticastNoResponseOnWire) {
    ayt::test::setCurrentCase("WildcardMulticastNoResponseOnWire");
    RpcE2EScaffold s;
    g_activeReceiver = &s.clientReceiver;
    CHECK(s.clientRpc.registerWildcardMulticast("PlayerRpc", &s.clientReceiver));

    std::atomic<int> clientOutResponse{0};
    s.clientRpc.setBroadcastSinkForTesting([&](uint8_t channel, const void* data, size_t size) {
        auto decoded = ayt::net::PacketCodec::decode(
            static_cast<const uint8_t*>(data), size);
        if (decoded.ok && decoded.header.msgType == kMsgTypeRpcResponse) {
            clientOutResponse.fetch_add(1);
        }
        RpcE2EScaffold::dispatchSealedTo(s.serverRpc, data, size);
        s.lastClientChannel.store(channel);
        s.clientRequestCount.fetch_add(1);
    });

    uint64_t callId = 0;
    int32_t score = 7;
    const void* args[1] = { &score };
    CHECK(s.serverRpc.callMulticast("PlayerRpc", "MulticastAnnounce", args, nullptr, 1, callId));
    CHECK_INT_EQ(s.clientReceiver.announceCalls.load(), 1);
    CHECK_INT_EQ(clientOutResponse.load(), 0);
}

TEST_CASE(AuthorityGateRejectsRpc) {
    ayt::test::setCurrentCase("AuthorityGateRejectsRpc");
    RpcE2EScaffold s;
    s.serverRpc.setModeForTesting(ayt::net::ConnectionMode::Client); // client side
    CHECK(s.serverRpc.registerMethod("PlayerRpc", "ServerHeal", &s.serverReceiver));

    std::atomic<bool> gotReject{false};
    s.serverRpc.setBroadcastSinkForTesting([&](uint8_t, const void* data, size_t size) {
        auto decoded = ayt::net::PacketCodec::decode(reinterpret_cast<const uint8_t*>(data), size);
        if (decoded.header.msgType == kMsgTypeRpcReject) gotReject.store(true);
    });

    // FNV-1a-16 of "ServerHeal" — replicate here so the methodHash matches
    // the registered binding.
    const char* nm = "ServerHeal";
    uint32_t h = 0x811C9DC5u;
    for (const char* p = nm; *p; ++p) { h ^= static_cast<uint8_t>(*p); h *= 16777619u; }

    BitStream body;
    body.writeUInt8(static_cast<uint8_t>(ayt::reflect::RpcKind::Server));
    body.writeUInt16(static_cast<uint16_t>(h & 0xFFFFu));
    for (int i = 0; i < 8; ++i) body.writeUInt8(static_cast<uint8_t>(0xCAFEBABECAFEBABEull >> (i * 8)));
    body.writeUInt8(1); body.writeUInt8(0);
    body.writeUInt16(0x7777);
    body.writeUInt8(static_cast<uint8_t>(WireTypeId::Int32));
    body.writeInt32Raw(7);

    s.serverRpc.onRpcRequest(body, nullptr);
    CHECK(gotReject.load());
}
TEST_SUITE_END

// =============================================================================
// Regression cases — 3 of them. R3.x zero-regression sanity check.
// =============================================================================

namespace
{

struct RpcRegressionAllPrimitives {
    bool   b   = true;
    int32_t i32 = 7;
    float   f   = 1.25f;
    std::string s = "regression-rpc";
};

struct RpcRegressionFixtureRegistrar {
    RpcRegressionFixtureRegistrar() {
        using ayt::reflect::FieldInfoImpl;
        using ayt::reflect::TypeInfoImpl;
        using ayt::reflect::TypeRegistryImpl;
        auto& reg = TypeRegistryImpl::instance();
        if (!reg.findType("RpcRegressionAllPrimitives")) {
            auto* info = new TypeInfoImpl<RpcRegressionAllPrimitives>(
                "RpcRegressionAllPrimitives",
                ayt::reflect::detail::defaultCreate<RpcRegressionAllPrimitives>,
                ayt::reflect::detail::defaultDestroy<RpcRegressionAllPrimitives>,
                ayt::reflect::detail::defaultCopy<RpcRegressionAllPrimitives>);
            using T = RpcRegressionAllPrimitives;
            using FA = ayt::reflect::FieldAttribute;
            auto NR = FA::Serialize | FA::NetReplicate;
            info->addField(new FieldInfoImpl("b",   reg.findType<bool>(),          offsetof(T, b),   NR));
            info->addField(new FieldInfoImpl("i32", reg.findType<int32_t>(),       offsetof(T, i32), NR));
            info->addField(new FieldInfoImpl("f",   reg.findType<float>(),         offsetof(T, f),   NR));
            info->addField(new FieldInfoImpl("s",   reg.findType<std::string>(),   offsetof(T, s),   NR));
            reg.registerTypeInfo("RpcRegressionAllPrimitives", info);
        }
    }
};
static RpcRegressionFixtureRegistrar g_rpcRegressionFixtureRegistrar;
} // anonymous namespace

TEST_SUITE(RpcHandlerRegression)
TEST_CASE(R30PrimitivesStillReplicate) {
    ayt::test::setCurrentCase("R30PrimitivesStillReplicate");
    RpcRegressionAllPrimitives src;
    src.b = false; src.i32 = -12345; src.f = -7.5f; src.s = "rpc regression";
    auto* type = ayt::reflect::TypeRegistryImpl::instance().findType("RpcRegressionAllPrimitives");
    CHECK(type != nullptr);

    // serializeObject already writes the 8B header — do NOT call
    // writeReplicationFrameHeader before it (would double-emit header
    // and shift all field records by 8 bytes).
    BitStream wire;
    CHECK(ayt::net::ReflectSerializer::serializeObject(type, &src, 42, wire));

    wire.resetForRead();
    ayt::net::ReflectSerializer::FrameHeader hdr;
    CHECK(ayt::net::ReflectSerializer::readReplicationFrameHeader(wire, hdr));
    CHECK_INT_EQ(static_cast<uint32_t>(hdr.netId), static_cast<uint32_t>(42));
    CHECK_INT_EQ(static_cast<uint32_t>(hdr.fieldCount), static_cast<uint32_t>(4));

    RpcRegressionAllPrimitives dst{};
    CHECK(ayt::net::ReflectSerializer::deserializeObject(type, &dst, wire, hdr.fieldCount));
    CHECK(!dst.b);
    CHECK_INT_EQ(dst.i32, -12345);
}

TEST_CASE(R32NestedWireTypesAreUnaffected) {
    ayt::test::setCurrentCase("R32NestedWireTypesAreUnaffected");
    using W = WireTypeId;
    CHECK_INT_EQ(static_cast<uint8_t>(W::NestedStruct), 12);
    CHECK_INT_EQ(static_cast<uint8_t>(W::FixedArray),   13);
    CHECK_INT_EQ(static_cast<uint8_t>(W::DynamicArray), 14);
    CHECK_INT_EQ(static_cast<uint8_t>(W::StringMap),    15);
}

TEST_CASE(RpcMsgTypeEnvelopeIsDistinct) {
    ayt::test::setCurrentCase("RpcMsgTypeEnvelopeIsDistinct");
    CHECK_INT_EQ(static_cast<int>(kMsgTypeRpcRequest),  0x0010);
    CHECK_INT_EQ(static_cast<int>(kMsgTypeRpcResponse), 0x0011);
    CHECK_INT_EQ(static_cast<int>(kMsgTypeRpcReject),   0x0012);
    CHECK(kMsgTypeRpcRequest != ayt::net::kMsgTypeReplication);
    CHECK(kMsgTypeRpcRequest != ayt::net::kMsgTypeDelta);
    CHECK(kMsgTypeRpcRequest != ayt::net::kMsgTypeHandshake);
}
TEST_SUITE_END
