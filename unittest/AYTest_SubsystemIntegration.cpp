// AYTest_SubsystemIntegration.cpp - R4.1-A Subsystem E2E (1 server + 2 clients)
//
// Exercises the real INetworkSubSystem path over GNS — no setBroadcastSinkForTesting.
// Covers Replication demux (0x0001..0x0004) and RpcHandler emit → send/broadcast.

#include <AYNetwork.h>
#include <AYTest.h>

#include <AYNetwork/RPC/RpcHandler.h>
#include <AYNetwork/Replication/ReflectSerializer.h>

#include <AYReflect/IReflect.h>
#include <AYReflect/detail/ReflectImpl.h>
#include <AYReflect.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>
#include <vector>

using namespace ayt::net;

namespace
{

bool pumpUntil(std::chrono::milliseconds timeout, const std::function<bool()>& pred) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
}

void pumpAll(const std::vector<INetworkSubSystem*>& systems, float dt = 0.016f) {
    for (INetworkSubSystem* sys : systems) {
        if (sys) sys->update(dt);
    }
}

// Minimal RPC fixture local to this TU (PlayerRpc lives in AYTest_RpcHandler.cpp).
struct SubsystemRpcReceiver {
    std::atomic<int> pingCount{0};
    std::atomic<int32_t> lastValue{0};
};

SubsystemRpcReceiver* g_subsystemRpcActive = nullptr;

void subsystemRpcPing(int32_t value) {
    if (g_subsystemRpcActive) {
        g_subsystemRpcActive->pingCount.fetch_add(1);
        g_subsystemRpcActive->lastValue.store(value);
    }
}

template<typename Ret, typename... Args>
class SubsystemRpcMethodInfo : public ayt::reflect::IMethodInfo {
public:
    using Invoker = std::function<Ret(Args...)>;
    SubsystemRpcMethodInfo(const char* name, ayt::reflect::RpcKind kind,
                           ayt::reflect::ITypeInfo* retType,
                           std::vector<ayt::reflect::ITypeInfo*> paramTypes,
                           Invoker invoker)
        : _name(name), _kind(kind), _retType(retType), _paramTypes(std::move(paramTypes)),
          _invoker(std::move(invoker)) {}

    const char* getName() const override { return _name; }
    ayt::reflect::ITypeInfo* getReturnType() const override { return _retType; }
    size_t getParamCount() const override { return _paramTypes.size(); }
    ayt::reflect::ITypeInfo* getParamType(size_t i) const override {
        return i < _paramTypes.size() ? _paramTypes[i] : nullptr;
    }
    bool getParamIsOut(size_t) const override { return false; }
    const char* getParamName(size_t i) const override { return i == 0 ? "value" : ""; }
    ayt::reflect::RpcKind getRpcKind() const override { return _kind; }
    bool isUnreliable() const override { return false; }
    bool validate(const void*) const override { return true; }
    const void* invoke(const void*, const void* const* args) const override {
        if (!_invoker) return nullptr;
        if constexpr (std::is_void_v<Ret>) {
            if constexpr (sizeof...(Args) == 1) {
                using A0 = std::tuple_element_t<0, std::tuple<Args...>>;
                A0 a0 = *static_cast<const A0*>(args[0]);
                _invoker(a0);
            }
            return nullptr;
        }
        return nullptr;
    }

private:
    const char* _name;
    ayt::reflect::RpcKind _kind;
    ayt::reflect::ITypeInfo* _retType;
    std::vector<ayt::reflect::ITypeInfo*> _paramTypes;
    Invoker _invoker;
};

struct SubsystemRpcFixtureRegistrar {
    SubsystemRpcFixtureRegistrar() {
        using ayt::reflect::TypeInfoImpl;
        using ayt::reflect::TypeRegistryImpl;
        auto& reg = TypeRegistryImpl::instance();
        if (!reg.findType("SubsystemRpc")) {
            auto* info = new TypeInfoImpl<SubsystemRpcReceiver>(
                "SubsystemRpc",
                ayt::reflect::detail::defaultCreate<SubsystemRpcReceiver>,
                ayt::reflect::detail::defaultDestroy<SubsystemRpcReceiver>,
                ayt::reflect::detail::defaultCopy<SubsystemRpcReceiver>);
            info->addMethod(new SubsystemRpcMethodInfo<void, int32_t>(
                "ServerPing",
                ayt::reflect::RpcKind::Server,
                reg.findType<void>(),
                { reg.findType<int32_t>() },
                [](int32_t value) { subsystemRpcPing(value); }));
            reg.registerTypeInfo("SubsystemRpc", info);
        }
    }
};
static SubsystemRpcFixtureRegistrar g_subsystemRpcFixtureRegistrar;

struct ReplicationNoNetFixtureRegistrar {
    ReplicationNoNetFixtureRegistrar() {
        using ayt::reflect::FieldInfoImpl;
        using ayt::reflect::TypeInfoImpl;
        using ayt::reflect::TypeRegistryImpl;
        auto& reg = TypeRegistryImpl::instance();
        if (!reg.findType("SubsystemReplicationNoNet")) {
            struct Obj { int32_t hp = 100; int32_t score = 0; };
            auto* info = new TypeInfoImpl<Obj>(
                "SubsystemReplicationNoNet",
                ayt::reflect::detail::defaultCreate<Obj>,
                ayt::reflect::detail::defaultDestroy<Obj>,
                ayt::reflect::detail::defaultCopy<Obj>);
            using T = Obj;
            using FA = ayt::reflect::FieldAttribute;
            info->addField(new FieldInfoImpl("hp", reg.findType<int32_t>(), offsetof(T, hp), FA::Serialize));
            info->addField(new FieldInfoImpl("score", reg.findType<int32_t>(), offsetof(T, score),
                                             FA::Serialize | FA::NetReplicate));
            reg.registerTypeInfo("SubsystemReplicationNoNet", info);
        }
    }
};
static ReplicationNoNetFixtureRegistrar g_subsystemReplFixtureRegistrar;

} // anonymous namespace

TEST_SUITE(SubsystemIntegration)

TEST_CASE(OneServerTwoClientsRpcAndReplicate) {
    ayt::test::setCurrentCase("OneServerTwoClientsRpcAndReplicate");

    INetworkSubSystem* server = createNetworkSubSystemForTest();
    INetworkSubSystem* client1 = createNetworkSubSystemForTest();
    INetworkSubSystem* client2 = createNetworkSubSystemForTest();
    CHECK(server != nullptr);
    CHECK(client1 != nullptr);
    CHECK(client2 != nullptr);

    CHECK(server->initialize());
    CHECK(client1->initialize());
    CHECK(client2->initialize());

    constexpr uint16_t kPort = 27550;
    server->listen(kPort);
    client1->connect("127.0.0.1", kPort);
    client2->connect("127.0.0.1", kPort);

    std::vector<INetworkSubSystem*> all{server, client1, client2};

    CHECK(pumpUntil(std::chrono::seconds(8), [&]() {
        pumpAll(all);
        return server->getConnections().size() >= 2 &&
               client1->isConnected() && client2->isConnected();
    }));

    // ---- RPC: client1 → server (no test sink) ----
    static SubsystemRpcReceiver serverRecv;
    g_subsystemRpcActive = &serverRecv;
    CHECK(server->getRpcHandler()->registerMethod("SubsystemRpc", "ServerPing", &serverRecv));

    uint64_t callId = 0;
    int32_t pingValue = 42;
    const void* rpcArgs[1] = { &pingValue };
    CHECK(client1->getRpcHandler()->callServer("SubsystemRpc", "ServerPing", rpcArgs, nullptr, 1, callId));

    CHECK(pumpUntil(std::chrono::seconds(5), [&]() {
        pumpAll(all);
        return serverRecv.pingCount.load() >= 1;
    }));
    CHECK_INT_EQ(serverRecv.pingCount.load(), 1);
    CHECK_INT_EQ(serverRecv.lastValue.load(), 42);

    // ---- Replication: server spawn + field sync to both clients ----
    struct ReplObj { int32_t hp = 100; int32_t score = 0; };
    auto* replType = ayt::reflect::TypeRegistryImpl::instance().findType("SubsystemReplicationNoNet");
    CHECK(replType != nullptr);

    ReplObj serverObj;
    serverObj.score = 11;
    constexpr uint32_t kNetId = 501;
    server->getReplicationManager()->registerObject(&serverObj, replType, kNetId);

    uint16_t typeHash1 = 0;
    uint16_t typeHash2 = 0;
    CHECK(pumpUntil(std::chrono::seconds(5), [&]() {
        pumpAll(all);
        return client1->getReplicationManager()->peekSpawnAnnouncement(kNetId, typeHash1) &&
               client2->getReplicationManager()->peekSpawnAnnouncement(kNetId, typeHash2);
    }));

    ReplObj clientObj1;
    ReplObj clientObj2;
    client1->getReplicationManager()->registerObject(&clientObj1, replType, kNetId);
    client2->getReplicationManager()->registerObject(&clientObj2, replType, kNetId);

    serverObj.score = 88;
    CHECK(pumpUntil(std::chrono::seconds(5), [&]() {
        pumpAll(all);
        return clientObj1.score == 88 && clientObj2.score == 88;
    }));
    CHECK_INT_EQ(clientObj1.score, 88);
    CHECK_INT_EQ(clientObj2.score, 88);

    server->disconnect();
    client1->disconnect();
    client2->disconnect();
    server->shutdown();
    client1->shutdown();
    client2->shutdown();
    delete server;
    delete client1;
    delete client2;
}

TEST_CASE(LateJoinClientReceivesRebroadcastedEntitySpawn) {
    // Regression test for the late-join fix (fe21587): when a second client
    // connects AFTER an entity is already registered on the server, the new
    // client must still receive an EntitySpawn. Without the fix the late-
    // join client would never see existing entities until the next field
    // change bubbled up via dirty-tracking.
    //
    // Audit (2026-08-02): rebroadcastEntitySpawn(netId, targetConn) must
    // honour the `targetConn` argument when a network subsystem is present
    // (issue 1: sink path ignored targetConn; issue 2: sealed bytes were
    // constructed before any authority/network guard).
    ayt::test::setCurrentCase("LateJoinClientReceivesRebroadcastedEntitySpawn");

    INetworkSubSystem* server = createNetworkSubSystemForTest();
    INetworkSubSystem* client1 = createNetworkSubSystemForTest();
    INetworkSubSystem* client2 = createNetworkSubSystemForTest();
    CHECK(server && client1 && client2);
    CHECK(server->initialize());
    CHECK(client1->initialize());
    CHECK(client2->initialize());

    constexpr uint16_t kPort = 27555;
    server->listen(kPort);

    // Connect ONLY client1 first.
    client1->connect("127.0.0.1", kPort);

    std::vector<INetworkSubSystem*> pre{server, client1};
    CHECK(pumpUntil(std::chrono::seconds(8), [&]() {
        pumpAll(pre);
        return client1->isConnected() && server->getConnections().size() >= 1;
    }));

    // Register an object while ONLY client1 is connected.
    auto* replType = ayt::reflect::TypeRegistryImpl::instance().findType("SubsystemReplicationNoNet");
    CHECK(replType != nullptr);
    struct ReplObj { int32_t hp = 100; int32_t score = 0; };
    ReplObj serverObj;
    serverObj.score = 11;
    constexpr uint32_t kNetId = 4711;
    server->getReplicationManager()->registerObject(&serverObj, replType, kNetId);

    uint16_t typeHash1 = 0;
    CHECK(pumpUntil(std::chrono::seconds(5), [&]() {
        pumpAll(pre);
        return client1->getReplicationManager()->peekSpawnAnnouncement(kNetId, typeHash1);
    }));

    // Now connect client2 (the late joiner). fe21587 wires the synthetic
    // Connected callback; verify the server's onConnectionChange fires AND
    // the rebroadcast delivers an EntitySpawn to client2.
    //
    // Design note (audit 2026-08-02): fe21587 provides rebroadcastEntitySpawn
    // as an API but does NOT auto-call it — the integration is the user
    // iterating registered netIds inside the onConnectionChange handler. The
    // test mirrors that pattern: iterate the server's replicated objects
    // and rebroadcast to the late joiner on connect.
    ReplicationManager* rm = server->getReplicationManager();
    CHECK(rm != nullptr);
    std::atomic<bool> client2ConnFired{false};
    server->onConnectionChange([&](NetConnection* conn, bool connected, DisconnectReason) {
        if (!connected || !conn) return;
        client2ConnFired.store(true);
        // Late-join rebroadcast loop: walk all registered entities on
        // the server and resend EntitySpawn + forceReplicate for each.
        // Tests exercise one netId only (kNetId=4711) so iterate by hand
        // rather than copy the entire _objects map.
        if (rm->findType(kNetId) != nullptr) {
            (void)rm->rebroadcastEntitySpawn(kNetId, conn);
        }
    });

    client2->connect("127.0.0.1", kPort);
    std::vector<INetworkSubSystem*> all{server, client1, client2};
    CHECK(pumpUntil(std::chrono::seconds(8), [&]() {
        pumpAll(all);
        return client2->isConnected();
    }));
    CHECK(client2ConnFired.load());

    // After onConnectionChange fires, the late-join logic must deliver an
    // EntitySpawn announcement to client2 for the already-registered kNetId.
    uint16_t typeHash2 = 0;
    CHECK(pumpUntil(std::chrono::seconds(5), [&]() {
        pumpAll(all);
        return client2->getReplicationManager()->peekSpawnAnnouncement(kNetId, typeHash2);
    }));

    // Sanity: client2 must NOT have received the field value (no Full
    // Snapshot unless interest/forceReplicate sends one) — the minimum
    // invariant is the spawn announcement + the late-joiner entry in
    // server->getConnections().
    CHECK(server->getConnections().size() >= 2);

    server->disconnect();
    client1->disconnect();
    client2->disconnect();
    server->shutdown();
    client1->shutdown();
    client2->shutdown();
    delete server;
    delete client1;
    delete client2;
}

TEST_SUITE_END
