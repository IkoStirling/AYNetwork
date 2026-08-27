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
#include <cstring>
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

    uint64_t typeHash1 = 0;
    uint64_t typeHash2 = 0;
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

TEST_CASE(LateJoinClientAutomaticallyReceivesSpawnAndFull) {
    ayt::test::setCurrentCase("LateJoinClientAutomaticallyReceivesSpawnAndFull");

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

    uint64_t typeHash1 = 0;
    CHECK(pumpUntil(std::chrono::seconds(5), [&]() {
        pumpAll(pre);
        return client1->getReplicationManager()->peekSpawnAnnouncement(kNetId, typeHash1);
    }));

    // Pre-register the late joiner's destination slot. The authority must
    // discover the new peer by itself and send Spawn + Full on its next tick;
    // no connection callback or manual rebroadcast is allowed.
    ReplObj client2Obj;
    client2Obj.score = -1;
    client2->getReplicationManager()->registerObject(&client2Obj, replType, kNetId);

    client2->connect("127.0.0.1", kPort);
    std::vector<INetworkSubSystem*> all{server, client1, client2};
    uint64_t typeHash2 = 0;
    CHECK(pumpUntil(std::chrono::seconds(8), [&]() {
        pumpAll(all);
        return client2->isConnected() && client2Obj.score == 11 &&
               client2->getReplicationManager()->peekSpawnAnnouncement(kNetId, typeHash2);
    }));
    CHECK_INT_EQ(client2Obj.score, 11);
    CHECK(typeHash2 == ReflectSerializer::hashTypeSchema(replType));
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

TEST_CASE(ApplicationHandlerReceivesDecodedBodyExactlyOnce) {
    ayt::test::setCurrentCase("ApplicationHandlerReceivesDecodedBodyExactlyOnce");

    INetworkSubSystem* server = createNetworkSubSystemForTest();
    INetworkSubSystem* client = createNetworkSubSystemForTest();
    CHECK(server && client);
    CHECK(server->initialize());
    CHECK(client->initialize());

    int deliveryCount = 0;
    std::vector<uint8_t> received;
    server->onMessage(CHANNEL_RELIABLE,
        [&](NetConnection* from, uint8_t channel, const void* data, size_t size) {
            CHECK(from != nullptr);
            CHECK_INT_EQ(channel, CHANNEL_RELIABLE);
            ++deliveryCount;
            const auto* bytes = static_cast<const uint8_t*>(data);
            received.assign(bytes, bytes + size);
        });

    constexpr uint16_t kPort = 27556;
    server->listen(kPort);
    client->connect("127.0.0.1", kPort);
    std::vector<INetworkSubSystem*> systems{server, client};
    CHECK(pumpUntil(std::chrono::seconds(8), [&]() {
        pumpAll(systems);
        return client->isConnected() && !server->getConnections().empty();
    }));

    const std::vector<uint8_t> payload{0x10, 0x20, 0x30, 0x40, 0x50};
    client->send(CHANNEL_RELIABLE, payload.data(), payload.size());
    CHECK(pumpUntil(std::chrono::seconds(5), [&]() {
        pumpAll(systems);
        return deliveryCount == 1;
    }));
    CHECK(received == payload);

    // A host that still invokes the legacy fixedUpdate adapter after update
    // must not advance transport/RPC processing a second time.
    for (int i = 0; i < 10; ++i) {
        server->update(0.016f);
        server->fixedUpdate(0.016f);
        client->update(0.016f);
        client->fixedUpdate(0.016f);
    }
    CHECK_INT_EQ(deliveryCount, 1);

    server->disconnect();
    client->disconnect();
    server->shutdown();
    client->shutdown();
    delete server;
    delete client;
}

TEST_CASE(OnePeerCanReconnectWhileAnotherPeerRemainsUsable) {
    ayt::test::setCurrentCase("OnePeerCanReconnectWhileAnotherPeerRemainsUsable");

    std::unique_ptr<INetworkSubSystem> server(createNetworkSubSystemForTest());
    std::unique_ptr<INetworkSubSystem> client1(createNetworkSubSystemForTest());
    std::unique_ptr<INetworkSubSystem> client2(createNetworkSubSystemForTest());
    CHECK(server && client1 && client2);
    CHECK(server->initialize());
    CHECK(client1->initialize());
    CHECK(client2->initialize());

    std::atomic<int> client1Connected{0};
    std::atomic<int> client1Disconnected{0};
    client1->onConnectionChange([&](NetConnection*, bool connected, DisconnectReason) {
        if (connected) ++client1Connected;
        else ++client1Disconnected;
    });
    std::atomic<int> client2Deliveries{0};
    server->onMessage(CHANNEL_RELIABLE,
        [&](NetConnection*, uint8_t, const void* data, size_t size) {
            static constexpr char kKeepAlive[] = "peer-two-still-alive";
            if (size == sizeof(kKeepAlive) - 1 &&
                std::memcmp(data, kKeepAlive, size) == 0) {
                ++client2Deliveries;
            }
        });

    constexpr uint16_t kPort = 27557;
    server->listen(kPort);
    client1->connect("127.0.0.1", kPort);
    client2->connect("127.0.0.1", kPort);
    std::vector<INetworkSubSystem*> systems{server.get(), client1.get(), client2.get()};
    CHECK(pumpUntil(std::chrono::seconds(8), [&]() {
        pumpAll(systems);
        return server->getConnections().size() == 2 &&
               client1->isConnected() && client2->isConnected();
    }));
    CHECK_INT_EQ(client1Connected.load(), 1);
    CHECK_INT_EQ(client1Disconnected.load(), 0);

    client1->disconnect();
    CHECK(pumpUntil(std::chrono::seconds(5), [&]() {
        pumpAll(systems);
        return server->getConnections().size() == 1 &&
               !client1->isConnected() && client2->isConnected();
    }));
    CHECK_INT_EQ(client1Disconnected.load(), 1);

    static constexpr char kKeepAlive[] = "peer-two-still-alive";
    client2->send(CHANNEL_RELIABLE, kKeepAlive, sizeof(kKeepAlive) - 1);
    CHECK(pumpUntil(std::chrono::seconds(5), [&]() {
        pumpAll(systems);
        return client2Deliveries.load() == 1;
    }));

    client1->connect("127.0.0.1", kPort);
    CHECK(pumpUntil(std::chrono::seconds(8), [&]() {
        pumpAll(systems);
        return server->getConnections().size() == 2 && client1->isConnected();
    }));
    CHECK_INT_EQ(client1Connected.load(), 2);
    CHECK_INT_EQ(client1Disconnected.load(), 1);

    server->disconnect();
    client1->disconnect();
    client2->disconnect();
    server->shutdown();
    client1->shutdown();
    client2->shutdown();
}

TEST_CASE(ProductionDefaultsAndAdmissionGate) {
    ayt::test::setCurrentCase("ProductionDefaultsAndAdmissionGate");

    INetworkSubSystem* server = createNetworkSubSystemForTest();
    INetworkSubSystem* client = createNetworkSubSystemForTest();
    CHECK(server != nullptr);
    CHECK(client != nullptr);
    CHECK(server->initialize());
    CHECK(client->initialize());

    CHECK_INT_EQ(server->getProtocolVersion(), kProtocolVersion);
    CHECK(!server->isConnected());

    NetworkLimits limits = server->getLimits();
    limits.maxConnections = 1;
    limits.maxQueuedInboundMessages = 16;
    server->setLimits(limits);
    CHECK_INT_EQ(server->getLimits().maxConnections, 1);

    std::atomic<int> admissionCalls{0};
    server->setAcceptCallback([&](NetConnection* connection) {
        CHECK(connection != nullptr);
        admissionCalls.fetch_add(1);
        return false;
    });

    constexpr uint16_t kPort = 27552;
    server->listen(kPort);
    CHECK(server->isListening());
    CHECK(!server->isConnected());
    client->connect("127.0.0.1", kPort);

    std::vector<INetworkSubSystem*> systems{server, client};
    CHECK(pumpUntil(std::chrono::seconds(5), [&]() {
        pumpAll(systems);
        return admissionCalls.load() == 1;
    }));
    CHECK(server->getConnections().empty());

    server->disconnect();
    client->disconnect();
    server->shutdown();
    client->shutdown();
    delete server;
    delete client;
}

TEST_CASE(SubsystemProtocolMismatchIsRejected) {
    ayt::test::setCurrentCase("SubsystemProtocolMismatchIsRejected");

    INetworkSubSystem* server = createNetworkSubSystemForTest();
    INetworkSubSystem* client = createNetworkSubSystemForTest();
    CHECK(server->initialize());
    CHECK(client->initialize());
    client->setProtocolVersion(kProtocolVersion + 1);

    std::atomic<bool> mismatch{false};
    client->onConnectionChange(
        [&](NetConnection*, bool connected, DisconnectReason reason) {
            if (!connected && reason == DisconnectReason::ProtocolMismatch) {
                mismatch.store(true);
            }
        });

    constexpr uint16_t kPort = 27553;
    server->listen(kPort);
    client->connect("127.0.0.1", kPort);
    std::vector<INetworkSubSystem*> systems{server, client};
    CHECK(pumpUntil(std::chrono::seconds(5), [&]() {
        pumpAll(systems);
        return mismatch.load();
    }));
    CHECK(server->getConnections().empty());

    server->disconnect();
    client->disconnect();
    server->shutdown();
    client->shutdown();
    delete server;
    delete client;
}

TEST_SUITE_END
