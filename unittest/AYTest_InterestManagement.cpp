// AYTest_InterestManagement.cpp - R4.1-B Interest Management tests

#include <AYNetwork.h>
#include <AYTest.h>

#include <AYNetwork/Protocol/PacketCodec.h>
#include <AYNetwork/Replication/ReflectSerializer.h>

#include <AYReflect/IReflect.h>
#include <AYReflect/detail/ReflectImpl.h>
#include <AYReflect.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <vector>

using namespace ayt::net;

namespace
{

struct InterestReplObj {
    int32_t score = 0;
};

struct InterestReplRegistrar {
    InterestReplRegistrar() {
        using ayt::reflect::FieldInfoImpl;
        using ayt::reflect::TypeInfoImpl;
        using ayt::reflect::TypeRegistryImpl;
        auto& reg = TypeRegistryImpl::instance();
        if (!reg.findType("InterestReplObj")) {
            auto* info = new TypeInfoImpl<InterestReplObj>(
                "InterestReplObj",
                ayt::reflect::detail::defaultCreate<InterestReplObj>,
                ayt::reflect::detail::defaultDestroy<InterestReplObj>,
                ayt::reflect::detail::defaultCopy<InterestReplObj>);
            using T = InterestReplObj;
            info->addField(new FieldInfoImpl(
                "score", reg.findType<int32_t>(), offsetof(T, score),
                ayt::reflect::FieldAttribute::Serialize | ayt::reflect::FieldAttribute::NetReplicate));
            reg.registerTypeInfo("InterestReplObj", info);
        }
    }
};
static InterestReplRegistrar g_interestReplRegistrar;

class MockNetConnection : public NetConnection {
public:
    MockNetConnection(uint32_t id, NetVec3* viewerPos)
        : _id(id), _viewerPos(viewerPos) {}

    uint32_t getId() const override { return _id; }
    uint32_t getHostId() const override { return _id; }
    const char* getAddress() const override { return "127.0.0.1"; }
    bool isConnected() const override { return true; }
    int getPing() const override { return 0; }
    void send(uint8_t, const void* data, size_t size) override {
        sendCount.fetch_add(1);
        const DecodedPacket decoded = PacketCodec::decode(
            static_cast<const uint8_t*>(data), size);
        if (decoded.ok) {
            messageTypes.push_back(decoded.header.msgType);
            packets.push_back(decoded);
        }
    }
    void disconnect(const char*) override {}
    void setUserData(void* data) override { _viewerPos = static_cast<NetVec3*>(data); }
    void* getUserData() const override { return _viewerPos; }

    std::atomic<int> sendCount{0};
    std::vector<uint16_t> messageTypes;
    std::vector<DecodedPacket> packets;

    size_t countMessage(uint16_t messageType) const {
        return static_cast<size_t>(std::count(
            messageTypes.begin(), messageTypes.end(), messageType));
    }

    void clearMessages() {
        sendCount.store(0);
        messageTypes.clear();
        packets.clear();
    }

private:
    uint32_t _id;
    NetVec3* _viewerPos = nullptr;
};

class InterestTestNetwork : public INetworkSubSystem {
public:
    MockNetConnection* addConnection(uint32_t id, NetVec3* viewerPos) {
        auto conn = std::make_unique<MockNetConnection>(id, viewerPos);
        MockNetConnection* raw = conn.get();
        _owned.push_back(std::move(conn));
        _ptrs.push_back(raw);
        return raw;
    }

    const char* getName() const override { return "InterestTestNetwork"; }
    const ::ayt::game::SubSystemDescriptor& getDescriptor() const override {
        static ::ayt::game::SubSystemDescriptor d{"InterestTestNetwork", {}, 0};
        return d;
    }
    bool initialize() override { return true; }
    void shutdown() override {}
    void update(float) override {}
    void fixedUpdate(float dt) override { update(dt); }
    void connect(const char*, uint16_t) override {}
    void listen(uint16_t) override {}
    void disconnect() override {}
    bool isConnected() const override { return true; }
    ConnectionMode getMode() const override { return ConnectionMode::ListenServer; }
    void send(uint8_t, const void*, size_t) override {}
    void sendTo(NetConnection* conn, uint8_t channel, const void* data, size_t size) override {
        if (conn) conn->send(channel, data, size);
    }
    void broadcast(uint8_t, const void*, size_t) override { broadcastCount.fetch_add(1); }
    void broadcastExcept(NetConnection*, uint8_t, const void*, size_t) override {}
    void onMessage(uint8_t, MessageHandler) override {}
    void onConnectionChange(ConnectionHandler) override {}
    void setAcceptCallback(AcceptCallback) override {}
    void kickConnection(NetConnection*, const char*) override {}
    const std::vector<NetConnection*>& getConnections() override { return _ptrs; }
    NetConnection* getConnection() const override { return nullptr; }
    uint32_t getHostId() const override { return 1; }
    void setExtension(INetworkExtension* ext) override { _extension = ext; }
    ReplicationManager* getReplicationManager() override { return nullptr; }
    RpcHandler* getRpcHandler() override { return nullptr; }
    // R5.3 (2026-08-24): Replay recorder wiring — stubs return nullptr.
    void setReplayRecorder(ayt::replay::IReplayRecorder* /*rec*/) override {}
    ayt::replay::IReplayRecorder* getReplayRecorder() const override { return nullptr; }

    std::atomic<int> broadcastCount{0};
    INetworkExtension* _extension = nullptr;

private:
    std::vector<std::unique_ptr<MockNetConnection>> _owned;
    std::vector<NetConnection*> _ptrs;
};

class RecordingExtension : public INetworkExtension {
public:
    std::atomic<int> preReplicateCalls{0};
    std::atomic<int> postReplicateCalls{0};
    bool clearTargets = false;
    uint32_t denyConnId = 0;

    bool isRelevant(NetConnection* viewer, void*, const ayt::reflect::ITypeInfo*, uint32_t) override {
        if (!viewer || denyConnId == 0) return true;
        return viewer->getId() != denyConnId;
    }

    void onPreReplicate(void*, const ayt::reflect::ITypeInfo*, uint32_t,
                        std::vector<NetConnection*>& targets) override {
        preReplicateCalls.fetch_add(1);
        if (clearTargets) targets.clear();
    }

    void onPostReplicate(void*, const ayt::reflect::ITypeInfo*, uint32_t) override {
        postReplicateCalls.fetch_add(1);
    }
};

} // anonymous namespace

TEST_SUITE(InterestManagement)

TEST_CASE(DisabledRadiusTargetsEveryPeer) {
    ayt::test::setCurrentCase("DisabledRadiusTargetsEveryPeer");

    InterestTestNetwork net;
    MockNetConnection* first = net.addConnection(1, nullptr);
    MockNetConnection* second = net.addConnection(2, nullptr);

    ReplicationManager mgr(&net);
    RecordingExtension ext;
    mgr.setExtension(&ext);

    InterestReplObj obj;
    obj.score = 7;
    auto* type = ayt::reflect::TypeRegistryImpl::instance().findType("InterestReplObj");
    CHECK(type != nullptr);
    mgr.registerObject(&obj, type, 42);
    const int preAfterSpawn = ext.preReplicateCalls.load();
    mgr.tick(0.016f);

    CHECK_INT_EQ(first->sendCount.load(), 2);
    CHECK_INT_EQ(second->sendCount.load(), 2);
    CHECK_INT_EQ(first->countMessage(kMsgTypeEntitySpawn), 1u);
    CHECK_INT_EQ(first->countMessage(kMsgTypeReplication), 1u);
    CHECK_INT_EQ(net.broadcastCount.load(), 0);
    CHECK_INT_EQ(ext.preReplicateCalls.load(), preAfterSpawn + 1);
}

TEST_CASE(DistanceCullExcludesFarViewer) {
    ayt::test::setCurrentCase("DistanceCullExcludesFarViewer");

    InterestTestNetwork net;
    NetVec3 nearPos{0.f, 0.f, 0.f};
    NetVec3 farPos{100.f, 0.f, 0.f};
    MockNetConnection* nearConn = net.addConnection(1, &nearPos);
    MockNetConnection* farConn = net.addConnection(2, &farPos);

    ReplicationManager mgr(&net);
    mgr.setInterestRadius(50.f);

    InterestReplObj obj;
    obj.score = 11;
    auto* type = ayt::reflect::TypeRegistryImpl::instance().findType("InterestReplObj");
    mgr.registerObject(&obj, type, 99);
    mgr.setObjectLocation(99, NetVec3{10.f, 0.f, 0.f});
    nearConn->sendCount.store(0);
    farConn->sendCount.store(0);
    mgr.tick(0.016f);

    CHECK_INT_EQ(nearConn->sendCount.load(), 2);
    CHECK_INT_EQ(farConn->sendCount.load(), 0);
    CHECK_INT_EQ(net.broadcastCount.load(), 0);
}

TEST_CASE(OnPreReplicateCanSuppressDelivery) {
    ayt::test::setCurrentCase("OnPreReplicateCanSuppressDelivery");

    InterestTestNetwork net;
    net.addConnection(1, nullptr);

    ReplicationManager mgr(&net);
    RecordingExtension ext;
    ext.clearTargets = true;
    mgr.setExtension(&ext);

    InterestReplObj obj;
    auto* type = ayt::reflect::TypeRegistryImpl::instance().findType("InterestReplObj");
    mgr.registerObject(&obj, type, 7);
    const int preAfterSpawn = ext.preReplicateCalls.load();
    net.broadcastCount.store(0);
    mgr.tick(0.016f);

    CHECK_INT_EQ(ext.preReplicateCalls.load(), preAfterSpawn + 1);
    CHECK_INT_EQ(ext.postReplicateCalls.load(), 0);
    CHECK_INT_EQ(net.broadcastCount.load(), 0);
}

TEST_CASE(IsRelevantPredicateFiltersTargets) {
    ayt::test::setCurrentCase("IsRelevantPredicateFiltersTargets");

    InterestTestNetwork net;
    MockNetConnection* allowed = net.addConnection(1, nullptr);
    MockNetConnection* denied = net.addConnection(2, nullptr);

    ReplicationManager mgr(&net);
    RecordingExtension ext;
    ext.denyConnId = 2;
    mgr.setExtension(&ext);

    InterestReplObj obj;
    auto* type = ayt::reflect::TypeRegistryImpl::instance().findType("InterestReplObj");
    mgr.registerObject(&obj, type, 3);
    allowed->sendCount.store(0);
    denied->sendCount.store(0);
    mgr.tick(0.016f);

    CHECK_INT_EQ(allowed->sendCount.load(), 2);
    CHECK_INT_EQ(denied->sendCount.load(), 0);
}

TEST_CASE(InterestTransitionsSendSpawnFullDeltaAndDespawnPerPeer) {
    ayt::test::setCurrentCase("InterestTransitionsSendSpawnFullDeltaAndDespawnPerPeer");

    InterestTestNetwork net;
    NetVec3 firstViewer{0.f, 0.f, 0.f};
    NetVec3 secondViewer{100.f, 0.f, 0.f};
    MockNetConnection* first = net.addConnection(1, &firstViewer);
    MockNetConnection* second = net.addConnection(2, &secondViewer);

    ReplicationManager mgr(&net);
    mgr.setInterestRadius(25.f);
    InterestReplObj obj;
    obj.score = 10;
    auto* type = ayt::reflect::TypeRegistryImpl::instance().findType("InterestReplObj");
    CHECK(type != nullptr);
    mgr.registerObject(&obj, type, 123);
    mgr.setObjectLocation(123, NetVec3{0.f, 0.f, 0.f});

    mgr.tick(0.016f);
    CHECK_INT_EQ(first->countMessage(kMsgTypeEntitySpawn), 1u);
    CHECK_INT_EQ(first->countMessage(kMsgTypeReplication), 1u);
    CHECK_INT_EQ(second->sendCount.load(), 0);

    obj.score = 20;
    mgr.tick(0.016f);
    CHECK_INT_EQ(first->countMessage(kMsgTypeDelta), 1u);

    mgr.setObjectLocation(123, NetVec3{100.f, 0.f, 0.f});
    mgr.tick(0.016f);
    CHECK_INT_EQ(first->countMessage(kMsgTypeEntityDespawn), 1u);
    CHECK_INT_EQ(second->countMessage(kMsgTypeEntitySpawn), 1u);
    CHECK_INT_EQ(second->countMessage(kMsgTypeReplication), 1u);
    CHECK_INT_EQ(second->countMessage(kMsgTypeDelta), 0u);
}

TEST_CASE(ObjectPolicyOverridesAoiAndDistanceLod) {
    ayt::test::setCurrentCase("ObjectPolicyOverridesAoiAndDistanceLod");
    InterestTestNetwork net;
    NetVec3 viewer{0.f, 0.f, 0.f};
    MockNetConnection* connection = net.addConnection(1, &viewer);
    ReplicationManager manager(&net);
    manager.setInterestRadius(10.f);

    InterestReplObj object;
    auto* type = ayt::reflect::TypeRegistryImpl::instance().findType(
        "InterestReplObj");
    manager.registerObject(&object, type, 200);
    manager.setObjectLocation(200, {100.f, 0.f, 0.f});

    ReplicationObjectPolicy policy;
    policy.interestRadius = 150.f;
    policy.mediumDistance = 25.f;
    policy.farDistance = 50.f;
    policy.farIntervalTicks = 3;
    CHECK(manager.setObjectReplicationPolicy(200, policy));

    manager.tick(0.016f);
    CHECK_INT_EQ(connection->countMessage(kMsgTypeEntitySpawn), 1u);
    CHECK_INT_EQ(connection->countMessage(kMsgTypeReplication), 1u);
    connection->clearMessages();

    object.score = 1;
    manager.tick(0.016f);
    manager.tick(0.016f);
    CHECK_INT_EQ(connection->countMessage(kMsgTypeDelta), 0u);
    CHECK(manager.getLastTickStats().deferredByLod != 0);
    manager.tick(0.016f);
    CHECK_INT_EQ(connection->countMessage(kMsgTypeDelta), 1u);
}

TEST_CASE(PriorityBudgetDefersLowerPriorityEntity) {
    ayt::test::setCurrentCase("PriorityBudgetDefersLowerPriorityEntity");
    InterestTestNetwork net;
    MockNetConnection* connection = net.addConnection(1, nullptr);
    ReplicationManager manager(&net);
    InterestReplObj low;
    InterestReplObj high;
    auto* type = ayt::reflect::TypeRegistryImpl::instance().findType(
        "InterestReplObj");
    manager.registerObject(&low, type, 10);
    manager.registerObject(&high, type, 20);
    ReplicationObjectPolicy lowPolicy;
    lowPolicy.priority = 0.1f;
    ReplicationObjectPolicy highPolicy;
    highPolicy.priority = 10.f;
    CHECK(manager.setObjectReplicationPolicy(10, lowPolicy));
    CHECK(manager.setObjectReplicationPolicy(20, highPolicy));
    manager.tick(0.016f);
    connection->clearMessages();

    ReplicationScalabilityConfig config;
    config.maxBytesPerConnectionPerTick = 64;
    manager.setScalabilityConfig(config);
    low.score = 1;
    high.score = 2;
    manager.tick(0.016f);

    CHECK_INT_EQ(connection->countMessage(kMsgTypeDelta), 1u);
    uint32_t replicatedNetId = 0;
    for (DecodedPacket& packet : connection->packets) {
        if (packet.header.msgType != kMsgTypeDelta) continue;
        BitStream body(packet.body.data(), packet.body.size());
        uint32_t serverTick = 0;
        ReflectSerializer::FrameHeader header;
        CHECK(ReflectSerializer::readServerTick(body, serverTick));
        CHECK(ReflectSerializer::readReplicationFrameHeader(body, header));
        replicatedNetId = header.netId;
    }
    CHECK_INT_EQ(replicatedNetId, 20u);
    CHECK_INT_EQ(manager.getLastTickStats().deferredByBudget, 1u);
}

TEST_CASE(BackpressurePausesDeltaAndRecoversWithoutLosingDirtyState) {
    ayt::test::setCurrentCase(
        "BackpressurePausesDeltaAndRecoversWithoutLosingDirtyState");
    InterestTestNetwork net;
    MockNetConnection* connection = net.addConnection(1, nullptr);
    ReplicationManager manager(&net);
    InterestReplObj object;
    auto* type = ayt::reflect::TypeRegistryImpl::instance().findType(
        "InterestReplObj");
    manager.registerObject(&object, type, 30);
    manager.tick(0.016f);
    connection->clearMessages();

    uint32_t queuedBytes = 4096;
    manager.setSendQueueBytesProvider(
        [&queuedBytes](NetConnection*) { return queuedBytes; });
    ReplicationScalabilityConfig config;
    config.softSendQueueBytes = 1024;
    config.hardSendQueueBytes = 2048;
    manager.setScalabilityConfig(config);
    object.score = 9;
    manager.tick(0.016f);
    CHECK_INT_EQ(connection->countMessage(kMsgTypeDelta), 0u);
    CHECK_INT_EQ(manager.getLastTickStats().hardPressureConnections, 1u);
    CHECK(manager.getLastTickStats().deferredByBackpressure != 0);

    queuedBytes = 0;
    manager.tick(0.016f);
    CHECK_INT_EQ(connection->countMessage(kMsgTypeDelta), 1u);
}

TEST_CASE(LifecycleBatchCombinesSpawnAndDespawnAnnouncements) {
    ayt::test::setCurrentCase(
        "LifecycleBatchCombinesSpawnAndDespawnAnnouncements");
    InterestTestNetwork net;
    NetVec3 viewer{0.f, 0.f, 0.f};
    MockNetConnection* connection = net.addConnection(1, &viewer);
    ReplicationManager authority(&net);
    authority.setInterestRadius(20.f);
    ReplicationScalabilityConfig config;
    config.enableLifecycleBatching = true;
    config.maxLifecycleEventsPerPacket = 64;
    authority.setScalabilityConfig(config);

    InterestReplObj objects[3];
    auto* type = ayt::reflect::TypeRegistryImpl::instance().findType(
        "InterestReplObj");
    for (uint32_t i = 0; i < 3; ++i) {
        authority.registerObject(&objects[i], type, 100 + i);
        authority.setObjectLocation(100 + i, {0.f, 0.f, 0.f});
    }
    authority.tick(0.016f);
    CHECK_INT_EQ(connection->countMessage(kMsgTypeEntityLifecycleBatch), 1u);
    CHECK_INT_EQ(connection->countMessage(kMsgTypeEntitySpawn), 0u);
    CHECK_INT_EQ(connection->countMessage(kMsgTypeReplication), 0u);
    CHECK_INT_EQ(authority.getLastTickStats().lifecycleEvents, 3u);

    ReplicationManager client(nullptr);
    for (DecodedPacket& packet : connection->packets) {
        if (packet.header.msgType != kMsgTypeEntityLifecycleBatch) continue;
        BitStream body(packet.body.data(), packet.body.size());
        CHECK(client.onReceive(packet.header.msgType, body, nullptr));
    }
    CHECK_INT_EQ(client.spawnAnnouncementCount(), 3u);

    connection->clearMessages();
    authority.tick(0.016f);
    CHECK_INT_EQ(connection->countMessage(kMsgTypeReplication), 3u);
    viewer = {100.f, 0.f, 0.f};
    connection->clearMessages();
    authority.tick(0.016f);
    CHECK_INT_EQ(connection->countMessage(kMsgTypeEntityLifecycleBatch), 1u);
    for (DecodedPacket& packet : connection->packets) {
        if (packet.header.msgType != kMsgTypeEntityLifecycleBatch) continue;
        BitStream body(packet.body.data(), packet.body.size());
        CHECK(client.onReceive(packet.header.msgType, body, nullptr));
    }
    CHECK_INT_EQ(client.spawnAnnouncementCount(), 0u);
}

TEST_CASE(SingleDespawnClearsPendingSpawnAnnouncement) {
    ayt::test::setCurrentCase(
        "SingleDespawnClearsPendingSpawnAnnouncement");
    ReplicationManager client(nullptr);

    BitStream spawn;
    ReflectSerializer::writeEntitySpawn(spawn, 700, 0x1234u);
    spawn.resetForRead();
    CHECK(client.onReceive(kMsgTypeEntitySpawn, spawn, nullptr));
    CHECK_INT_EQ(client.spawnAnnouncementCount(), 1u);

    BitStream despawn;
    ReflectSerializer::writeEntityDespawn(despawn, 700);
    despawn.resetForRead();
    CHECK(client.onReceive(kMsgTypeEntityDespawn, despawn, nullptr));
    CHECK_INT_EQ(client.spawnAnnouncementCount(), 0u);
}

TEST_SUITE_END
