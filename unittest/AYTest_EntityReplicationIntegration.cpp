// AYTest_EntityReplicationIntegration.cpp - R4.1-A AYEntity bridge over Subsystem
//
// Minimal demo path: Entity + HealthComponent (NetReplicate currentHp) registered
// via EntityReplicationAdapter, replicated server→client over real GNS.

#include <AYNetwork.h>
#include <AYTest.h>
#include <AYEntity.h>
#include <AYEntity/EntityModule.h>
#include <AYEntity/ComponentRegistration.h>

#include <AYNetwork/Replication/EntityReplicationAdapter.h>
#include <AYNetwork/Replication/EntityReplicationWorldBinder.h>
#include <AYEntity/components/HealthComponent.h>
#include <AYEntity/components/NetworkComponent.h>
#include <AYReflect/detail/ReflectImpl.h>

#include <chrono>
#include <functional>
#include <thread>
#include <vector>

using namespace ayt::net;
using ayt::entity::Entity;
using ayt::entity::HealthComponent;
using ayt::entity::NetworkComponent;
using ayt::entity::World;

namespace
{

struct SecondaryReplicatedComponent : ayt::entity::IComponent {
    int32_t value = 0;
    const char* getName() const override { return "SecondaryReplicatedComponent"; }
};

struct MultiReplicatedComponent : ayt::entity::IComponent {
    int32_t value = 0;
    const char* getName() const override { return "MultiReplicatedComponent"; }
};

struct SecondaryReplicatedRegistrar {
    SecondaryReplicatedRegistrar() {
        auto& registry = ayt::reflect::TypeRegistryImpl::instance();
        if (registry.findType("SecondaryReplicatedComponent")) return;
        using Info = ayt::reflect::TypeInfoImpl<SecondaryReplicatedComponent>;
        auto* info = new Info(
            "SecondaryReplicatedComponent",
            ayt::reflect::detail::defaultCreate<SecondaryReplicatedComponent>,
            ayt::reflect::detail::defaultDestroy<SecondaryReplicatedComponent>,
            ayt::reflect::detail::defaultCopy<SecondaryReplicatedComponent>);
        info->addField(new ayt::reflect::FieldInfoImpl(
            "value", registry.findType<int32_t>(),
            offsetof(SecondaryReplicatedComponent, value),
            ayt::reflect::FieldAttribute::Serialize |
                ayt::reflect::FieldAttribute::NetReplicate));
        registry.registerTypeInfo("SecondaryReplicatedComponent", info);
        using MultiInfo = ayt::reflect::TypeInfoImpl<MultiReplicatedComponent>;
        auto* multiInfo = new MultiInfo(
            "MultiReplicatedComponent",
            ayt::reflect::detail::defaultCreate<MultiReplicatedComponent>,
            ayt::reflect::detail::defaultDestroy<MultiReplicatedComponent>,
            ayt::reflect::detail::defaultCopy<MultiReplicatedComponent>);
        multiInfo->addField(new ayt::reflect::FieldInfoImpl(
            "value", registry.findType<int32_t>(),
            offsetof(MultiReplicatedComponent, value),
            ayt::reflect::FieldAttribute::Serialize |
                ayt::reflect::FieldAttribute::NetReplicate));
        registry.registerTypeInfo("MultiReplicatedComponent", multiInfo);
    }
};

SecondaryReplicatedRegistrar g_secondaryReplicatedRegistrar;

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

} // anonymous namespace

TEST_SUITE(EntityReplicationIntegration)

TEST_CASE(WorldBinderUsesInstanceIdsForReplicatedMultiComponents) {
    World::instance().initialize();
    ayt::entity::registerEntityComponents();
    CHECK(ayt::entity::registerComponent<MultiReplicatedComponent>(
        ayt::entity::ComponentRegistry::instance(),
        "MultiReplicatedComponent", "Multi Replicated", "Tests",
        ayt::entity::ComponentMultiplicity::Multiple).succeeded());
    ReplicationManager manager(nullptr);
    Entity* entity = Entity::create();
    constexpr uint32_t kEntityNetId = 9182;
    entity->addComponent<NetworkComponent>()->setNetId(kEntityNetId);
    auto* first = entity->createComponent<MultiReplicatedComponent>();
    auto* second = entity->createComponent<MultiReplicatedComponent>();
    CHECK_NOT_NULL(first);
    CHECK_NOT_NULL(second);
    const std::string firstId = entity->componentInstance(first)->id;
    const std::string secondId = entity->componentInstance(second)->id;
    EntityReplicationWorldBinder binder(manager, World::instance());
    CHECK_INT_EQ(binder.synchronize(), 2u);
    auto* type = ayt::reflect::TypeRegistryImpl::instance()
        .findType<MultiReplicatedComponent>();
    CHECK_NOT_NULL(type);
    const uint64_t schema = ReflectSerializer::hashTypeSchema(type);
    const uint32_t firstNetId = EntityReplicationWorldBinder::makeComponentNetId(
        kEntityNetId, schema, firstId);
    const uint32_t secondNetId = EntityReplicationWorldBinder::makeComponentNetId(
        kEntityNetId, schema, secondId);
    CHECK(firstNetId != secondNetId);
    CHECK(manager.findObject(firstNetId) == first);
    CHECK(manager.findObject(secondNetId) == second);
    CHECK(entity->removeComponentById(firstId));
    CHECK_INT_EQ(binder.synchronize(), 1u);
    CHECK(manager.findObject(firstNetId) == nullptr);
    CHECK(manager.findObject(secondNetId) == second);
    binder.clear();
    Entity::destroy(entity);
    World::instance().shutdown();
}

TEST_CASE(HealthComponentReplicatesViaAdapter) {
    ayt::test::setCurrentCase("HealthComponentReplicatesViaAdapter");

    World::instance().initialize();
    ayt::entity::registerEntityComponents();

    INetworkSubSystem* server = createNetworkSubSystemForTest();
    INetworkSubSystem* client = createNetworkSubSystemForTest();
    CHECK(server != nullptr);
    CHECK(client != nullptr);
    CHECK(server->initialize());
    CHECK(client->initialize());

    constexpr uint16_t kPort = 27560;
    constexpr uint32_t kNetId = 601;

    server->listen(kPort);
    client->connect("127.0.0.1", kPort);

    std::vector<INetworkSubSystem*> all{server, client};
    CHECK(pumpUntil(std::chrono::seconds(8), [&]() {
        pumpAll(all);
        return !server->getConnections().empty() && client->isConnected();
    }));

    Entity* serverEntity = Entity::create();
    auto* serverNet = serverEntity->addComponent<NetworkComponent>();
    serverNet->setNetId(kNetId);
    serverEntity->addComponent<HealthComponent>();
    HealthComponent* serverHealth = serverEntity->getComponent<HealthComponent>();
    CHECK(serverHealth != nullptr);
    serverHealth->setHp(50);

    CHECK(EntityReplicationAdapter::registerEntityComponent<HealthComponent>(
        *server->getReplicationManager(), serverEntity, kNetId));

    uint64_t typeHash = 0;
    CHECK(pumpUntil(std::chrono::seconds(5), [&]() {
        pumpAll(all);
        return client->getReplicationManager()->peekSpawnAnnouncement(kNetId, typeHash);
    }));

    Entity* clientEntity = Entity::create();
    clientEntity->addComponent<NetworkComponent>()->setNetId(kNetId);
    clientEntity->addComponent<HealthComponent>();
    HealthComponent* clientHealth = clientEntity->getComponent<HealthComponent>();
    CHECK(clientHealth != nullptr);

    CHECK(EntityReplicationAdapter::registerEntityComponent<HealthComponent>(
        *client->getReplicationManager(), clientEntity, kNetId));

    serverHealth->setHp(25);
    CHECK(pumpUntil(std::chrono::seconds(5), [&]() {
        pumpAll(all);
        return clientHealth->getHp() == 25;
    }));
    CHECK_INT_EQ(clientHealth->getHp(), 25);

    EntityReplicationAdapter::unregisterEntityComponent(*server->getReplicationManager(), kNetId);
    EntityReplicationAdapter::unregisterEntityComponent(*client->getReplicationManager(), kNetId);

    Entity::destroy(serverEntity);
    Entity::destroy(clientEntity);

    server->disconnect();
    client->disconnect();
    server->shutdown();
    client->shutdown();
    delete server;
    delete client;

    World::instance().shutdown();
}

TEST_CASE(WorldBinderDiscoversMultipleComponentsAndRemovals) {
    ayt::test::setCurrentCase("WorldBinderDiscoversMultipleComponentsAndRemovals");

    World::instance().initialize();
    ayt::entity::registerEntityComponents();
    const auto secondaryRegistration =
        ayt::entity::ComponentRegistry::instance()
            .registerType<SecondaryReplicatedComponent>(
                "SecondaryReplicatedComponent");
    CHECK(secondaryRegistration.succeeded());

    ReplicationManager manager(nullptr);
    Entity* entity = Entity::create();
    constexpr uint32_t kEntityNetId = 8123;
    entity->addComponent<NetworkComponent>()->setNetId(kEntityNetId);
    auto* health = entity->addComponent<HealthComponent>();
    auto* secondary = entity->addComponent<SecondaryReplicatedComponent>();
    CHECK(health != nullptr);
    CHECK(secondary != nullptr);

    EntityReplicationWorldBinder binder(manager, World::instance());
    CHECK_INT_EQ(binder.synchronize(), 2u);
    CHECK_INT_EQ(manager.getRegisteredCount(), 2u);

    auto* healthType = ayt::reflect::TypeRegistryImpl::instance().findType<HealthComponent>();
    auto* secondaryType = ayt::reflect::TypeRegistryImpl::instance().findType<SecondaryReplicatedComponent>();
    CHECK(healthType != nullptr);
    CHECK(secondaryType != nullptr);
    const uint32_t healthNetId = EntityReplicationWorldBinder::makeComponentNetId(
        kEntityNetId, ReflectSerializer::hashTypeSchema(healthType));
    const uint32_t secondaryNetId = EntityReplicationWorldBinder::makeComponentNetId(
        kEntityNetId, ReflectSerializer::hashTypeSchema(secondaryType));
    CHECK(healthNetId != secondaryNetId);
    CHECK(manager.findObject(healthNetId) == health);
    CHECK(manager.findObject(secondaryNetId) == secondary);

    entity->removeComponent<SecondaryReplicatedComponent>();
    CHECK_INT_EQ(binder.synchronize(), 1u);
    CHECK(manager.findObject(secondaryNetId) == nullptr);

    Entity::destroy(entity);
    CHECK_INT_EQ(binder.synchronize(), 0u);
    CHECK_INT_EQ(manager.getRegisteredCount(), 0u);
    CHECK_INT_EQ(binder.collisionCount(), 0u);

    World::instance().shutdown();
}

TEST_SUITE_END
