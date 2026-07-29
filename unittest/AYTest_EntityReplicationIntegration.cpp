// AYTest_EntityReplicationIntegration.cpp - R4.1-A AYEntity bridge over Subsystem
//
// Minimal demo path: Entity + HealthComponent (NetReplicate currentHp) registered
// via EntityReplicationAdapter, replicated server→client over real GNS.

#include <AYNetwork.h>
#include <AYTest.h>
#include <AYEntity.h>
#include <AYEntityModule.h>

#include <Replication/EntityReplicationAdapter.h>
#include <components/AYHealthComponent.h>
#include <components/AYNetworkComponent.h>

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

    uint16_t typeHash = 0;
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

TEST_SUITE_END
