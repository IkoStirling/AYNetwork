#include <AYNetwork/Session/DedicatedServerRuntime.h>
#include <AYNetwork/Session/InMemoryOnlineServices.h>
#include <AYTest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <set>
#include <thread>

using namespace ayt::net;

namespace
{

class TestDedicatedWorldHost final : public IDedicatedWorldHost {
public:
    bool startAuthoritativeWorld(
        const DedicatedAllocation& allocation) override {
        started.insert(allocation.allocationId);
        return acceptWorld;
    }
    void stopAuthoritativeWorld(DedicatedAllocationId id) override {
        stopped.insert(id);
    }
    void playerConnected(DedicatedAllocationId id, const PeerId& peer,
                         NetConnection*) override {
        connected.emplace(id, peer.value);
    }
    void playerDisconnected(DedicatedAllocationId id,
                            const PeerId& peer) override {
        disconnected.emplace(id, peer.value);
    }
    void tickAuthoritativeWorlds(float) override { ++ticks; }

    bool acceptWorld = true;
    uint32_t ticks = 0;
    std::set<DedicatedAllocationId> started;
    std::set<DedicatedAllocationId> stopped;
    std::set<std::pair<DedicatedAllocationId, std::string>> connected;
    std::set<std::pair<DedicatedAllocationId, std::string>> disconnected;
};

template <typename Predicate>
bool waitFor(DedicatedServerRuntime& runtime,
             NetworkDedicatedSessionConnector* connector,
             Predicate&& predicate, uint32_t timeoutMs = 3000) {
    const auto deadline = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        runtime.update(0.001f);
        if (connector) connector->update();
        if (predicate()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return predicate();
}

} // namespace

TEST_SUITE(DedicatedServerRuntime)

TEST_CASE(DedicatedAdmissionCodecIsBoundedAndCanonical) {
    ayt::test::setCurrentCase("DedicatedAdmissionCodecIsBoundedAndCanonical");
    DedicatedAdmission source;
    source.allocationId = 91;
    source.peerId = PeerId{"player-one"};
    source.reservationToken = std::string(64, 'a');
    std::vector<uint8_t> bytes;
    CHECK(encodeDedicatedAdmission(source, bytes));
    DedicatedAdmission decoded;
    CHECK(decodeDedicatedAdmission(bytes.data(), bytes.size(), decoded));
    CHECK_INT_EQ(decoded.allocationId, source.allocationId);
    CHECK(decoded.peerId == source.peerId);
    CHECK(decoded.reservationToken == source.reservationToken);
    bytes.push_back(0);
    CHECK(!decodeDedicatedAdmission(bytes.data(), bytes.size(), decoded));
}

TEST_CASE(HeadlessRuntimeLoadsAdmissionAndDrainsAllocation) {
    ayt::test::setCurrentCase("HeadlessRuntimeLoadsAdmissionAndDrainsAllocation");
    auto service = std::make_shared<InMemoryOnlineServices>(
        InMemoryOnlineServicesConfig{});
    auto worlds = std::make_shared<TestDedicatedWorldHost>();
    std::unique_ptr<INetworkSubSystem> server(createNetworkSubSystemForTest());
    std::unique_ptr<INetworkSubSystem> client(createNetworkSubSystemForTest());
    CHECK(server && client);
    CHECK(server->initialize());
    CHECK(client->initialize());

    DedicatedServerRuntimeConfig config;
    config.registration = {
        "dedicated-runtime-test", "test", "build-1", "127.0.0.1",
        7976, 8};
    config.heartbeatIntervalMs = 20;
    config.allocationPollIntervalMs = 5;
    config.retryIntervalMs = 5;
    config.drainTimeoutMs = 1000;
    DedicatedServerRuntime runtime(*server, service, worlds, config);
    CHECK(runtime.start());

    DedicatedAllocationRequest request;
    request.region = "test";
    request.buildId = "build-1";
    request.playerCount = 1;
    request.players = {PeerId{"dedicated-client"}};
    request.matchId = 41;
    request.content = {"maps/runtime-test", "1", 99};
    const auto allocated = service->allocateServer(request);
    CHECK(allocated);
    CHECK_INT_EQ(allocated.value.matchId, request.matchId);
    CHECK(allocated.value.content == request.content);
    CHECK(waitFor(runtime, nullptr, [&] {
        return worlds->started.contains(allocated.value.allocationId);
    }));

    NetworkDedicatedSessionConnectorConfig connectorConfig;
    connectorConfig.localPeerId = PeerId{"dedicated-client"};
    connectorConfig.connectTimeoutMs = 1500;
    NetworkDedicatedSessionConnector connector(*client, connectorConfig);
    DedicatedAllocation clientAllocation = allocated.value;
    CHECK(deriveDedicatedAdmissionToken(
        allocated.value.reservationToken, connectorConfig.localPeerId,
        clientAllocation.reservationToken));
    CHECK(clientAllocation.reservationToken !=
          allocated.value.reservationToken);
    CHECK(connector.connect(clientAllocation));
    CHECK(waitFor(runtime, &connector, [&] {
        return connector.getStatus().state ==
                   DedicatedSessionConnectionState::Active &&
               worlds->connected.contains(
                   {allocated.value.allocationId, "dedicated-client"});
    }));

    connector.disconnect();
    CHECK(waitFor(runtime, nullptr, [&] {
        return worlds->stopped.contains(allocated.value.allocationId);
    }));
    CHECK(!service->listServerAllocations(
        DedicatedServerCredential{runtime.getStatus().server.serverId,
                                  std::string(64, 'x')}));

    runtime.beginDrain();
    CHECK(waitFor(runtime, nullptr, [&] {
        return runtime.getStatus().state == DedicatedServerRuntimeState::Stopped;
    }));
    client->shutdown();
    server->shutdown();
}

} // TEST_SUITE(DedicatedServerRuntime)
