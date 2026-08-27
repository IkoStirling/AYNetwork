// AYTest_P2P.cpp - backend-neutral P2P config and UDP signaling coverage.

#include <AYNetwork.h>
#include <AYNetwork/Signaling/UdpSignaling.h>
#include <AYNetwork/Transport/GnsConnection.h>
#include <AYNetwork/Transport/UdpSocket.h>
#include <AYTest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

using namespace ayt::net;

namespace
{

class RecordingSignaling final : public ISignalingTransport {
public:
    bool start(const PeerId& localPeer) override {
        if (!localPeer.isValid()) return false;
        local = localPeer;
        running.store(true);
        return true;
    }
    void stop() override { running.store(false); }
    bool isRunning() const override { return running.load(); }
    bool sendSignal(const PeerId& destination, const void*, size_t size) override {
        if (!running.load() || !destination.isValid() || size == 0) return false;
        remote = destination;
        ++sendCount;
        return true;
    }
    size_t poll(const ReceiveHandler&, size_t) override { return 0; }

    PeerId local;
    PeerId remote;
    std::atomic<size_t> sendCount{0};
    std::atomic<bool> running{false};
};

} // namespace

TEST_SUITE(P2P)

TEST_CASE(PeerIdentityAndConfigValidation) {
    ayt::test::setCurrentCase("PeerIdentityAndConfigValidation");
    CHECK(PeerId{"player-01:session_a"}.isValid());
    CHECK(!PeerId{""}.isValid());
    CHECK(!PeerId{"contains space"}.isValid());
    CHECK(!PeerId{std::string(64, 'x')}.isValid());

    P2PConfig config;
    config.localPeerId = PeerId{"player-a"};
    config.icePolicy = P2PIcePolicy::DirectOnly;
    CHECK(config.isValid());
    config.icePolicy = P2PIcePolicy::RelayOnly;
    CHECK(!config.isValid());
    config.turnServers = {"turn:relay.example.test:3478"};
    config.turnUsers = {"user"};
    config.turnPasswords = {"pass"};
    CHECK(config.isValid());
    config.turnPasswords.clear();
    CHECK(!config.isValid());
    config.turnPasswords = {"pass"};
    config.turnServers = {"turn:a.test:3478,turn:b.test:3478"};
    CHECK(!config.isValid());
    config.turnServers = {"turn:a.test:3478"};
    config.turnUsers = {""};
    CHECK(!config.isValid());
}

TEST_CASE(StunHostnamesArePreparedBeforeEnteringGns) {
    ayt::test::setCurrentCase("StunHostnamesArePreparedBeforeEnteringGns");
    P2PConfig config;
    config.localPeerId = PeerId{"resolver-test"};
    config.icePolicy = P2PIcePolicy::DirectOnly;
    config.allowPrivateCandidates = false;
    config.stunServers = {"localhost:3478", "127.0.0.1:3479", "stun:localhost:3480"};
    std::string error;
    CHECK(GnsConnection::prepareP2PConfig(config, &error));
    CHECK(error.empty());
    CHECK(config.stunServers[0] == "127.0.0.1:3478");
    CHECK(config.stunServers[1] == "127.0.0.1:3479");
    CHECK(config.stunServers[2] == "stun:127.0.0.1:3480");

    config.stunServers = {"host:invalid-port"};
    CHECK(!GnsConnection::prepareP2PConfig(config, &error));
    CHECK(!error.empty());
}

TEST_CASE(UdpClientRejectsPacketFromUnexpectedEndpoint) {
    ayt::test::setCurrentCase("UdpClientRejectsPacketFromUnexpectedEndpoint");
    UdpSocket expectedServer;
    CHECK(expectedServer.create());
    CHECK(expectedServer.bind("127.0.0.1", 0));
    expectedServer.setNonBlocking(true);

    UdpSignalingClientConfig config;
    config.serverAddress = "127.0.0.1";
    config.serverPort = expectedServer.getBoundPort();
    config.heartbeatIntervalMs = 0;
    UdpSignalingClient client(config);
    CHECK(client.start(PeerId{"victim"}));

    uint8_t registration[256]{};
    char clientAddress[64]{};
    uint16_t clientPort = 0;
    int registrationBytes = -1;
    const auto registrationDeadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (registrationBytes <= 0 &&
           std::chrono::steady_clock::now() < registrationDeadline) {
        registrationBytes = expectedServer.receiveFrom(
            clientAddress, &clientPort, registration, sizeof(registration));
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    CHECK(registrationBytes > 0);
    CHECK(clientPort != 0);

    // AYSG v1 Signal: header + "attacker" + "victim" + one opaque byte.
    std::vector<uint8_t> forged(16 + 8 + 6 + 1, 0);
    forged[0] = 'A'; forged[1] = 'Y'; forged[2] = 'S'; forged[3] = 'G';
    forged[4] = 1;
    forged[5] = 2;
    forged[6] = 8;
    forged[7] = 6;
    forged[8] = 1;
    const char from[] = "attacker";
    const char to[] = "victim";
    std::copy(from, from + 8, forged.begin() + 16);
    std::copy(to, to + 6, forged.begin() + 24);
    forged.back() = 0x42;

    UdpSocket attacker;
    CHECK(attacker.create());
    CHECK(attacker.bind("127.0.0.1", 0));
    CHECK(attacker.sendTo(clientAddress, clientPort, forged.data(), forged.size()) > 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    size_t delivered = client.poll([](const PeerId&, const void*, size_t) {});
    CHECK_INT_EQ(delivered, 0);

    CHECK(expectedServer.sendTo(
        clientAddress, clientPort, forged.data(), forged.size()) > 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    delivered = client.poll([](const PeerId&, const void*, size_t) {});
    CHECK_INT_EQ(delivered, 1);

    client.stop();
    attacker.close();
    expectedServer.close();
}

TEST_CASE(UdpRendezvousForwardsOpaqueSignal) {
    ayt::test::setCurrentCase("UdpRendezvousForwardsOpaqueSignal");
    UdpSignalingServerConfig serverConfig;
    serverConfig.bindAddress = "127.0.0.1";
    UdpSignalingServer server(serverConfig);
    CHECK(server.start());
    CHECK(server.getBoundPort() != 0);

    UdpSignalingClientConfig clientConfig;
    clientConfig.serverAddress = "127.0.0.1";
    clientConfig.serverPort = server.getBoundPort();
    clientConfig.heartbeatIntervalMs = 0;
    UdpSignalingClient alice(clientConfig);
    UdpSignalingClient bob(clientConfig);
    CHECK(alice.start(PeerId{"alice"}));
    CHECK(bob.start(PeerId{"bob"}));

    const auto registrationDeadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (server.getPeerCount() < 2 &&
           std::chrono::steady_clock::now() < registrationDeadline) {
        (void)server.pump();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    CHECK_INT_EQ(server.getPeerCount(), 2);

    const std::vector<uint8_t> signal{0x01, 0x7f, 0x00, 0xff, 0x42};
    CHECK(alice.sendSignal(PeerId{"bob"}, signal.data(), signal.size()));
    bool received = false;
    PeerId sender;
    std::vector<uint8_t> body;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!received && std::chrono::steady_clock::now() < deadline) {
        (void)server.pump();
        (void)bob.poll([&](const PeerId& from, const void* data, size_t size) {
            sender = from;
            const auto* bytes = static_cast<const uint8_t*>(data);
            body.assign(bytes, bytes + size);
            received = true;
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    CHECK(received);
    CHECK(sender == PeerId{"alice"});
    CHECK(body == signal);

    alice.stop();
    bob.stop();
    (void)server.pump();
    server.stop();
}

TEST_CASE(UdpRendezvousExpiresInactivePeer) {
    ayt::test::setCurrentCase("UdpRendezvousExpiresInactivePeer");
    UdpSignalingServerConfig serverConfig;
    serverConfig.bindAddress = "127.0.0.1";
    serverConfig.peerTimeoutMs = 20;
    UdpSignalingServer server(serverConfig);
    CHECK(server.start());
    UdpSignalingClientConfig clientConfig;
    clientConfig.serverPort = server.getBoundPort();
    clientConfig.heartbeatIntervalMs = 0;
    UdpSignalingClient client(clientConfig);
    CHECK(client.start(PeerId{"expires"}));
    const auto registrationDeadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (server.getPeerCount() == 0 &&
           std::chrono::steady_clock::now() < registrationDeadline) {
        (void)server.pump();
    }
    CHECK_INT_EQ(server.getPeerCount(), 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    server.pruneExpired();
    CHECK_INT_EQ(server.getPeerCount(), 0);
    client.stop();
    server.stop();
}

TEST_CASE(SubsystemConfiguresAndStartsP2PRoute) {
    ayt::test::setCurrentCase("SubsystemConfiguresAndStartsP2PRoute");
    std::unique_ptr<INetworkSubSystem> network(createNetworkSubSystemForTest());
    CHECK(network != nullptr);
    CHECK(network->initialize());

    auto signaling = std::make_shared<RecordingSignaling>();
    P2PConfig config;
    config.localPeerId = PeerId{"p2p-test-local"};
    config.virtualPort = 7;
    config.icePolicy = P2PIcePolicy::DirectOnly;
    config.allowPrivateCandidates = true;
    CHECK(network->configureP2P(config, signaling));
    CHECK(network->isP2PConfigured());
    CHECK(network->getLocalPeerId() == config.localPeerId);
    CHECK_INT_EQ(network->addP2PSessionEventListener({}), 0);
    const uint64_t firstListener = network->addP2PSessionEventListener(
        [](const P2PSessionEvent&) {});
    const uint64_t secondListener = network->addP2PSessionEventListener(
        [](const P2PSessionEvent&) {});
    CHECK(firstListener != 0);
    CHECK(secondListener != 0);
    CHECK(firstListener != secondListener);
    CHECK(network->removeP2PSessionEventListener(firstListener));
    CHECK(!network->removeP2PSessionEventListener(firstListener));
    CHECK(!network->removeP2PSessionEventListener(0));
    CHECK(network->removeP2PSessionEventListener(secondListener));
    {
        const P2PSessionInfo session = network->getP2PSessionInfo();
        CHECK(session.role == P2PSessionRole::None);
        CHECK(session.state == P2PSessionState::Idle);
        CHECK(session.localPeerId == config.localPeerId);
        CHECK_INT_EQ(session.virtualPort, config.virtualPort);
        CHECK_INT_EQ(session.readyPeerCount, 0);
    }
    CHECK(network->listenP2P());
    CHECK(network->isListening());
    {
        const P2PSessionInfo session = network->getP2PSessionInfo();
        CHECK(session.role == P2PSessionRole::Host);
        CHECK(session.state == P2PSessionState::Hosting);
        CHECK(session.hostPeerId == config.localPeerId);
        CHECK(session.sessionId != 0);
        CHECK_INT_EQ(session.epoch, 1);
        CHECK_INT_EQ(session.localSeatId, 1);
        CHECK(session.migration == P2PHostMigrationState::Disabled);
        CHECK(network->getP2PPeers().empty());
        const auto members = network->getP2PSessionMembers();
        CHECK_INT_EQ(members.size(), 1);
        CHECK(members.front().peerId == config.localPeerId);
        CHECK_INT_EQ(members.front().seatId, 1);
        CHECK(members.front().isHost);
        CHECK(members.front().connected);
        CHECK(!members.front().reserved);
        const auto barrier = network->getP2PReadyBarrierInfo();
        CHECK_INT_EQ(barrier.totalMemberCount, 1);
        CHECK_INT_EQ(barrier.readyMemberCount, 0);
        CHECK(!barrier.open);
    }
    CHECK(network->setP2PReconnectGracePeriodMs(30000));
    CHECK(!network->setP2PReconnectGracePeriodMs(600001));
    network->setP2PHostMigrationEnabled(true);
    CHECK(network->getP2PSessionInfo().migration ==
          P2PHostMigrationState::Stable);
    CHECK(!network->requestP2PHostMigration());
    CHECK(!network->reconnectP2P());
    network->setP2PHostMigrationEnabled(false);
    CHECK(network->getP2PSessionInfo().migration ==
          P2PHostMigrationState::Disabled);
    CHECK(network->setP2PLocalReady(true));
    CHECK(network->getP2PReadyBarrierInfo().open);
    network->disconnect();
    CHECK(!network->isListening());

    const PeerId remote{"p2p-test-remote"};
    const std::array<uint8_t, 4> ticket{0xde, 0xad, 0xbe, 0xef};
    CHECK(network->setP2PJoinTicket(ticket.data(), ticket.size()));
    std::vector<uint8_t> oversizedTicket(kP2PMaxJoinTicketBytes + 1, 0);
    CHECK(!network->setP2PJoinTicket(oversizedTicket.data(), oversizedTicket.size()));
    CHECK(network->connectP2P(remote));
    {
        const P2PSessionInfo session = network->getP2PSessionInfo();
        CHECK(session.role == P2PSessionRole::Client);
        CHECK(session.state == P2PSessionState::Connecting);
        CHECK(session.hostPeerId == remote);
        const auto peers = network->getP2PPeers();
        CHECK_INT_EQ(peers.size(), 1);
        CHECK(peers.front().peerId == remote);
        CHECK(peers.front().state == P2PPeerState::Connecting);
        CHECK(peers.front().isSessionHost);
        CHECK(!peers.front().admitted);
        CHECK(peers.front().connectionId != 0);
        CHECK(network->findP2PPeer(remote) == network->getConnection());
    }
    const auto signalDeadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (signaling->sendCount.load() == 0 &&
           std::chrono::steady_clock::now() < signalDeadline) {
        network->update(0.001f);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    CHECK(signaling->sendCount.load() >= 1);
    const P2PConnectionInfo info = network->getP2PConnectionInfo();
    CHECK(info.localPeerId == config.localPeerId);
    CHECK(info.remotePeerId == remote);
    CHECK(network->disconnectP2PPeer(remote, "session test complete"));
    CHECK(network->findP2PPeer(remote) == nullptr);
    CHECK(network->getP2PPeers().empty());
    CHECK(!network->disconnectP2PPeer(PeerId{"missing-peer"}));
    network->disconnect();
    network->shutdown();
}

TEST_CASE(RelayPolicyIsAcceptedByGnsConnectionFactory) {
    ayt::test::setCurrentCase("RelayPolicyIsAcceptedByGnsConnectionFactory");
    std::unique_ptr<INetworkSubSystem> network(createNetworkSubSystemForTest());
    CHECK(network && network->initialize());
    auto signaling = std::make_shared<RecordingSignaling>();
    P2PConfig config;
    config.localPeerId = PeerId{"p2p-relay-local"};
    config.virtualPort = 9;
    config.icePolicy = P2PIcePolicy::RelayOnly;
    config.turnServers = {"turn:127.0.0.1:3478"};
    config.turnUsers = {"short-lived-user"};
    config.turnPasswords = {"short-lived-password"};
    CHECK(config.isValid());
    CHECK(network->configureP2P(config, signaling));
    CHECK(network->connectP2P(PeerId{"p2p-relay-remote"}));
    CHECK(network->getP2PConnectionInfo().path == P2PPathKind::Unknown);
    network->disconnect();
    network->shutdown();
}

TEST_SUITE_END
