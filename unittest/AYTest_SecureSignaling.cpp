// AYTest_SecureSignaling.cpp - authenticated room signaling coverage.

#include <AYNetwork/Signaling/SecureUdpSignaling.h>
#include <AYNetwork/Transport/UdpSocket.h>
#include <AYTest.h>
#include <src/Signaling/SecureSignalingProtocol.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

using namespace ayt::net;

namespace
{

SignalingToken tokenFromSeed(uint8_t seed) {
    SignalingToken token;
    for (size_t i = 0; i < token.bytes.size(); ++i) {
        token.bytes[i] = static_cast<uint8_t>(seed + i);
    }
    return token;
}

struct CredentialBook {
    struct Entry {
        SignalingRoomId room;
        SecureSignalingCredential credential;
    };
    std::unordered_map<std::string, Entry> entries;

    SecureSignalingCredentialResolver resolver() {
        return [this](const PeerId& peer, const SignalingRoomId& room,
                      SecureSignalingCredential& result) {
            const auto it = entries.find(peer.value);
            if (it == entries.end() || it->second.room != room) return false;
            result = it->second.credential;
            return true;
        };
    }
};

SecureUdpSignalingClient makeClient(uint16_t port, const char* room,
                                    const SignalingToken& token) {
    SecureUdpSignalingClientConfig config;
    config.serverPort = port;
    config.roomId = SignalingRoomId{room};
    config.token = token;
    config.heartbeatIntervalMs = 0;
    config.registrationRefreshMs = 0;
    return SecureUdpSignalingClient(std::move(config));
}

template <typename Predicate, typename... Clients>
bool pumpUntil(SecureUdpSignalingServer& server, Predicate predicate,
               Clients&... clients) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!predicate() && std::chrono::steady_clock::now() < deadline) {
        (void)server.pump();
        const auto discard = [](const PeerId&, const void*, size_t) {};
        (static_cast<void>(clients.poll(discard)), ...);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return predicate();
}

} // namespace

TEST_SUITE(SecureSignaling)

TEST_CASE(TokenAndRoomValidation) {
    ayt::test::setCurrentCase("TokenAndRoomValidation");
    const auto token = tokenFromSeed(1);
    const std::string encoded = signalingTokenToHex(token);
    CHECK_INT_EQ(encoded.size(), 64);
    SignalingToken decoded;
    CHECK(parseSignalingTokenHex(encoded, decoded));
    CHECK(decoded == token);
    CHECK(!parseSignalingTokenHex("abcd", decoded));
    CHECK(SignalingRoomId{"room-01:match"}.isValid());
    CHECK(!SignalingRoomId{"bad room"}.isValid());
    CHECK(!SignalingToken{}.isValid());
}

TEST_CASE(ReplayWindowRejectsDuplicatesAndStalePackets) {
    ayt::test::setCurrentCase("ReplayWindowRejectsDuplicatesAndStalePackets");
    ayt::net::detail::SignalingReplayWindow window;
    CHECK(!window.accept(0));
    CHECK(window.accept(1));
    CHECK(!window.accept(1));
    CHECK(window.accept(3));
    CHECK(window.accept(2));
    CHECK(!window.accept(2));
    CHECK(window.accept(100));
    CHECK(!window.accept(3));
    window.reset();
    CHECK(window.accept(3));
}

TEST_CASE(PendingQueueIsBoundedBeforeRegistration) {
    ayt::test::setCurrentCase("PendingQueueIsBoundedBeforeRegistration");
    SecureUdpSignalingClientConfig config;
    config.serverPort = 29999;
    config.roomId = SignalingRoomId{"room-a"};
    config.token = tokenFromSeed(1);
    config.maxPendingSignals = 1;
    SecureUdpSignalingClient client(std::move(config));
    CHECK(client.start(PeerId{"alice"}));
    const uint8_t payload = 1;
    CHECK(client.sendSignal(PeerId{"bob"}, &payload, 1));
    CHECK(!client.sendSignal(PeerId{"bob"}, &payload, 1));
    CHECK(client.getLastError() == SecureSignalingError::QueueFull);
    CHECK_INT_EQ(client.getPendingSignalCount(), 1);
    client.stop();
}

TEST_CASE(MalformedDatagramIsDropped) {
    ayt::test::setCurrentCase("MalformedDatagramIsDropped");
    CredentialBook book;
    book.entries["alice"] = {
        SignalingRoomId{"room-a"}, {tokenFromSeed(1), 0}};
    SecureUdpSignalingServerConfig serverConfig;
    serverConfig.bindAddress = "127.0.0.1";
    serverConfig.resolveCredential = book.resolver();
    SecureUdpSignalingServer server(std::move(serverConfig));
    CHECK(server.start());
    UdpSocket attacker;
    CHECK(attacker.create());
    CHECK(attacker.bind("127.0.0.1", 0));
    const std::vector<uint8_t> garbage(128, 0xa5);
    CHECK(attacker.sendTo("127.0.0.1", server.getBoundPort(),
                          garbage.data(), garbage.size()) > 0);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (server.getStats().malformedDrops == 0 &&
           std::chrono::steady_clock::now() < deadline) {
        (void)server.pump();
    }
    CHECK_INT_EQ(server.getStats().malformedDrops, 1);
    attacker.close();
    server.stop();
}

TEST_CASE(AuthenticatedRoomForwardsQueuedOpaqueSignal) {
    ayt::test::setCurrentCase("AuthenticatedRoomForwardsQueuedOpaqueSignal");
    CredentialBook book;
    const auto aliceToken = tokenFromSeed(1);
    const auto bobToken = tokenFromSeed(65);
    book.entries["alice"] = {SignalingRoomId{"room-a"}, {aliceToken, 0}};
    book.entries["bob"] = {SignalingRoomId{"room-a"}, {bobToken, 0}};
    SecureUdpSignalingServerConfig serverConfig;
    serverConfig.bindAddress = "127.0.0.1";
    serverConfig.resolveCredential = book.resolver();
    SecureUdpSignalingServer server(std::move(serverConfig));
    CHECK(server.start());
    auto alice = makeClient(server.getBoundPort(), "room-a", aliceToken);
    auto bob = makeClient(server.getBoundPort(), "room-a", bobToken);
    CHECK(alice.start(PeerId{"alice"}));
    CHECK(bob.start(PeerId{"bob"}));

    const std::vector<uint8_t> payload{0, 1, 2, 0xff, 0x42};
    CHECK(alice.sendSignal(PeerId{"bob"}, payload.data(), payload.size()));
    CHECK_INT_EQ(alice.getPendingSignalCount(), 1);
    CHECK(pumpUntil(server, [&] { return alice.isReady() && bob.isReady(); }, alice, bob));

    bool received = false;
    PeerId sender;
    std::vector<uint8_t> body;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!received && std::chrono::steady_clock::now() < deadline) {
        (void)server.pump();
        (void)alice.poll([](const PeerId&, const void*, size_t) {});
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
    CHECK(body == payload);
    CHECK_INT_EQ(alice.getPendingSignalCount(), 0);
    CHECK_INT_EQ(server.getStats().forwardedSignals, 1);
    alice.stop();
    bob.stop();
    server.stop();
}

TEST_CASE(WrongCredentialCannotRegister) {
    ayt::test::setCurrentCase("WrongCredentialCannotRegister");
    CredentialBook book;
    book.entries["alice"] = {SignalingRoomId{"room-a"}, {tokenFromSeed(1), 0}};
    SecureUdpSignalingServerConfig serverConfig;
    serverConfig.bindAddress = "127.0.0.1";
    serverConfig.resolveCredential = book.resolver();
    SecureUdpSignalingServer server(std::move(serverConfig));
    CHECK(server.start());
    auto alice = makeClient(server.getBoundPort(), "room-a", tokenFromSeed(2));
    CHECK(alice.start(PeerId{"alice"}));
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(50);
    while (std::chrono::steady_clock::now() < deadline) {
        (void)server.pump();
        (void)alice.poll([](const PeerId&, const void*, size_t) {});
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    CHECK(!alice.isReady());
    CHECK_INT_EQ(server.getPeerCount(), 0);
    CHECK(server.getStats().authenticationFailures >= 1);
    alice.stop();
    server.stop();
}

TEST_CASE(ExpiredCredentialCannotRegister) {
    ayt::test::setCurrentCase("ExpiredCredentialCannotRegister");
    CredentialBook book;
    const auto token = tokenFromSeed(1);
    book.entries["alice"] = {SignalingRoomId{"room-a"}, {token, 1}};
    SecureUdpSignalingServerConfig serverConfig;
    serverConfig.bindAddress = "127.0.0.1";
    serverConfig.resolveCredential = book.resolver();
    SecureUdpSignalingServer server(std::move(serverConfig));
    CHECK(server.start());
    auto alice = makeClient(server.getBoundPort(), "room-a", token);
    CHECK(alice.start(PeerId{"alice"}));
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(30);
    while (std::chrono::steady_clock::now() < deadline) {
        (void)server.pump();
        (void)alice.poll([](const PeerId&, const void*, size_t) {});
    }
    CHECK(!alice.isReady());
    CHECK_INT_EQ(server.getPeerCount(), 0);
    CHECK(server.getStats().authenticationFailures >= 1);
    alice.stop();
    server.stop();
}

TEST_CASE(RevokedActiveCredentialIsEvicted) {
    ayt::test::setCurrentCase("RevokedActiveCredentialIsEvicted");
    CredentialBook book;
    const auto aliceToken = tokenFromSeed(1);
    const auto bobToken = tokenFromSeed(65);
    book.entries["alice"] = {SignalingRoomId{"room-a"}, {aliceToken, 0}};
    book.entries["bob"] = {SignalingRoomId{"room-a"}, {bobToken, 0}};
    SecureUdpSignalingServerConfig serverConfig;
    serverConfig.bindAddress = "127.0.0.1";
    serverConfig.credentialRecheckIntervalMs = 0;
    serverConfig.resolveCredential = book.resolver();
    SecureUdpSignalingServer server(std::move(serverConfig));
    CHECK(server.start());
    auto alice = makeClient(server.getBoundPort(), "room-a", aliceToken);
    auto bob = makeClient(server.getBoundPort(), "room-a", bobToken);
    CHECK(alice.start(PeerId{"alice"}));
    CHECK(bob.start(PeerId{"bob"}));
    CHECK(pumpUntil(server, [&] { return alice.isReady() && bob.isReady(); }, alice, bob));
    CHECK_INT_EQ(server.getPeerCount(), 2);
    book.entries.erase("alice");
    const uint8_t payload = 1;
    CHECK(alice.sendSignal(PeerId{"bob"}, &payload, 1));
    CHECK(pumpUntil(server, [&] { return server.getPeerCount() == 1; }, alice, bob));
    CHECK(server.getStats().authenticationFailures >= 1);
    alice.stop();
    bob.stop();
    server.stop();
}

TEST_CASE(RoomIsolationRejectsCrossRoomTarget) {
    ayt::test::setCurrentCase("RoomIsolationRejectsCrossRoomTarget");
    CredentialBook book;
    const auto aliceToken = tokenFromSeed(1);
    const auto charlieToken = tokenFromSeed(97);
    book.entries["alice"] = {SignalingRoomId{"room-a"}, {aliceToken, 0}};
    book.entries["charlie"] = {SignalingRoomId{"room-b"}, {charlieToken, 0}};
    SecureUdpSignalingServerConfig serverConfig;
    serverConfig.bindAddress = "127.0.0.1";
    serverConfig.resolveCredential = book.resolver();
    SecureUdpSignalingServer server(std::move(serverConfig));
    CHECK(server.start());
    auto alice = makeClient(server.getBoundPort(), "room-a", aliceToken);
    auto charlie = makeClient(server.getBoundPort(), "room-b", charlieToken);
    CHECK(alice.start(PeerId{"alice"}));
    CHECK(charlie.start(PeerId{"charlie"}));
    CHECK(pumpUntil(server, [&] { return alice.isReady() && charlie.isReady(); }, alice, charlie));
    const uint8_t payload = 7;
    CHECK(alice.sendSignal(PeerId{"charlie"}, &payload, 1));
    CHECK(pumpUntil(server,
        [&] { return server.getStats().roomMismatchDrops >= 1; }, alice, charlie));
    CHECK(alice.getLastError() == SecureSignalingError::RoomMismatch);
    alice.stop();
    charlie.stop();
    server.stop();
}

TEST_CASE(PerPeerRateLimitDropsBurst) {
    ayt::test::setCurrentCase("PerPeerRateLimitDropsBurst");
    CredentialBook book;
    const auto aliceToken = tokenFromSeed(1);
    const auto bobToken = tokenFromSeed(65);
    book.entries["alice"] = {SignalingRoomId{"room-a"}, {aliceToken, 0}};
    book.entries["bob"] = {SignalingRoomId{"room-a"}, {bobToken, 0}};
    SecureUdpSignalingServerConfig serverConfig;
    serverConfig.bindAddress = "127.0.0.1";
    serverConfig.maxPacketsPerPeerPerSecond = 1;
    serverConfig.resolveCredential = book.resolver();
    SecureUdpSignalingServer server(std::move(serverConfig));
    CHECK(server.start());
    auto alice = makeClient(server.getBoundPort(), "room-a", aliceToken);
    auto bob = makeClient(server.getBoundPort(), "room-a", bobToken);
    CHECK(alice.start(PeerId{"alice"}));
    CHECK(bob.start(PeerId{"bob"}));
    CHECK(pumpUntil(server, [&] { return alice.isReady() && bob.isReady(); }, alice, bob));
    const uint8_t payload = 1;
    CHECK(alice.sendSignal(PeerId{"bob"}, &payload, 1));
    CHECK(alice.sendSignal(PeerId{"bob"}, &payload, 1));
    CHECK(pumpUntil(server,
        [&] { return server.getStats().rateLimitedDrops >= 1; }, alice, bob));
    CHECK_INT_EQ(server.getStats().forwardedSignals, 1);
    alice.stop();
    bob.stop();
    server.stop();
}

TEST_SUITE_END
