// design reference: AYNetwork/design.md, checked owner-thread egress.
#include <AYNetwork.h>
#include <AYNetwork/Transport/NetConnectionImpl.h>
#include <AYNetwork/Transport/GnsConnection.h>
#include <AYTest.h>
#include <chrono>
#include <thread>
#include <vector>
using namespace ayt::net;
TEST_SUITE(CheckedSend)
TEST_CASE(CheckedSendQueueAndPartialFragmentFailure) {
    std::unique_ptr<INetworkSubSystem> server(createNetworkSubSystemForTest()),client(createNetworkSubSystemForTest());
    CHECK(server && client);if(!server || !client)return;
    CHECK(server->initialize() && client->initialize());
    struct Cleanup{INetworkSubSystem* a;INetworkSubSystem* b;~Cleanup(){b->shutdown();a->shutdown();}}cleanup{server.get(),client.get()};
    server->listen(27583);client->connect("127.0.0.1",27583);
    const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(8);
    while(std::chrono::steady_clock::now()<deadline && (!client->isConnected() || server->getConnections().empty())) {
        server->update(.001f);client->update(.001f);std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    CHECK(client->isConnected() && !server->getConnections().empty());
    if(!client->isConnected() || server->getConnections().empty())return;
    auto* peer=client->getConnection();std::vector<uint8_t> payload(8192,0x5a);
    CHECK(client->trySendTo(nullptr,0,payload.data(),payload.size())==NetSendResult::Disconnected);
    CHECK(client->trySendTo(peer,4,payload.data(),payload.size())==NetSendResult::Invalid);
    CHECK(client->trySendTo(peer,0,nullptr,1)==NetSendResult::Invalid);
    CHECK(client->trySendTo(peer,0,payload.data(),payload.size(),8192)==NetSendResult::Invalid); // Framing also counts.
    CHECK(client->trySendTo(server->getConnections().front(),0,payload.data(),payload.size())==NetSendResult::Invalid);
    auto* inner=static_cast<NetConnectionImpl*>(peer)->getInner();unsigned calls=0;
    inner->setFakeTransportSender([&](const auto*,auto,auto){return ++calls!=2;});
    CHECK(client->trySendTo(peer,0,payload.data(),payload.size())==NetSendResult::RetryLater);
    CHECK(calls==2); // A failed later fragment cannot be reported as accepted.
    calls=0;inner->setFakeTransportSender([&](const auto*,auto,auto){++calls;return true;});
    CHECK(client->trySendTo(peer,0,payload.data(),payload.size())==NetSendResult::Accepted);
    CHECK(calls>2);inner->setFakeTransportSender(nullptr);
    unsigned received=0;server->onMessage(CHANNEL_RELIABLE,[&](auto*,auto,const void* data,size_t size){
        CHECK(size==payload.size());if(size==payload.size())CHECK(std::equal(payload.begin(),payload.end(),static_cast<const uint8_t*>(data)));++received;
    });
    unsigned accepted=0;bool pressure=false;
    for(unsigned i=0;i<10000;++i) {
        const auto result=client->trySendTo(peer,0,payload.data(),payload.size(),10000);
        if(result==NetSendResult::RetryLater){pressure=true;break;}
        CHECK(result==NetSendResult::Accepted);if(result!=NetSendResult::Accepted)break;++accepted;
    }
    CHECK(accepted>0 && pressure);
    const auto delivered=std::chrono::steady_clock::now()+std::chrono::seconds(8);
    while(received<accepted && std::chrono::steady_clock::now()<delivered) {
        server->update(.001f);client->update(.001f);std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    CHECK(received==accepted);
    // Fault interceptors have a separate queue: checked acceptance must not hide it.
    TransportFaultProfile profile;profile.latencyMeanMs=1;client->setTransportFaultProfile(peer->getId(),profile);
    CHECK(client->trySendTo(peer,0,payload.data(),payload.size())==NetSendResult::Unsupported);
}
TEST_SUITE_END
