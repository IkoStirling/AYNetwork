// AYNetwork_DedicatedServer - deployable Headless Dedicated runtime shell.

#include <AYNetwork/NetworkModule.h>
#include <AYNetwork/Session/DedicatedServerRuntime.h>
#include <AYNetwork/Session/HttpOnlineServices.h>

#include <atomic>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <memory>
#include <string>
#include <thread>

namespace
{

std::atomic<bool> g_draining{false};
void onSignal(int) { g_draining.store(true); }

bool parsePort(const char* text, uint16_t& value) {
    if (!text) return false;
    unsigned parsed = 0;
    const std::string input{text};
    const auto result = std::from_chars(
        input.data(), input.data() + input.size(), parsed);
    if (result.ec != std::errc{} || result.ptr != input.data() + input.size() ||
        parsed == 0 || parsed > 65535) return false;
    value = static_cast<uint16_t>(parsed);
    return true;
}

std::string environment(const char* name) {
    const char* value = std::getenv(name);
    return value ? std::string{value} : std::string{};
}

class ReferenceWorldHost final : public ayt::net::IDedicatedWorldHost {
public:
    bool startAuthoritativeWorld(
        const ayt::net::DedicatedAllocation& allocation) override {
        if (!allocation.content.isValid()) return false;
        _worlds.emplace(allocation.allocationId, allocation);
        std::printf(
            "AY_DEDICATED_WORLD state=loaded allocation=%llu match=%llu "
            "content=%s version=%s seed=%llu\n",
            static_cast<unsigned long long>(allocation.allocationId),
            static_cast<unsigned long long>(allocation.matchId),
            allocation.content.contentId.c_str(),
            allocation.content.contentVersion.c_str(),
            static_cast<unsigned long long>(allocation.content.contentSeed));
        std::fflush(stdout);
        return true;
    }

    void stopAuthoritativeWorld(
        ayt::net::DedicatedAllocationId allocationId) override {
        _worlds.erase(allocationId);
        std::printf("AY_DEDICATED_WORLD state=unloaded allocation=%llu\n",
                    static_cast<unsigned long long>(allocationId));
    }

    void playerConnected(ayt::net::DedicatedAllocationId allocationId,
                         const ayt::net::PeerId& peer,
                         ayt::net::NetConnection*) override {
        std::printf(
            "AY_DEDICATED_PLAYER state=connected allocation=%llu peer=%s\n",
            static_cast<unsigned long long>(allocationId), peer.value.c_str());
    }

    void playerDisconnected(ayt::net::DedicatedAllocationId allocationId,
                            const ayt::net::PeerId& peer) override {
        std::printf(
            "AY_DEDICATED_PLAYER state=disconnected allocation=%llu peer=%s\n",
            static_cast<unsigned long long>(allocationId), peer.value.c_str());
    }

    void tickAuthoritativeWorlds(float) override {
        // The game executable replaces this reference host with its scene/world
        // adapter. Allocation content is already validated and authoritative
        // client admission is enforced by DedicatedServerRuntime.
    }

private:
    std::map<ayt::net::DedicatedAllocationId,
             ayt::net::DedicatedAllocation> _worlds;
};

} // namespace

int main(int argc, char** argv) {
    if (argc != 9) {
        std::fprintf(stderr,
            "usage: AYNetwork_DedicatedServer <backend-address> <backend-port> "
            "<instance> <region> <build> <public-address> <game-port> "
            "<capacity>\n");
        return 2;
    }
    uint16_t backendPort = 0;
    uint16_t gamePort = 0;
    uint16_t capacity = 0;
    if (!parsePort(argv[2], backendPort) || !parsePort(argv[7], gamePort) ||
        !parsePort(argv[8], capacity)) {
        std::fprintf(stderr, "invalid Dedicated Server port or capacity\n");
        return 2;
    }
    const std::string controlToken = environment("AY_ONLINE_SERVER_TOKEN");
    if (controlToken.size() < 32) {
        std::fprintf(stderr,
            "AY_ONLINE_SERVER_TOKEN must contain the fleet control token\n");
        return 2;
    }

    ayt::net::HttpOnlineServicesClientConfig backendConfig;
    backendConfig.serverAddress = argv[1];
    backendConfig.serverPort = backendPort;
    backendConfig.useTls = environment("AY_ONLINE_BACKEND_TLS") == "1" ||
        environment("AY_ONLINE_BACKEND_TLS") == "true";
    backendConfig.dedicatedControlToken = controlToken;
    auto backend = std::make_shared<ayt::net::HttpOnlineServices>(
        std::move(backendConfig));
    auto worlds = std::make_shared<ReferenceWorldHost>();

    ayt::net::registerNetworkSubSystem();
    auto* network = ayt::net::findRegisteredNetworkSubSystem();
    if (!network || !network->initialize()) {
        std::fprintf(stderr, "failed to initialize Dedicated network runtime\n");
        return 1;
    }

    ayt::net::DedicatedServerRuntimeConfig runtimeConfig;
    runtimeConfig.registration = {
        argv[3], argv[4], argv[5], argv[6], gamePort, capacity};
    ayt::net::DedicatedServerRuntime runtime(
        *network, backend, worlds, std::move(runtimeConfig));
    if (!runtime.start()) {
        const auto status = runtime.getStatus();
        std::fprintf(stderr, "failed to start Dedicated runtime: %s\n",
                     status.message.c_str());
        network->shutdown();
        return 1;
    }

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);
    std::printf(
        "AY_DEDICATED_SERVER state=ready instance=%s region=%s build=%s "
        "address=%s port=%u capacity=%u\n",
        argv[3], argv[4], argv[5], argv[6], gamePort, capacity);
    std::fflush(stdout);

    auto previous = std::chrono::steady_clock::now();
    bool drainStarted = false;
    int exitCode = 0;
    while (true) {
        const auto current = std::chrono::steady_clock::now();
        const float delta = std::chrono::duration<float>(current - previous).count();
        previous = current;
        if (g_draining.load() && !drainStarted) {
            drainStarted = true;
            runtime.beginDrain();
            std::printf("AY_DEDICATED_SERVER state=draining\n");
        }
        runtime.update(delta);
        const auto status = runtime.getStatus();
        if (status.state == ayt::net::DedicatedServerRuntimeState::Stopped) break;
        if (status.state == ayt::net::DedicatedServerRuntimeState::Failed) {
            std::fprintf(stderr, "AY_DEDICATED_SERVER state=failed error=%u "
                         "service_error=%u message=%s\n",
                         static_cast<unsigned>(status.error),
                         static_cast<unsigned>(status.serviceError),
                         status.message.c_str());
            exitCode = 1;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    runtime.stop();
    network->shutdown();
    std::printf("AY_DEDICATED_SERVER state=stopped\n");
    return exitCode;
}
