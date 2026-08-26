// AYNetwork_SignalingServer - minimal UDP rendezvous process.

#include <AYNetwork/Signaling/UdpSignaling.h>

#include <atomic>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <string>
#include <thread>

namespace {
std::atomic<bool> g_running{true};
void onSignal(int) { g_running.store(false); }
}

int main(int argc, char** argv) {
    ayt::net::UdpSignalingServerConfig config;
    config.port = 28080;
    if (argc >= 2) config.bindAddress = argv[1];
    if (argc >= 3) {
        unsigned value = 0;
        const std::string text = argv[2];
        const auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
        if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size() ||
            value == 0 || value > 65535) {
            std::fprintf(stderr, "usage: AYNetwork_SignalingServer [bind-address] [port]\n");
            return 2;
        }
        config.port = static_cast<uint16_t>(value);
    }

    ayt::net::UdpSignalingServer server(config);
    if (!server.start()) {
        std::fprintf(stderr, "failed to bind signaling server on %s:%u\n",
                     config.bindAddress.c_str(), config.port);
        return 1;
    }
    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);
    std::printf("AYNetwork signaling server listening on %s:%u\n",
                config.bindAddress.c_str(), server.getBoundPort());
    while (g_running.load()) {
        const size_t processed = server.pump();
        if (processed == 0) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    server.stop();
    return 0;
}
