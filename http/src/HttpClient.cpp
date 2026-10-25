#include <AYNetwork/HttpClient.h>
#include <httplib.h>
#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <memory>
#include <thread>
#include <charconv>
#ifdef _WIN32
#include "WinHttpTransport.h"
#endif

namespace ayt::net::http {
bool validOrigin(std::string_view origin) {
    std::size_t start = origin.starts_with("https://") ? 8 : origin.starts_with("http://") ? 7 : 0;
    if (!start || origin.size() <= start || origin.size() > 2048) return false;
    auto host = origin.substr(start);
    return host.find_first_of("/@?#\\ \t\r\n") == std::string_view::npos && host.find('\0') == std::string_view::npos;
}
std::string environmentCredential(std::string_view name) {
    if (name.empty() || name.size() > 256 || !std::all_of(name.begin(), name.end(), [](unsigned char c) { return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_'; })) return {};
#ifdef _WIN32
    char* value = nullptr; std::size_t size = 0;
    if (_dupenv_s(&value, &size, std::string(name).c_str()) || !value) return {};
    std::unique_ptr<char, decltype(&std::free)> owned(value, &std::free);
    return {owned.get()};
#else
    const auto* value = std::getenv(std::string(name).c_str()); return value ? value : "";
#endif
}
Response perform(const Request& request) {
    Response response;
    if (!validOrigin(request.origin) || request.path.empty() || request.path.front() != '/' || request.path.starts_with("//") || request.path.find_first_of("\r\n") != std::string::npos || request.path.find('\0') != std::string::npos || (request.method != "POST" && request.method != "GET") || request.timeout.count() <= 0 || request.timeout > std::chrono::hours(1) || !request.maxResponseBytes || request.maxResponseBytes > 64 * 1024 * 1024 || request.body.size() > 16 * 1024 * 1024) { response.status = Status::Invalid; return response; }
    for (const auto& [key, value] : request.headers) if (key.empty() || key.find_first_of(": \t\r\n") != std::string::npos || key.find('\0') != std::string::npos || value.find_first_of("\r\n") != std::string::npos || value.find('\0') != std::string::npos) { response.status = Status::Invalid; return response; }
    auto cancelled = [&] { try { return request.cancelled && request.cancelled(); } catch (...) { return true; } };
    if (cancelled()) { response.status = Status::Cancelled; return response; }
    try {
#ifdef _WIN32
        auto destination=std::string_view(request.origin).substr(request.origin.starts_with("https://")?8:7);
        const bool local=destination=="localhost"||destination.starts_with("localhost:")||destination=="127.0.0.1"||destination.starts_with("127.0.0.1:")||destination=="[::1]"||destination.starts_with("[::1]:");
        // The tested native WinHTTP path bypasses an explicit proxy for a
        // loopback destination. Preserve the established explicit-routing
        // contract with the existing client for this narrow compatibility case.
        if(request.proxyOrigin.empty()||!local)return detail::performWinHttp(request);
#endif
        httplib::Client client(request.origin);
        if (!client.is_valid()) { response.status = Status::Invalid; return response; }
        const auto proxy=request.proxyOrigin.empty()&&request.useSystemProxy?systemProxyOrigin(request.origin):request.proxyOrigin;
        if(!proxy.empty()){
            if(!proxy.starts_with("http://")||!validOrigin(proxy)){response.status=Status::Invalid;return response;}auto address=proxy.substr(7);std::string host;std::string portText;int port=80;
            if(address.starts_with('[')){const auto end=address.find(']');if(end==std::string::npos){response.status=Status::Invalid;return response;}host=address.substr(1,end-1);if(end+1<address.size()){if(address[end+1]!=':'){response.status=Status::Invalid;return response;}portText=address.substr(end+2);}}
            else {const auto colon=address.find(':');host=address.substr(0,colon);if(colon!=std::string::npos)portText=address.substr(colon+1);}
            if(!portText.empty()){const auto parsed=std::from_chars(portText.data(),portText.data()+portText.size(),port);if(parsed.ec!=std::errc{}||parsed.ptr!=portText.data()+portText.size()||port<1||port>65535){response.status=Status::Invalid;return response;}}
            else if(address.ends_with(':')){response.status=Status::Invalid;return response;}
            if(host.empty()){response.status=Status::Invalid;return response;}client.set_proxy(host,port);
        }
        client.set_follow_location(false);
        client.set_connection_timeout(request.timeout);
        client.set_read_timeout(request.timeout);
        client.set_write_timeout(request.timeout);
        client.set_max_timeout(request.timeout);
        client.set_payload_max_length(request.maxResponseBytes);
        client.enable_server_certificate_verification(true);
        const auto deadline = std::chrono::steady_clock::now() + request.timeout;
        std::atomic<bool> stopped{false}, timedOut{false};
        std::jthread watchdog([&](std::stop_token stop) {
            while (!stop.stop_requested()) {
                if (cancelled()) { stopped = true; client.stop(); return; }
                if (std::chrono::steady_clock::now() >= deadline) { timedOut = true; client.stop(); return; }
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
        });
        httplib::Request wire;
        wire.method = request.method; wire.path = request.path; wire.body = request.body;
        for (const auto& pair : request.headers) wire.headers.emplace(pair);
        bool overBudget = false, consumerFailed = false;
        wire.content_receiver = [&](const char* data, std::size_t size, std::uint64_t, std::uint64_t) {
            if (size > request.maxResponseBytes - response.body.size()) { overBudget = true; return false; }
            response.body.append(data, size);
            if (request.onChunk) {
                try { if (!request.onChunk({data, size})) { consumerFailed = true; return false; } }
                catch (...) { consumerFailed = true; return false; }
            }
            return !cancelled() && std::chrono::steady_clock::now() < deadline;
        };
        auto result = client.send(wire);
        watchdog.request_stop(); watchdog.join();
        if (result) response.httpStatus = result->status;
        if (stopped || cancelled()) response.status = Status::Cancelled;
        else if (timedOut || std::chrono::steady_clock::now() >= deadline) response.status = Status::Timeout;
        else if (overBudget || (!result && result.error() == httplib::Error::ExceedMaxPayloadSize)) response.status = Status::Limit;
        else if (consumerFailed) response.status = Status::Consumer;
        else response.status = result ? Status::Ok : Status::Transport;
        if (!response) response.body.clear();
    } catch (...) { response.status = Status::Transport; response.body.clear(); }
    return response;
}
}
