#pragma once
#include <chrono>
#include <cstddef>
#include <functional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace ayt::net::http {
enum class Status { Ok, Invalid, Cancelled, Timeout, Limit, Transport, Consumer };
struct Request {
    std::string origin; // scheme://host[:port], no userinfo, path, query or fragment
    std::string path = "/";
    std::string method = "POST";
    std::vector<std::pair<std::string, std::string>> headers;
    std::string body;
    std::chrono::milliseconds timeout{30000};
    std::size_t maxResponseBytes = 4 * 1024 * 1024;
    std::function<bool()> cancelled;
    /// Streaming callback runs on the invoking thread; false aborts consumption.
    /// Callback bytes are tentative until the final response succeeds.
    std::function<bool(std::string_view)> onChunk;
    /// Optional credential-free HTTP CONNECT proxy origin. Empty means direct,
    /// unless useSystemProxy opts into the current Windows manual proxy setting.
    std::string proxyOrigin;
    bool useSystemProxy = false;
};
struct Response {
    Status status = Status::Transport;
    int httpStatus = 0;
    std::string body;
    explicit operator bool() const noexcept { return status == Status::Ok; }
};
/// Bounded synchronous transport with verified TLS, disabled redirects, and
/// per-call clients. No request/response logging or inclusion in error strings.
/// Cancellation/total timeout stop active sockets; OS DNS/launch can still delay return.
Response perform(const Request& request);
/// Read a credential when needed. Missing/invalid names return empty; never logs.
std::string environmentCredential(std::string_view name);
/// True for a syntactically valid origin. Plain HTTP is allowed by this generic
/// transport; credential-bearing callers must enforce their deployment policy.
bool validOrigin(std::string_view origin);
/// Read the current Windows manual proxy/bypass configuration for this origin.
/// Returns an HTTP proxy origin or empty for direct/loopback/unsupported settings.
/// Does not execute PAC, discover proxies, change OS settings or read credentials.
std::string systemProxyOrigin(std::string_view destinationOrigin);
}
