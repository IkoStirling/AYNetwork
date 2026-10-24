#include <AYTestFixtures.h>
#include <AYNetwork/HttpClient.h>
#include <httplib.h>
#include <thread>
#include <atomic>
using namespace ayt::net::http;
TEST_SUITE(HttpClientTests)
TEST_CASE(loopback_transport_handles_chunks_limits_redirects_and_cancellation) {
    httplib::Server server;
    server.Post("/echo", [](const auto& req, auto& res) { res.set_content(req.body, "application/json"); });
    server.Get("/redirect", [](const auto&, auto& res) { res.set_redirect("/echo"); });
    server.Get("/slow", [](const auto&, auto& res) { std::this_thread::sleep_for(std::chrono::milliseconds(120)); res.set_content("late", "text/plain"); });
    int port = server.bind_to_any_port("127.0.0.1"); CHECK(port > 0);
    std::jthread runner([&] { server.listen_after_bind(); }); server.wait_until_ready();
    struct Stop { httplib::Server& server; ~Stop() { server.stop(); } } stop{server};
    Request req; req.origin = "http://127.0.0.1:" + std::to_string(port); req.path = "/echo"; req.body = "hello";
    std::string chunks; req.onChunk = [&](std::string_view text) { chunks += text; return true; };
    auto result = perform(req); CHECK(static_cast<bool>(result)); CHECK(result.httpStatus == 200); CHECK(result.body == "hello"); CHECK(chunks == "hello");
    req.maxResponseBytes = 2; CHECK_FALSE(static_cast<bool>(perform(req)));
    req.maxResponseBytes = 1024; req.onChunk = {}; req.path = "/redirect"; req.method = "GET"; req.body.clear();
    CHECK(perform(req).httpStatus == 302);
    req.cancelled = [] { return true; }; CHECK(perform(req).status == Status::Cancelled);
    req.cancelled = {}; req.path = "/slow"; req.timeout = std::chrono::milliseconds(20);
    CHECK(perform(req).status == Status::Timeout);
    req.timeout = std::chrono::seconds(2); std::atomic<bool> cancelled{false};
    req.cancelled = [&] { return cancelled.load(); };
    std::jthread cancel([&] { std::this_thread::sleep_for(std::chrono::milliseconds(20)); cancelled=true; });
    CHECK(perform(req).status == Status::Cancelled);
}
TEST_CASE(origins_and_header_injection_are_rejected) {
    CHECK_FALSE(validOrigin("http://user:password@host")); CHECK_FALSE(validOrigin("https://host/path"));
    CHECK_FALSE(validOrigin("https://host?key=secret")); CHECK(validOrigin("https://host:443"));
    Request req; req.origin = "http://127.0.0.1:1"; req.headers = {{"Authorization", "Bearer value\r\nInjected: yes"}};
    CHECK(perform(req).status == Status::Invalid);
    CHECK(environmentCredential("bad=name").empty());
}
TEST_SUITE_END
int main(int argc, char** argv) { return ayt::test::runTests("AYNetworkHttp", argc, argv); }
