#include <AYTestFixtures.h>
#include <AYNetwork/HttpClient.h>
#include <httplib.h>
#include <thread>
#include <atomic>
#include <mutex>
#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <fstream>
#endif
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
TEST_CASE(cancellation_interrupts_a_request_waiting_for_its_first_header){
    httplib::Server server;std::atomic<bool> entered=false,cancelled=false;
    server.Get("/headers",[&](const auto&,auto& response){entered=true;std::this_thread::sleep_for(std::chrono::seconds(3));response.set_content("late","text/plain");});
    const auto port=server.bind_to_any_port("127.0.0.1");CHECK(port>0);std::jthread runner([&]{server.listen_after_bind();});server.wait_until_ready();struct Stop{httplib::Server& s;~Stop(){s.stop();}}stop{server};
    Request request;request.origin="http://127.0.0.1:"+std::to_string(port);request.method="GET";request.path="/headers";request.timeout=std::chrono::seconds(5);request.cancelled=[&]{return cancelled.load();};
    std::jthread cancel([&]{while(!entered)std::this_thread::sleep_for(std::chrono::milliseconds(1));cancelled=true;});
    const auto start=std::chrono::steady_clock::now();const auto result=perform(request);const auto elapsed=std::chrono::steady_clock::now()-start;
    CHECK(result.status==Status::Cancelled);CHECK(elapsed<std::chrono::seconds(1));CHECK(result.body.empty());
}
TEST_CASE(deadline_interrupts_a_first_header_wait_without_publishing_partial_bytes){
    httplib::Server server;server.Get("/headers",[](const auto&,auto& response){std::this_thread::sleep_for(std::chrono::seconds(2));response.set_content("late","text/plain");});
    const auto port=server.bind_to_any_port("127.0.0.1");CHECK(port>0);std::jthread runner([&]{server.listen_after_bind();});server.wait_until_ready();struct Stop{httplib::Server& s;~Stop(){s.stop();}}stop{server};
    Request request;request.origin="http://127.0.0.1:"+std::to_string(port);request.method="GET";request.path="/headers";request.timeout=std::chrono::milliseconds(50);
    const auto start=std::chrono::steady_clock::now();const auto result=perform(request);CHECK(result.status==Status::Timeout);CHECK(std::chrono::steady_clock::now()-start<std::chrono::seconds(1));CHECK(result.body.empty());
}
TEST_CASE(chunk_callbacks_keep_the_owner_thread_and_reject_or_cancel_without_partial_results){
    httplib::Server server;server.Get("/chunks",[](const auto&,auto& response){response.set_chunked_content_provider("text/plain",[](size_t offset,httplib::DataSink& sink){if(offset>=8192){sink.done();return true;}std::this_thread::sleep_for(std::chrono::milliseconds(15));std::string part(1024,'x');return sink.write(part.data(),part.size());});});
    const auto port=server.bind_to_any_port("127.0.0.1");CHECK(port>0);std::jthread runner([&]{server.listen_after_bind();});server.wait_until_ready();struct Stop{httplib::Server& s;~Stop(){s.stop();}}stop{server};
    Request request;request.origin="http://127.0.0.1:"+std::to_string(port);request.method="GET";request.path="/chunks";request.timeout=std::chrono::seconds(3);const auto owner=std::this_thread::get_id();bool same=true;std::size_t bytes=0;
    request.onChunk=[&](std::string_view value){same=same&&std::this_thread::get_id()==owner;bytes+=value.size();return true;};auto complete=perform(request);CHECK(static_cast<bool>(complete));CHECK(same&&bytes==8192&&complete.body.size()==8192);
    request.onChunk=[](auto){return false;};auto rejected=perform(request);CHECK(rejected.status==Status::Consumer&&rejected.body.empty());
    request.onChunk=[](auto)->bool{throw std::runtime_error("synthetic");};rejected=perform(request);CHECK(rejected.status==Status::Consumer&&rejected.body.empty());
    std::atomic<bool> cancelled=false;request.onChunk=[&](auto){cancelled=true;return true;};request.cancelled=[&]{return cancelled.load();};const auto interrupted=perform(request);CHECK(interrupted.status==Status::Cancelled&&interrupted.body.empty());
    request.cancelled={};request.onChunk={};request.maxResponseBytes=1500;auto limited=perform(request);CHECK(limited.status==Status::Limit&&limited.body.empty());
}
#ifdef _WIN32
TEST_CASE(response_limit_counts_decompressed_bytes){
    static const unsigned char compressed[]={0x1f,0x8b,0x08,0x00,0x00,0x00,0x00,0x00,0x02,0x0a,0xed,0xc1,0x01,0x0d,0x00,0x00,0x00,0xc2,0xa0,0xda,0x8f,0x6f,0x0f,0x07,0x14,0x00,0x00,0x00,0xf0,0x6e,0xc1,0x77,0x10,0x3e,0x00,0x10,0x00,0x00};
    httplib::Server server;server.Get("/gzip",[](const auto&,auto& response){response.set_content(reinterpret_cast<const char*>(compressed),sizeof(compressed),"text/plain");response.set_header("Content-Encoding","gzip");});
    const auto port=server.bind_to_any_port("127.0.0.1");CHECK(port>0);std::jthread runner([&]{server.listen_after_bind();});server.wait_until_ready();struct Stop{httplib::Server& s;~Stop(){s.stop();}}stop{server};
    Request request;request.origin="http://127.0.0.1:"+std::to_string(port);request.method="GET";request.path="/gzip";request.maxResponseBytes=5000;auto result=perform(request);CHECK(static_cast<bool>(result)&&result.body==std::string(4096,'x'));
    request.maxResponseBytes=1000;result=perform(request);CHECK(result.status==Status::Limit&&result.body.empty());
}
#endif
#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
TEST_CASE(untrusted_local_tls_certificate_is_rejected_without_reaching_the_handler){
    ayt::test::ScratchDirectory scratch("http-untrusted-certificate");
    std::unique_ptr<EVP_PKEY,decltype(&EVP_PKEY_free)> key(EVP_PKEY_Q_keygen(nullptr,nullptr,"RSA",2048),EVP_PKEY_free);
    std::unique_ptr<X509,decltype(&X509_free)> certificate(X509_new(),X509_free);CHECK(key&&certificate);if(!key||!certificate)return;
    X509_set_version(certificate.get(),2);ASN1_INTEGER_set(X509_get_serialNumber(certificate.get()),1);X509_gmtime_adj(X509_getm_notBefore(certificate.get()),-60);X509_gmtime_adj(X509_getm_notAfter(certificate.get()),3600);X509_set_pubkey(certificate.get(),key.get());
    auto* subject=X509_get_subject_name(certificate.get());X509_NAME_add_entry_by_txt(subject,"CN",MBSTRING_ASC,reinterpret_cast<const unsigned char*>("localhost"),-1,-1,0);X509_set_issuer_name(certificate.get(),subject);CHECK(X509_sign(certificate.get(),key.get(),EVP_sha256())>0);
    auto save=[&](const std::filesystem::path& file,bool privateKey){std::unique_ptr<BIO,decltype(&BIO_free)> bio(BIO_new(BIO_s_mem()),BIO_free);if(!bio)return false;const int ok=privateKey?PEM_write_bio_PrivateKey(bio.get(),key.get(),nullptr,nullptr,0,nullptr,nullptr):PEM_write_bio_X509(bio.get(),certificate.get());if(!ok)return false;char* bytes=nullptr;const auto length=BIO_get_mem_data(bio.get(),&bytes);std::ofstream output(file,std::ios::binary);output.write(bytes,length);return static_cast<bool>(output);};
    const auto cert=scratch.path()/"synthetic-cert.pem",pem=scratch.path()/"synthetic-key.pem";CHECK(save(cert,false)&&save(pem,true));std::atomic<bool> reached=false;
    httplib::SSLServer server(cert.string().c_str(),pem.string().c_str());CHECK(server.is_valid());server.Get("/",[&](const auto&,auto& response){reached=true;response.set_content("untrusted","text/plain");});const auto port=server.bind_to_any_port("127.0.0.1");CHECK(port>0);std::jthread runner([&]{server.listen_after_bind();});server.wait_until_ready();struct Stop{httplib::Server& s;~Stop(){s.stop();}}stop{server};
    Request request;request.origin="https://127.0.0.1:"+std::to_string(port);request.path="/";request.method="GET";request.timeout=std::chrono::seconds(3);const auto result=perform(request);CHECK(!static_cast<bool>(result)&&result.status==Status::Transport&&result.body.empty());CHECK(!reached);
}
#endif
TEST_CASE(explicit_http_proxy_routes_without_resolving_the_origin_and_rejects_credentials){
    httplib::Server proxy;std::string target;std::mutex targetMutex;proxy.Get(R"(.*)",[&](const auto& req,auto& res){{std::lock_guard lock(targetMutex);target=req.path;}res.set_content("through-proxy","text/plain");});const auto port=proxy.bind_to_any_port("127.0.0.1");CHECK(port>0);std::jthread runner([&]{proxy.listen_after_bind();});proxy.wait_until_ready();struct Stop{httplib::Server& s;~Stop(){s.stop();}}stop{proxy};
    Request request;request.origin="http://unresolvable-fixture.invalid";request.path="/probe";request.method="GET";request.timeout=std::chrono::seconds(2);request.proxyOrigin="http://127.0.0.1:"+std::to_string(port);auto result=perform(request);CHECK(static_cast<bool>(result));CHECK(result.body=="through-proxy");{std::lock_guard lock(targetMutex);CHECK(target.find("/probe")!=std::string::npos);}
    request.origin="http://127.0.0.1:1";result=perform(request);CHECK(static_cast<bool>(result));CHECK(result.body=="through-proxy");
    request.proxyOrigin="http://user:password@127.0.0.1:1";CHECK(perform(request).status==Status::Invalid);request.proxyOrigin="https://127.0.0.1:1";CHECK(perform(request).status==Status::Invalid);request.proxyOrigin="http://127.0.0.1:999999";CHECK(perform(request).status==Status::Invalid);request.proxyOrigin="http://127.0.0.1:";CHECK(perform(request).status==Status::Invalid);
    CHECK(systemProxyOrigin("http://127.0.0.1:1234").empty());CHECK(systemProxyOrigin("http://localhost:1234").empty());CHECK(systemProxyOrigin("invalid").empty());
}
TEST_SUITE_END
int main(int argc, char** argv) { return ayt::test::runTests("AYNetworkHttp", argc, argv); }
