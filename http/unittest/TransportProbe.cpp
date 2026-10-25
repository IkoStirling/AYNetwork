#include <AYNetwork/HttpClient.h>
#include <iostream>
int main(int argc,char** argv){
    if(argc<3||argc>5)return 2;
    ayt::net::http::Request request;request.method="GET";request.origin=argv[1];request.path=argv[2];request.timeout=std::chrono::seconds(15);request.maxResponseBytes=1024*1024;
    if(argc==4){if(std::string_view(argv[3])!="--system-proxy")return 2;request.useSystemProxy=true;}
    if(argc==5){if(std::string_view(argv[3])!="--proxy")return 2;request.proxyOrigin=argv[4];}
    const auto start=std::chrono::steady_clock::now();const auto result=ayt::net::http::perform(request);
    std::cout<<"{\"status\":"<<static_cast<int>(result.status)<<",\"http_status\":"<<result.httpStatus<<",\"body_bytes\":"<<result.body.size()<<",\"elapsed_ms\":"<<std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now()-start).count()<<"}\n";
    return result?0:1;
}
