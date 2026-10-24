#include <AYNetwork/HttpClient.h>
#include <algorithm>
#ifdef _WIN32
#include <Windows.h>
#include <winhttp.h>
#endif
namespace ayt::net::http {
namespace {
std::string lower(std::string value){std::transform(value.begin(),value.end(),value.begin(),[](unsigned char c){return c>='A'&&c<='Z'?static_cast<char>(c+32):static_cast<char>(c);});return value;}
std::string trim(std::string value){const auto first=value.find_first_not_of(" \t"),last=value.find_last_not_of(" \t");return first==std::string::npos?std::string{}:value.substr(first,last-first+1);}
bool matches(const std::string& host,const std::string& pattern){size_t h=0,p=0,star=std::string::npos,back=0;while(h<host.size()){if(p<pattern.size()&&(pattern[p]=='?'||pattern[p]==host[h])){++h;++p;}else if(p<pattern.size()&&pattern[p]=='*'){star=p++;back=h;}else if(star!=std::string::npos){p=star+1;h=++back;}else return false;}while(p<pattern.size()&&pattern[p]=='*')++p;return p==pattern.size();}
#ifdef _WIN32
std::string utf8(const wchar_t* text){if(!text)return {};const auto length=static_cast<int>(wcslen(text));if(length>8192)return {};const auto n=WideCharToMultiByte(CP_UTF8,WC_ERR_INVALID_CHARS,text,length,nullptr,0,nullptr,nullptr);if(n<=0)return {};std::string out(n,'\0');WideCharToMultiByte(CP_UTF8,0,text,length,out.data(),n,nullptr,nullptr);return out;}
#endif
}
std::string systemProxyOrigin(std::string_view destination){
    if(!validOrigin(destination))return {};const bool secure=destination.starts_with("https://");auto host=lower(std::string(destination.substr(secure?8:7)));if(host.starts_with('[')){const auto end=host.find(']');if(end==std::string::npos)return {};host=host.substr(1,end-1);}else if(const auto colon=host.find(':');colon!=std::string::npos)host.resize(colon);
    if(host=="localhost"||host=="127.0.0.1"||host=="::1")return {};
#ifdef _WIN32
    WINHTTP_CURRENT_USER_IE_PROXY_CONFIG config{};if(!WinHttpGetIEProxyConfigForCurrentUser(&config))return {};
    struct Release{WINHTTP_CURRENT_USER_IE_PROXY_CONFIG& value;~Release(){if(value.lpszProxy)GlobalFree(value.lpszProxy);if(value.lpszProxyBypass)GlobalFree(value.lpszProxyBypass);if(value.lpszAutoConfigUrl)GlobalFree(value.lpszAutoConfigUrl);}}release{config};
    const auto bypass=lower(utf8(config.lpszProxyBypass));for(size_t start=0;start<bypass.size();){auto end=bypass.find(';',start);if(end==std::string::npos)end=bypass.size();const auto pattern=trim(bypass.substr(start,end-start));if((pattern=="<local>"&&host.find('.')==std::string::npos)||(!pattern.empty()&&matches(host,pattern)))return {};start=end+1;}
    auto server=trim(utf8(config.lpszProxy));if(server.empty())return {};
    if(server.find('=')!=std::string::npos){std::string selected;for(size_t start=0;start<server.size();){auto end=server.find(';',start);if(end==std::string::npos)end=server.size();const auto item=trim(server.substr(start,end-start));const auto equal=item.find('=');if(equal!=std::string::npos&&lower(trim(item.substr(0,equal)))==(secure?"https":"http")){selected=trim(item.substr(equal+1));break;}start=end+1;}server=std::move(selected);}
    if(server.empty())return {};if(server.find("://")==std::string::npos)server="http://"+server;if(!server.starts_with("http://")||!validOrigin(server))return {};return server;
#else
    return {};
#endif
}
}
