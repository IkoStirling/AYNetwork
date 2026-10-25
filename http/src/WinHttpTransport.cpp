#ifdef _WIN32
#include "WinHttpTransport.h"
#include <Windows.h>
#include <winhttp.h>
#include <algorithm>
#include <array>
#include <charconv>
#include <condition_variable>
#include <mutex>
#include <string>

namespace ayt::net::http::detail {
namespace {
std::wstring wide(const std::string& text){
    if(text.empty())return {};const auto count=MultiByteToWideChar(CP_UTF8,MB_ERR_INVALID_CHARS,text.data(),static_cast<int>(text.size()),nullptr,0);
    if(count<=0)return {};std::wstring result(count,L'\0');MultiByteToWideChar(CP_UTF8,MB_ERR_INVALID_CHARS,text.data(),static_cast<int>(text.size()),result.data(),count);return result;
}
struct Handle {
    HINTERNET value=nullptr;
    explicit Handle(HINTERNET handle):value(handle){}
    ~Handle(){if(value)WinHttpCloseHandle(value);}
    Handle(const Handle&)=delete;Handle& operator=(const Handle&)=delete;
};
struct Completion {
    std::mutex mutex;std::condition_variable changed;
    DWORD event=0,error=0,count=0;bool closing=false;
    std::array<char,16384> buffer{};
};
void CALLBACK callback(HINTERNET,DWORD_PTR context,DWORD status,void* information,DWORD length){
    if(!context)return;auto& completion=*reinterpret_cast<Completion*>(context);
    std::lock_guard lock(completion.mutex);
    switch(status){
    case WINHTTP_CALLBACK_STATUS_HANDLE_CLOSING:completion.closing=true;break;
    case WINHTTP_CALLBACK_STATUS_REQUEST_ERROR:
        completion.error=information&&length>=sizeof(WINHTTP_ASYNC_RESULT)?static_cast<WINHTTP_ASYNC_RESULT*>(information)->dwError:ERROR_WINHTTP_INTERNAL_ERROR;
        completion.event=status;break;
    case WINHTTP_CALLBACK_STATUS_SENDREQUEST_COMPLETE:
    case WINHTTP_CALLBACK_STATUS_HEADERS_AVAILABLE:completion.event=status;break;
    case WINHTTP_CALLBACK_STATUS_DATA_AVAILABLE:
        completion.count=information&&length>=sizeof(DWORD)?*static_cast<DWORD*>(information):0;completion.event=status;break;
    case WINHTTP_CALLBACK_STATUS_READ_COMPLETE:completion.count=length;completion.event=status;break;
    default:return;
    }
    completion.changed.notify_all();
}
// Only the invoking thread calls WinHTTP request methods or closes the handle.
// Keep the callback context/buffer alive until the final HANDLE_CLOSING notice.
struct AsyncRequest {
    HINTERNET value=nullptr;Completion& completion;bool observed=false;
    ~AsyncRequest(){if(!value)return;WinHttpCloseHandle(value);if(observed){std::unique_lock lock(completion.mutex);completion.changed.wait(lock,[&]{return completion.closing;});}}
};
bool proxyAddress(const std::string& origin,std::wstring& address){
    if(!origin.starts_with("http://")||!validOrigin(origin))return false;
    auto text=std::string_view(origin).substr(7);std::string_view host,port;
    if(text.starts_with('[')){const auto end=text.find(']');if(end==std::string_view::npos)return false;host=text.substr(0,end+1);if(end+1<text.size()){if(text[end+1]!=':')return false;port=text.substr(end+2);}}
    else{const auto colon=text.find(':');host=text.substr(0,colon);if(colon!=std::string_view::npos)port=text.substr(colon+1);}
    if(host.empty()||text.ends_with(':'))return false;if(!port.empty()){int parsed=0;const auto converted=std::from_chars(port.data(),port.data()+port.size(),parsed);if(converted.ec!=std::errc{}||converted.ptr!=port.data()+port.size()||parsed<1||parsed>65535)return false;}
    address=wide(std::string(text));return !address.empty();
}
}
Response performWinHttp(const Request& request){
    Response response;const auto started=std::chrono::steady_clock::now(),deadline=started+request.timeout;
    auto cancelled=[&]{try{return request.cancelled&&request.cancelled();}catch(...){return true;}};
    auto fail=[&](Status status){response.status=status;response.body.clear();return response;};
    const auto origin=wide(request.origin),path=wide(request.path),method=wide(request.method);
    if(origin.empty()||path.empty()||method.empty())return fail(Status::Invalid);
    URL_COMPONENTS parts{};parts.dwStructSize=sizeof(parts);parts.dwHostNameLength=static_cast<DWORD>(-1);
    if(!WinHttpCrackUrl(origin.c_str(),static_cast<DWORD>(origin.size()),0,&parts)||!parts.nPort||!parts.dwHostNameLength)return fail(Status::Invalid);
    const std::wstring host(parts.lpszHostName,parts.dwHostNameLength);
    const auto proxy=request.proxyOrigin.empty()&&request.useSystemProxy?systemProxyOrigin(request.origin):request.proxyOrigin;
    std::wstring proxyName;if(!proxy.empty()&&!proxyAddress(proxy,proxyName))return fail(Status::Invalid);
    Handle session(WinHttpOpen(L"AliyatEngine/HTTP",proxy.empty()?WINHTTP_ACCESS_TYPE_NO_PROXY:WINHTTP_ACCESS_TYPE_NAMED_PROXY,proxy.empty()?WINHTTP_NO_PROXY_NAME:proxyName.c_str(),WINHTTP_NO_PROXY_BYPASS,WINHTTP_FLAG_ASYNC));
    if(!session.value)return fail(Status::Transport);
    const int timeout=static_cast<int>(request.timeout.count());if(!WinHttpSetTimeouts(session.value,timeout,timeout,timeout,timeout))return fail(Status::Transport);
    Handle connection(WinHttpConnect(session.value,host.c_str(),parts.nPort,0));if(!connection.value)return fail(Status::Transport);
    Completion completion;AsyncRequest wire{WinHttpOpenRequest(connection.value,method.c_str(),path.c_str(),nullptr,WINHTTP_NO_REFERER,WINHTTP_DEFAULT_ACCEPT_TYPES,parts.nScheme==INTERNET_SCHEME_HTTPS?WINHTTP_FLAG_SECURE:0),completion};
    if(!wire.value)return fail(Status::Transport);
    DWORD_PTR context=reinterpret_cast<DWORD_PTR>(&completion);
    if(!WinHttpSetOption(wire.value,WINHTTP_OPTION_CONTEXT_VALUE,&context,sizeof(context)))return fail(Status::Transport);
    if(WinHttpSetStatusCallback(wire.value,callback,WINHTTP_CALLBACK_FLAG_ALL_COMPLETIONS|WINHTTP_CALLBACK_FLAG_HANDLES,0)==WINHTTP_INVALID_STATUS_CALLBACK)return fail(Status::Transport);
    wire.observed=true;
    DWORD disabled=WINHTTP_DISABLE_REDIRECTS|WINHTTP_DISABLE_COOKIES|WINHTTP_DISABLE_AUTHENTICATION;
    if(!WinHttpSetOption(wire.value,WINHTTP_OPTION_DISABLE_FEATURE,&disabled,sizeof(disabled)))return fail(Status::Transport);
    DWORD automatic=WINHTTP_AUTOLOGON_SECURITY_LEVEL_HIGH;
    if(!WinHttpSetOption(wire.value,WINHTTP_OPTION_AUTOLOGON_POLICY,&automatic,sizeof(automatic)))return fail(Status::Transport);
    DWORD decompression=WINHTTP_DECOMPRESSION_FLAG_GZIP|WINHTTP_DECOMPRESSION_FLAG_DEFLATE;
    if(!WinHttpSetOption(wire.value,WINHTTP_OPTION_DECOMPRESSION,&decompression,sizeof(decompression)))return fail(Status::Transport);
    std::wstring headers;
    for(const auto& [name,value]:request.headers){const auto key=wide(name),content=wide(value);if(key.empty()||!value.empty()&&content.empty())return fail(Status::Invalid);headers+=key+L": "+content+L"\r\n";}
    auto wait=[&](DWORD expected){
        std::unique_lock lock(completion.mutex);
        for(;;){
            // No user callback executes while the callback-state mutex is held.
            lock.unlock();const bool stop=cancelled();const bool timeoutReached=std::chrono::steady_clock::now()>=deadline;lock.lock();
            if(stop){response.status=Status::Cancelled;return false;}
            if(timeoutReached){response.status=Status::Timeout;return false;}
            if(completion.event==WINHTTP_CALLBACK_STATUS_REQUEST_ERROR){response.status=completion.error==ERROR_WINHTTP_TIMEOUT?Status::Timeout:Status::Transport;return false;}
            if(completion.event==expected){completion.event=0;return true;}
            completion.changed.wait_for(lock,std::chrono::milliseconds(5));
        }
    };
    auto issued=[&](BOOL ok){if(ok||GetLastError()==ERROR_IO_PENDING)return true;response.status=GetLastError()==ERROR_WINHTTP_TIMEOUT?Status::Timeout:Status::Transport;return false;};
    if(cancelled())return fail(Status::Cancelled);
    if(!issued(WinHttpSendRequest(wire.value,headers.empty()?WINHTTP_NO_ADDITIONAL_HEADERS:headers.c_str(),static_cast<DWORD>(headers.size()),request.body.empty()?WINHTTP_NO_REQUEST_DATA:const_cast<char*>(request.body.data()),static_cast<DWORD>(request.body.size()),static_cast<DWORD>(request.body.size()),context))||!wait(WINHTTP_CALLBACK_STATUS_SENDREQUEST_COMPLETE))return fail(response.status);
    if(!issued(WinHttpReceiveResponse(wire.value,nullptr))||!wait(WINHTTP_CALLBACK_STATUS_HEADERS_AVAILABLE))return fail(response.status);
    DWORD status=0,size=sizeof(status);if(!WinHttpQueryHeaders(wire.value,WINHTTP_QUERY_STATUS_CODE|WINHTTP_QUERY_FLAG_NUMBER,WINHTTP_HEADER_NAME_BY_INDEX,&status,&size,WINHTTP_NO_HEADER_INDEX))return fail(Status::Transport);
    response.httpStatus=static_cast<int>(status);
    for(;;){
        if(!issued(WinHttpQueryDataAvailable(wire.value,nullptr))||!wait(WINHTTP_CALLBACK_STATUS_DATA_AVAILABLE))return fail(response.status);
        DWORD available;{std::lock_guard lock(completion.mutex);available=completion.count;}
        if(!available)break;
        const auto count=std::min<DWORD>(available,static_cast<DWORD>(completion.buffer.size()));
        if(count>request.maxResponseBytes-response.body.size())return fail(Status::Limit);
        if(!issued(WinHttpReadData(wire.value,completion.buffer.data(),count,nullptr))||!wait(WINHTTP_CALLBACK_STATUS_READ_COMPLETE))return fail(response.status);
        DWORD read;{std::lock_guard lock(completion.mutex);read=completion.count;}
        if(read>count)return fail(Status::Transport);if(!read)break;
        response.body.append(completion.buffer.data(),read);
        if(request.onChunk)try{if(!request.onChunk({completion.buffer.data(),read}))return fail(Status::Consumer);}catch(...){return fail(Status::Consumer);}
    }
    if(cancelled())return fail(Status::Cancelled);if(std::chrono::steady_clock::now()>=deadline)return fail(Status::Timeout);
    response.status=Status::Ok;return response;
}
}
#endif
