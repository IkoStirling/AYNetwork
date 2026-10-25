# Standalone bounded HTTP transport

**Version:** 0.1.0
**Date:** 2026-10-24
**Status:** Active
**Owner:** AYNetwork

Link `AYNetworkHttp` and include `AYNetwork/HttpClient.h`. Enable `AY_ENABLE_HTTP`,
or let the explicit inference/application-services opt-in select it. This target
does not link game sessions, GNS, Entity, replication or the AYNetwork main target.
It uses Windows asynchronous WinHTTP or the installed cpp-httplib transport on
other platforms, without exposing either backend's types/macros.

Optional Request.proxyOrigin selects a credential-free HTTP proxy; HTTPS uses
CONNECT and retains server certificate verification. Request.useSystemProxy opts
into the current Windows user's manual proxy and host bypass rules. Empty/default
is direct, with no system-setting changes, PAC execution or proxy auto-discovery.
systemProxyOrigin provides the same bounded resolution for other transports;
literal loopback/localhost bypass automatic proxy use. Explicit proxy origins must
be http://host[:port], without userinfo/path/query; authenticated/SOCKS/HTTPS proxies
and PAC/WPAD are not implemented. Unsupported system settings resolve to direct;
an invalid explicit proxy fails. Nothing reads Credential Manager or logs proxy
credentials. Tests cover a real loopback HTTP proxy with an unresolvable target,
invalid/credential-bearing proxies and loopback bypass. Public deployments remain
separate checks; HibernationAgent's live search smoke uses this option.

`perform` is synchronous and owns its request. It supports GET/POST, request/response
budgets, finite total/connect/read/write timeouts, optional tentative chunk delivery
and cancellation. Redirects are disabled and TLS peer verification remains enabled.
Cancellation predicates can be called concurrently by the owner and watchdog and
must be thread-safe. Returning false/throwing from a chunk consumer fails the call.
On Windows one owner pumps asynchronous send/headers/body completions and polls
cancellation/total deadline at most every 5ms between operations. Closing the
request cancels a pending header wait; its context remains alive until the final
HANDLE_CLOSING callback. No worker is detached. Cookies, redirects and automatic
Windows authentication are disabled; explicit headers remain caller-owned. TLS
uses Windows certificate verification. Gzip/deflate are decoded before the response
byte budget. An explicitly proxied literal loopback destination retains the
existing cpp-httplib client: native WinHTTP bypassed that proxy on the tested host.
That compatibility case and other platforms retain the client/watchdog, backend
TLS store and potentially delayed socket cancellation. Automatic system-proxy
resolution still bypasses loopback by policy; it never changes system settings.
OS setup/teardown can still delay return; this is not forced thread termination.
Responses on transport/cancel/limit errors contain no partial
body. HTTP error status codes remain distinguishable from transport failures.

Origins cannot contain userinfo, path, query or fragment. Generic HTTP is permitted;
credential-bearing higher layers impose HTTPS or an explicit local deployment rule.
`environmentCredential` reads only on demand. The module installs no request,
response, header, credential or error-body logger. Callers must not log secret
headers themselves. No automatic retries are performed.

Evidence: `unittest/Test_HttpClient.cpp`, loopback request/chunks/redirect/budget,
timeout/cancellation before the first header, callback ownership/consumer failure,
and injection rejection. The first-header regression reproduced a three-second
Windows cancellation delay before the WinHTTP implementation; now the call returns
within one second while the synthetic server continues its delayed response.
Live public TLS/provider calls are
separate deployment checks, not implied by these local tests.

Optional `AYNetwork_HttpProbe` (EXCLUDE_FROM_ALL) performs a bounded credential-free
GET and prints only status, byte count and elapsed time. It never prints response
content or headers. Example: `AYNetwork_HttpProbe https://example.com / --system-proxy`.
Cancellation lifetime follows [WinHttpCloseHandle](https://learn.microsoft.com/en-us/windows/win32/api/winhttp/nf-winhttp-winhttpclosehandle).
# Header-only TLS composition (2026-10-26)

AYNetworkHttp requires TLS rather than silently degrading to HTTP-only. If a
header-only httplib package was installed without a TLS feature, CMake composes
OpenSSL 3 and CPPHTTPLIB_OPENSSL_SUPPORT on its global imported interface target,
so the client and local consumers use a consistent class layout. A precompiled
httplib without TLS is rejected with a configuration error. Existing packages
with an enabled TLS backend retain that backend. AYNetwork_HttpTests covers the
bounded transport; local tests do not certify public TLS service availability.
