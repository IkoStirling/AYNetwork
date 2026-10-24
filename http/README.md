# Standalone bounded HTTP transport

**Version:** 0.1.0
**Date:** 2026-10-24
**Status:** Active
**Owner:** AYNetwork

Link `AYNetworkHttp` and include `AYNetwork/HttpClient.h`. Enable `AY_ENABLE_HTTP`,
or let the explicit inference/application-services opt-in select it. This target
does not link game sessions, GNS, Entity, replication or the AYNetwork main target.
It wraps the installed cpp-httplib backend without exposing its types/macros.

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

`perform` is synchronous and owns its client. It supports GET/POST, request/response
budgets, finite total/connect/read/write timeouts, optional tentative chunk delivery
and cancellation. Redirects are disabled and TLS peer verification remains enabled.
Cancellation predicates can be called concurrently by the owner and watchdog and
must be thread-safe. Returning false/throwing from a chunk consumer fails the call.
OS DNS and TLS/platform waits may delay socket cancellation; this is not forced
thread termination. Responses on transport/cancel/limit errors contain no partial
body. HTTP error status codes remain distinguishable from transport failures.

Origins cannot contain userinfo, path, query or fragment. Generic HTTP is permitted;
credential-bearing higher layers impose HTTPS or an explicit local deployment rule.
`environmentCredential` reads only on demand. The module installs no request,
response, header, credential or error-body logger. Callers must not log secret
headers themselves. No automatic retries are performed.

Evidence: `unittest/Test_HttpClient.cpp`, loopback request/chunks/redirect/budget,
timeout/cancellation and injection rejection. Live public TLS/provider calls are
separate deployment checks, not implied by these local tests.
