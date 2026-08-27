# Public direct-P2P probe

`AYNetwork_P2PSmokePeer` runs on two machines behind different NATs and asserts
the ICE path selected by GNS. It uses the production `INetworkSubSystem`, so a
successful session covers protocol handshake, PacketCodec, Server RPC,
authority-state replication, and application ping/echo. It exits with code 7
when the path does not match `AY_P2P_EXPECT_PATH`.

## Infrastructure

1. Expose `AYNetwork_SecureSignalingServer` on one public UDP port.
2. Configure a reachable public STUN endpoint. A public address for the
   signaling server does not replace STUN: signaling forwards opaque rendezvous
   data, while STUN discovers each peer's NAT-mapped UDP endpoint.
3. Issue a distinct short-lived signaling token to each peer.

TURN is optional and currently outside the direct-only release gate. An example
coturn baseline remains in `tools/p2p_deploy/turnserver.conf.example` for a later
fallback milestone.

## Secure signaling server

```text
AYNetwork_SecureSignalingServer 0.0.0.0 28080 credentials.txt
```

Credential file format is documented in `secure-signaling-v2.md`. Restrict the
file to the service account and delete expired probe credentials.

Generate independent probe tokens with:

```text
AYNetwork_SecureSignalingServer --generate-token
```

## Probe environment

Each peer receives its own `AY_P2P_SIGNAL_TOKEN`; all other values normally
match. Lists are comma separated.

```text
AY_P2P_SIGNAL_ROOM=room-7
AY_P2P_SIGNAL_TOKEN=<this-peer-64-hex-token>
AY_P2P_ICE_POLICY=direct
AY_P2P_STUN=stun.cloudflare.com:3478
AY_P2P_ALLOW_PRIVATE=false
AY_P2P_EXPECT_PATH=direct
AY_P2P_PROBE_SECONDS=60
AY_P2P_PROBE_INTERVAL_MS=100
```

The signaling token above authenticates access to the rendezvous room. It is
not the gameplay-session join ticket. To exercise Host-authoritative admission,
set the same opaque test value using different variable names:

```text
# Host only
AY_P2P_EXPECT_JOIN_TICKET=<temporary-session-ticket>

# Join only
AY_P2P_JOIN_TICKET=<temporary-session-ticket>
```

In an actual game, the account/matchmaking service supplies the opaque Join
ticket and the Host validator checks it. The built-in probe compares text only;
that comparison is a test harness, not a production ticket issuer. If the Host
does not install a validator, AYNetwork intentionally auto-admits for local
development compatibility.

Run the host first:

```text
AYNetwork_P2PSmokePeer host host-123 signal.example.com 28080 17
```

Do not start Join until Host prints `AY_P2P_HOST_READY`. Registration alone is
not sufficient: that marker is emitted only after the incoming P2P route has
been installed successfully.

Then run the joining peer from another network:

```text
AYNetwork_P2PSmokePeer join client-9 host-123 signal.example.com 28080 17
```

Success includes these machine-readable records:

```text
AY_P2P_ENGINE ... handshake=ok rpc=ok replication=ok
AY_P2P_ADMISSION ... state=admitted ticket=accepted
AY_P2P_BARRIER ... ready=2 total=2 open=true
AY_P2P_SESSION_VIEW ... role=host|client roster=ok ready_peers=...
AY_P2P_QUALITY ... loss_pct=... rtt_p50_ms=... rtt_p95_ms=... jitter_ms=...
AY_P2P_RESULT ... path=direct ...
```

`roster=ok` verifies that the connected `PeerId` resolves to the same live
`NetConnection`, that the session role/state is correct, and that the peer is
visible as Ready and admitted. Exit code 12 means this engine-session view was
inconsistent even if ICE itself connected. Exit code 13 is a join-ticket or
admission failure; exit code 14 means all admitted members did not reach the
Ready barrier. Application/RPC/replication probing starts only after admission
and the barrier opens.

For two connection cycles from the same Join process, set
`AY_P2P_RECONNECTS=1` on Join and `AY_P2P_EXPECT_SESSIONS=2` on Host. For
multiple joining processes, set Host's expected session count to their total.

To validate seat-preserving recovery instead of a fresh repeated join, set
`AY_P2P_RESUME_RECONNECTS=true` on both Host and Join, keep
`AY_P2P_RECONNECTS=1`, and expect two sessions. Success includes
`AY_P2P_RESUME ... seat=N next_session=2`; the probe separately asserts that
this seat equals the first connection's seat. The barrier stays closed while
the seat is reserved.

Host Migration needs one Host and at least two Join processes. Set these values
on all three before starting them:

```text
AY_P2P_HOST_MIGRATION=true
AY_P2P_MIGRATION_MEMBERS=3
```

Start Host first, then launch both Join commands with distinct local PeerIds.
After the three-member barrier opens, the probe Host requests a graceful
handoff. The lowest client seat becomes Host. Both survivors must emit
`AY_P2P_MIGRATION ... phase=complete`, preserve their seats, advance exactly
one epoch, report `barrier=2/2`, and observe `authority=17777` through Server
RPC plus replicated state. Force-terminating the old Host after both clients
emit `phase=armed` exercises crash recovery instead; it may take longer because
clients first retry the original Host reservation.

The graceful handoff is a reliable transaction: final Full snapshots precede
Prepare, all survivors ACK matching replicated/application-state hashes, Commit
is retried until Commit ACKs arrive, and only then does the old Host leave. This
is intentionally safe on paths whose RTT is greater than 100 ms. A rejection or
Prepare timeout aborts without advancing the epoch. After Commit, loss of the
old Host accelerates the already-decided election rather than starting a second
epoch.

## Engine lifecycle integration

Applications should subscribe with `addP2PSessionEventListener()` after the
network subsystem is registered and remove the returned non-zero listener id
before the application object is destroyed. Events are delivered after the
transport pump, not from a GNS callback. `MigrationStarted` pauses
authority-sensitive application work; `AuthorityChanged` is the commit point
for switching simulation ownership and rebroadcasting retained replication
spawns. `MigrationFailed` ends the transition without granting authority.

`SeatReserved`, `SeatRestored`, and `SeatReservationExpired` expose the
reconnect-grace lifecycle to lobby/game code. The event includes a snapshot of
`P2PSessionInfo`, the affected peer, and its seat. Listeners remain installed
across disconnect/reconnect cycles until explicitly removed.

`isP2PMigrationFrozen()` is the synchronous pause gate for game systems. For
non-replicated authority state, install `setP2PMigrationStateCallbacks()`:
capture is limited to 64 KiB, validation must be side-effect-free, and apply is
called only after Commit. Do not duplicate ReplicationManager-owned fields in
this payload. Crash recovery has no old Host to capture from and therefore does
not provide this optional state bridge.

AYEditor consumes this contract directly. A promoted client keeps its launch
role for World teardown, marks retained `NetworkComponent` instances as owned,
and uses the new session role as the live authority source. New peers therefore
receive spawn announcements from the migrated Host without recreating the
replicated entities.

## Required matrix

Run at least these gates before calling a release internet-ready:

| Gate | Settings | Expected |
|---|---|---|
| LAN direct | `direct`, private enabled | `path=direct` |
| Public direct | `direct`, STUN set, private disabled | `path=direct` |
| Signaling auth | invalid/expired peer token | registration fails |
| Room isolation | target in another room | signal rejected |
| Session admission | wrong/missing gameplay Join ticket | reason `AdmissionRejected` |
| Ready barrier | Host + Join set local Ready | `ready=2 total=2 open=true` |
| Seat recovery | resume reconnect with the same PeerId | old seat equals new seat |
| Graceful migration | Host + two clients | epoch +1, lowest seat is Host, barrier 2/2 |
| Crash migration | force-terminate armed Host | survivors elect once and preserve seats |
| Sustained path | 60 seconds, 100 ms interval | engine/quality/result all pass |

Repeat public-direct across home broadband, mobile hotspot and other available
networks. Symmetric NAT and UDP-restricted networks may fail in direct-only mode;
that is an explicit typed probe failure until TURN fallback becomes a milestone.
The repository can automate process/path assertions, but those external NAT
topologies and public credentials cannot be simulated by a same-host unit test.
