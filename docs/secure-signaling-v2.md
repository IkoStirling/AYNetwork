# AYNetwork Secure Signaling Protocol v2

Secure signaling v2 is a room-scoped, authenticated UDP transport for opaque
GameNetworkingSockets rendezvous messages. It implements
`ISignalingTransport`; GNS, replication, RPC, and gameplay code do not depend
on this protocol.

## Security model

The account/session service issues each peer a random 32-byte token bound to a
`PeerId`, `SignalingRoomId`, and expiry time. The token is supplied to the
client out of band and resolved by the signaling server through
`SecureSignalingCredentialResolver`.

- Every datagram is authenticated with HMAC-SHA256 over the complete header
  and body. Tokens are never sent on the wire.
- Sender and target use different tokens. The server verifies the sender and
  re-signs forwarded messages for the target.
- Signals may only be forwarded between currently registered peers in the
  same room.
- Each direction has a 64-packet sliding replay window.
- A new endpoint must complete `Register -> Challenge -> Confirm -> Ack`.
  Replaying a captured Register from another endpoint cannot move the route.
- A refresh from the already proven endpoint receives an Ack directly.
- Per-peer packet/byte limits, peer caps, pending-registration expiry, session
  expiry, heartbeat, and inactive-peer pruning are enforced by the server.
- Active credentials are periodically rechecked through the resolver (default
  five seconds, configurable down to every packet), so revocation does not
  depend on the peer disconnecting.

HMAC provides integrity and authentication, not confidentiality. GNS traffic
is encrypted after ICE connects, but rendezvous metadata can still be observed
on the signaling path. Deploy this UDP backend on a protected service network
or add a WSS/QUIC `ISignalingTransport` when signaling-metadata privacy is a
requirement.

## Wire format

All integers are little-endian. Maximum datagram size is configurable and
defaults to 48 KiB.

```text
offset  size  field
0       4     magic = "AYS2"
4       1     version = 2
5       1     packet type
6       1     from PeerId length
7       1     to PeerId length
8       1     room id length
9       3     reserved = 0
12      4     payload length
16      8     client nonce
24      8     server nonce (zero only for Register)
32      8     sequence number
40      N     from PeerId, to PeerId, room id, opaque payload
40+N    32    HMAC-SHA256 tag
```

Packet types are Register, RegisterChallenge, RegisterConfirm, RegisterAck,
Signal, Heartbeat, HeartbeatAck, Unregister, and Error. A Signal must contain a
target and non-empty opaque payload; control messages carry neither. Error
contains one `SecureSignalingError` byte.

## Credential resolver contract

The resolver is the production integration seam. It must return a credential
only when the account/session service confirms that the peer is allowed in the
requested room. Tokens should be random, single-session, short lived, and
revocable. Returning `expiresAtUnixSeconds = 0` is supported for tests but is
not recommended for public deployments.

The standalone server accepts a startup credential file for probes:

```text
# peer-id room-id 64-hex-token [expires-unix-seconds]
host-123 room-7 0123456789abcdef... 1790000000
client-9 room-7 fedcba9876543210... 1790000000
```

It is not an account server and does not mint credentials. A production
service embeds `SecureUdpSignalingServer` and supplies a resolver backed by its
session/token authority.
