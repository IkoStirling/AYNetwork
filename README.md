# AYNetwork

AYNetwork 是网络传输、协议、复制与 RPC 模块，使用 GameNetworkingSockets，并通过 AYReflect 元数据支持实体复制。

## 公开接口

```cpp
#include <AYNetwork.h>
#include <AYNetwork/INetwork.h>
#include <AYNetwork/Protocol/PacketCodec.h>
#include <AYNetwork/Replication/ReplicationManager.h>
```

稳定接口位于 `interface/AYNetwork/`，实现与协议公开头位于 `include/AYNetwork/`。

## 依赖

- AYCore、AYGameLoop、AYStorage、AYReflect
- GameNetworkingSockets、LZ4
- Win32：ws2_32

## P2P、NAT 穿透与中继

P2P 使用 GNS 1.6 的 custom signaling + ICE。游戏层只接触 `PeerId`、
`P2PConfig` 和 `ISignalingTransport`，不依赖 Steam：

```cpp
#include <AYNetwork.h>
#include <AYNetwork/Signaling/UdpSignaling.h>

auto signaling = std::make_shared<ayt::net::UdpSignalingClient>(
    ayt::net::UdpSignalingClientConfig{
        .serverAddress = "signal.example.com",
        .serverPort = 28080,
    });

ayt::net::P2PConfig p2p;
p2p.localPeerId = ayt::net::PeerId{"account-42-session-a"};
p2p.virtualPort = 0;
p2p.icePolicy = ayt::net::P2PIcePolicy::DirectOnly;
p2p.stunServers = {"stun.example.com:3478"};
p2p.allowPrivateCandidates = false;

network->initialize();
network->configureP2P(p2p, signaling);
// Host side:
network->listenP2P();
// Joining side instead:
network->connectP2P(ayt::net::PeerId{"host-session"});
```

`configureP2P` 必须在该进程创建任何 GNS 连接或监听器之前调用，因为 standalone
GNS 每个进程只有一个本地身份。既有 `connect(address, port)` / `listen(port)`
IP 模式保持兼容。

内置信令服务器是无游戏状态的 UDP rendezvous 转发器：

```text
cmake -S . -B <build> -DAYNETWORK_BUILD_SIGNALING_SERVER=ON
AYNetwork_SignalingServer 0.0.0.0 28080
```

公网/受信客户端优先使用 v2 鉴权信令。每个 Peer 使用账户/会话服务签发的独立
32-byte token，并由服务端 resolver 校验其房间与过期时间：

```text
AYNetwork_SecureSignalingServer 0.0.0.0 28080 credentials.txt
```

协议包含 HMAC-SHA256、双 nonce challenge/confirm 端点证明、重放窗口、房间隔离、
限流、心跳与 NAT rebinding。完整协议见
[docs/secure-signaling-v2.md](docs/secure-signaling-v2.md)。HMAC 不隐藏信令元数据；
需要元数据保密的部署仍应提供 WSS/QUIC `ISignalingTransport`。

本机多进程直连 smoke（先启动 server，再分别启动 host/join）：

```text
AYNetwork_P2PSmokePeer host smoke-host 127.0.0.1 28080 17
AYNetwork_P2PSmokePeer join smoke-client smoke-host 127.0.0.1 28080 17
```

同一工具通过正式 `INetworkSubSystem` 运行协议握手、PacketCodec、Server RPC、
权威对象复制和应用层质量探针。它可输出 RTT 分位数、抖动与丢包率，也可在同一
进程中执行断开重连和让 Host 等待多个独立会话。通过环境变量配置 STUN、短期凭证
和期望 direct 路径，用于两个真实 NAT 网络间的自动化门禁，见
[docs/p2p-public-probe.md](docs/p2p-public-probe.md)。

会话层 v1 提供后端无关的本地视图：`getP2PSessionInfo()` 查询 Host/Client 与
Hosting/Connecting/Active 状态，`getP2PPeers()` 返回按 PeerId 稳定排序的远端
成员，`findP2PPeer()` / `disconnectP2PPeer()` 用 PeerId 定位或移除连接。断线
原因通过 GNS 应用码跨端传递，因此 `UserQuit`、`Kicked`、`HostShutdown` 与真实
链路丢失可以被游戏逻辑可靠区分。

当前推荐部署是“自建鉴权信令 + 公共 STUN + direct-only”。TURN 仍受接口支持，
但不是当前发布门禁；在需要覆盖无法打洞的 NAT 时再部署和验证。

它适合开发、自托管原型和受信网络。公网生产环境应在自定义
`ISignalingTransport` 中加入账号鉴权、防重放、限流与 TLS/DTLS，或由现有后端
通过 WebSocket/HTTPS 转发相同的 opaque GNS 信令。TURN 凭证应短期签发，不能
把长期密钥写入客户端。

当前 P2P 拓扑仍沿用 AYNetwork 的 authority/listen-host 模型；房间目录、主机迁移
和真正的无主机一致性协议属于会话层，不由信令转发器承担。

协议版本、Authority 模型和复制/RPC 阶段见 [design.md](design.md)。
