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
- GameNetworkingSockets、LZ4、cpp-httplib、nlohmann-json、libsodium、SQLite
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

会话层提供后端无关的成员视图与 Host 权威准入。Client 用
`setP2PJoinTicket()` 设置账号/匹配服务签发的不透明票据，Host 通过
`setP2PJoinValidator()` 决定接纳或返回类型化拒绝原因；未准入连接不能发送应用、
RPC 或复制流量。未设置 validator 时保持开发兼容模式，自动接纳。

`setP2PLocalReady()` 与 `getP2PReadyBarrierInfo()` 提供带单调 revision 的加载屏障，
统计 Host 和所有已准入成员。`getP2PSessionInfo()`、`getP2PPeers()`、
`findP2PPeer()` / `disconnectP2PPeer()` 则提供稳定的角色、成员和连接映射。断线
原因通过 GNS 应用码跨端传递，可区分 `AdmissionRejected`、`UserQuit`、`Kicked`、
`HostShutdown` 与真实链路丢失。

断线恢复默认保留 30 秒席位。Host 只在 `ConnectionLost` 时保留
`PeerId + sessionId + epoch + seatId`；`UserQuit`、`Kicked` 等显式离开立即释放。
Client 调用 `reconnectP2P()` 会携带该恢复元组，校验成功后恢复原席位和 Ready 状态，
无需重新执行游戏票据校验。可用 `setP2PReconnectGracePeriodMs()` 在 0 到 10 分钟内
调整窗口；保留成员仍计入 Ready barrier，但在恢复前不算 Ready，因此游戏不会在缺员
时误开局。生产环境必须由鉴权信令或平台身份把 PeerId 绑定到账号，不能信任客户端
自报的 PeerId。

可选 Host Migration 保持 listen-host 权威模型：所有节点先调用
`setP2PHostMigrationEnabled(true)`。优雅退出时旧 Host 调用
`requestP2PHostMigration()`，以可靠控制帧发布下一 epoch 和候选 Host；异常断线时 Client
先尝试恢复原 Host，失败后按最低稳定席位、再按 PeerId 确定性选举。新 Host 保留
sessionId 和存活成员席位、递增 epoch，并强制向重连成员发送完整复制基线。
`getP2PSessionMembers()` 与 `getP2PSessionInfo()` 可观察席位、保留状态、epoch 和迁移阶段。
Prepare 开始时成员集会被冻结；期间新的 Join/Resume 返回 `SessionClosed`，Ready 修改和
席位过期暂停，Commit/Abort 只发给该固定参与者集合。`migrationFailure` 提供稳定的
类型化失败原因，而不是要求应用解析日志。

玩家控制对象应在 `ReplicationManager::registerObject()` 后调用
`bindP2PObjectOwner(netId, peerId, seatId)`。AYNetwork 保存稳定 PeerId/seat，并在断线
恢复或 Host Migration 后把它重新解析为当前 connectionId；`getP2PObjectOwner()` 可用于
诊断，`unbindP2PObjectOwner()` 恢复为 `SimulatedProxy`。游戏层不应长期保存旧
connectionId 作为 ownership 身份。

迁移只能继承每个候选节点已收到的复制状态；仅存在旧 Host 内存中的未复制状态、
未持久化 RPC 副作用和连接局部状态会丢失。该机制也不是分区共识：无法互通的网络
分区可能各自选主，游戏/匹配服务仍需 epoch 仲裁或会话终止策略。

### 参考会话后端

`AYNetwork_SessionServer` 提供第一版可替换的会话后端：TCP/HTTP 管理房间、成员凭证、
10 秒 Host 租约与 epoch CAS，UDP 端继续运行既有安全信令。两者只是参考部署中共用
一个进程和凭证目录；游戏数据仍由两端的 GNS P2P 连接直接传输。

```text
AYNetwork_SessionServer 0.0.0.0 18080 203.0.113.10 28080
AYNetwork_SessionProbe 203.0.113.10 18080
```

Host/Client 通过 `HttpP2PSessionService` 创建或加入会话，再用
`P2PSessionCoordinator` 异步完成 grant 安装、安全信令配置、listen/connect、Host
续租、离开和迁移 epoch CAS。应用只需在正常网络循环中调用 `update()`；后端 HTTP 不会
阻塞网络线程。优雅迁移在 Prepare ACK 后先提交后端 CAS，再允许 AYNetwork Commit；
崩溃迁移则要求确定性候选在 Promotion 前完成过期 lease 的 self-claim。低层的
`applyP2PSessionGrant()`、validator、Join Ticket 和 `P2PSessionLeaseKeeper` 仍可供
自定义大厅流程单独使用。

开发模式使用内存状态；生产模式可启用 SQLite WAL 持久化、ChaCha20-Poly1305 token
静态加密、持久 Ed25519 签名身份、准入认证、限流和 JSONL 审计。HTTP 仍应只监听回环
地址并由反向代理终止 TLS；SQLite 多实例仅限同机，跨主机需要事务数据库适配器。
后端 CAS 与 AYNetwork 的多节点 Prepare/Commit 由非阻塞 authority gate 串联，但仍不
是假装成一个分布式原子提交：CAS 后进程崩溃等跨系统失败必须按后端 epoch 关闭旧会话并
重新加入，不能回退 epoch。
接口、端点和完整接入顺序见
[docs/p2p-session-service.md](docs/p2p-session-service.md)。

### Lobby、Matchmaking 与 Dedicated Server

`OnlineServices.h` 提供后端中立的 `ILobbyService`、`IMatchmakingService` 和
`IDedicatedServerService`。`InMemoryOnlineServices` 是线程安全参考实现：Lobby 可按
revision CAS 更新并直接启动 P2P session；匹配支持完整 party、P2P/Dedicated/Any
拓扑和每个 ticket 的最小秘密暴露；Dedicated 目录支持注册凭证、租约、draining、容量
预留和过期 fencing。`SqliteOnlineServices` 提供 WAL、事务化容量预留、多进程 expiring
claim、重启恢复和 XChaCha20-Poly1305 凭证静态加密。`HttpOnlineServices` 提供可信身份
派生的远程客户端，路由可与 SessionServer 共用端口；生产参考进程使用账号服务签发的
后端中立 HMAC 玩家 token、持久 SQLite state 与独立 fleet token。完整边界见
[docs/online-services.md](docs/online-services.md)。

游戏侧可由 `OnlineSessionCoordinator` 非阻塞编排 Lobby 和匹配票据，并把 P2P assignment
中的现成 grant 直接交给 `P2PSessionCoordinator`，避免重复 join；Dedicated assignment
则通过可替换的 `IDedicatedSessionConnector` 安装 reservation token、连接并回报状态。
统一状态覆盖 `Idle / InLobby / Queueing / Assigned / Connecting / InSession / Failed`，
取消竞争会以 canonical ticket 结果为准，不会静默丢弃已经提交的 assignment。

当前推荐部署是“自建鉴权信令 + 公共 STUN + direct-only”。TURN 仍受接口支持，
但不是当前发布门禁；在需要覆盖无法打洞的 NAT 时再部署和验证。

它适合开发、自托管原型和受信网络。公网生产环境应在自定义
`ISignalingTransport` 中加入账号鉴权、防重放、限流与 TLS/DTLS，或由现有后端
通过 WebSocket/HTTPS 转发相同的 opaque GNS 信令。TURN 凭证应短期签发，不能
把长期密钥写入客户端。

当前 P2P 拓扑仍沿用 AYNetwork 的 authority/listen-host 模型，并已支持席位恢复和
Host Migration。房间目录、跨分区仲裁和真正的无主机一致性协议不由信令转发器承担。

协议版本、Authority 模型和复制/RPC 阶段见 [design.md](design.md)。
