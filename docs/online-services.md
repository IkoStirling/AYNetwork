# AYNetwork Online Services v1

## 定位

`OnlineServices.h` 定义 Lobby、Matchmaking 与 Dedicated Server 三组后端中立契约。
它们位于实时传输之上：Lobby 管组队与发现，Matchmaking 决定把哪些玩家放进同一局，
Dedicated Server 目录管理可分配进程；最终的 P2P 实时会话仍由
`IP2PSessionService` / `P2PSessionCoordinator` 建立。

```text
账号/平台认证
      │（可信 PeerId）
Lobby / Matchmaking / Dedicated Directory
      │
      ├─ P2P assignment ──> IP2PSessionService ──> P2PSessionCoordinator
      └─ Dedicated assignment ──> address + reservation token
```

接口假定调用方已经认证，任何 HTTP/RPC 适配器都必须从认证上下文取得 `PeerId`，不能
相信请求 body 自报的身份。

## Lobby

`ILobbyService` 支持创建、筛选、加入、离开、读取、按 revision CAS 更新，以及
`launchLobbyP2P`。Owner 离开时所有权确定性转给成员列表中的下一人；最后一人离开删除
Lobby，并向该次 leave 返回一个无成员的 `Closed` 墓碑，随后读取该 Lobby 得到 NotFound。
启动过程先把状态从 `Open` CAS 到 `Launching`，在锁外创建 P2P session 并为每个
成员签发 grant，成功后进入 `InSession`；失败会关闭已创建的 Host session 并重新开放
Lobby。启动期间不允许成员变化。

## Matchmaking

`IMatchmakingService` 接受单人或 party ticket，按 queue、region、build、topology、目标
人数和 virtual port 精确分组。参考实现保持入队顺序，以完整 party 为最小单位填满一局，
不会拆 party。匹配 worker 通过显式 `runMatchmaking()` 驱动：

- `P2P`：最早 ticket 的首位玩家成为初始 Host，后端创建会话并给每个 ticket 只返回其
  party 成员自己的 grant。
- `Dedicated`：只从同 region/build、有活租约且非 draining 的 server 分配；无容量时
  ticket 保持排队。
- `Any`：优先 Dedicated，无可用容量时回退 P2P。

Ticket 在 `Matching` 状态不可取消，避免分配与取消竞态；只有 ticket party 成员可以读取
结果。

## Dedicated Server

`IDedicatedServerService` 支持 server 注册、凭证化 heartbeat、draining、注销、容量预留
和释放。注册返回独立 64 字符 server token；分配返回 address/port、allocation ID、短期
reservation token 与过期时间。过期 server lease 会连同其 allocation 一起 fencing；
draining server 不再接新局，仍有 allocation 时注销会进入排空并返回冲突，释放后可重试。

参考选择器优先选择占用比例最低的 server，再以 server ID 打破平局，结果可复现且不会
超卖声明容量。

## 游戏侧 OnlineSessionCoordinator

`OnlineSessionCoordinator` 把上述后端接口收束成游戏循环可驱动的状态机。Lobby 的创建、
发现、加入、刷新、CAS 更新、离开和启动，以及 Matchmaking 的入队、轮询、取消与 assignment
接续均不会在调用线程执行 HTTP；游戏每帧调用一次 `update()` 消费完成结果即可。

```cpp
#include <AYNetwork/Session/OnlineSessionCoordinator.h>

P2PSessionCoordinator p2p(network, sessionService, p2pConfig);
OnlineSessionCoordinatorConfig onlineConfig;
onlineConfig.localPeerId = localPeer;
OnlineSessionCoordinator online(
    lobbyService, matchmakingService, p2p,
    std::move(onlineConfig), dedicatedConnector);

MatchmakingRequest request;
request.queue = "default";
request.region = "asia";
request.buildId = buildId;
request.topology = MatchTopology::Any;
request.targetPlayers = 2;
request.virtualPort = 7350;
online.startMatchmaking(std::move(request)); // 空 party 自动成为 solo ticket

// regular application/network loop
online.update();
const auto status = online.getStatus();
```

匹配完成后，P2P assignment 中属于本地 `PeerId` 的现成 grant 会直接交给
`P2PSessionCoordinator::startAssignedSession()`，不会再次调用后端 join、重复占用席位或重新
签票。初始 Host 由 grant 的后端 Host 身份决定，而不是由客户端猜测。

Dedicated assignment 通过 `IDedicatedSessionConnector` 交给应用。connector 必须先安装完整
`DedicatedAllocation`（包括短期 reservation token），再启动 IP 连接和专用服准入握手；
AYNetwork 不把某一种游戏服认证协议硬编码进通用 `connect()`。connector 的 `update()` 和
`getStatus()` 必须非阻塞。reservation token 属于秘密，不应写入普通日志或遥测。

协调器保留“取消意图”：即使 enqueue/poll 正在进行，也会在 ticket id 到达后继续取消。
若取消与 assignment 提交竞争并返回 Conflict，协调器会重新读取 canonical ticket；已经提交
的 assignment 不会被静默丢弃。连续临时轮询错误按配置重试，终止后仍可调用
`cancelMatchmaking()` 清理活 ticket。`leaveSession()` 会先离开 P2P 或断开 Dedicated，
Lobby 启动的会话随后再撤销 Lobby membership。

## 引擎级 OnlineSubSystem

普通引擎应用不需要自行持有两个协调器。`IOnlineSubSystem` 是 GameLoop facade：它在
`Ingress` 阶段运行，严格要求 `Network` 先初始化并先更新；默认自动创建
`HttpP2PSessionService` 与 `HttpOnlineServices`，把大厅、匹配、P2P 会话和 Dedicated
connector 收束成一个生命周期。

```cpp
#include <AYNetwork/Session/OnlineSubSystem.h>

OnlineSubSystemConfig config;
config.localPeerId = PeerId{"account:player-42"};
config.backend.serverAddress = "api.example.internal";
config.backend.serverPort = 8080;
config.p2p.p2p.icePolicy = P2PIcePolicy::DirectOnly;
config.p2p.p2p.allowPrivateCandidates = true;

// 必须在 preparePlaySession()/run() 前注册。
registerOnlineSubSystem(std::move(config));

// 账号登录或续签完成后可轮换，不需要重建 HTTP 客户端。
auto* online = findRegisteredOnlineSubSystem();
online->setPlayerAccessToken(accountAccessToken);
online->listLobbies({"asia", buildId, 1, 100});
```

若平台 SDK、应用后端或测试已经实现三个后端接口，可通过
`OnlineSubSystemDependencies` 同时注入 `IP2PSessionService`、`ILobbyService` 和
`IMatchmakingService`；部分注入会被拒绝，避免同一会话混用两套 authority。可选的
`IDedicatedSessionConnector` 可与默认 HTTP 后端一起使用。

玩家 bearer 和 P2P admission token 都由线程安全 provider 在每次请求时读取，支持运行时
轮换。存在 Lobby、匹配票据或会话时不允许清空玩家 bearer，防止失去清理资源所需的身份；
token 不进入状态对象、事件或普通日志。`shutdown()` 会在
`gracefulShutdownTimeoutMs` 的有界窗口内尝试取消匹配并离开会话/Lobby。

应用可订阅 `OnlineSessionStatusChangedEvent` 和 `OnlineLobbyListChangedEvent`。两者都是
可安全排队的平凡 payload，只携带状态、ID、epoch 和计数；详细 Lobby 列表通过
`getLobbyResults()` 拉取，并用 `getLobbyListGeneration()` 判断版本。命令受理状态会立即
排队，完成状态由后续 GameLoop update 排队，因此即使本地/快速后端在一帧内完成也不会
漏掉列表代次。

## 应用在线流程

`OnlineFlowCoordinator` 位于 UI/关卡系统与 `IOnlineSubSystem` 之间。它不实现账号登录，
而是接收账号服务已经签发的短期 token，然后统一表示登录页、主菜单、大厅浏览、房间、
匹配、会话加载、游戏中、清理和失败状态。应用每帧应先更新 Online subsystem，再调用
`OnlineFlowCoordinator::update()`。

```cpp
#include <AYNetwork/Session/OnlineFlowCoordinator.h>

auto* online = findRegisteredOnlineSubSystem();
OnlineFlowCoordinator flow(*online);

// 账号服务或平台适配器完成登录后：
flow.signIn(playerAccessToken, p2pAdmissionToken);
flow.browseLobbies({"asia", buildId, 1, 100});

// UI 发起匹配；assignment 到达后流程发布加载事件。
flow.startMatchmaking(matchRequest);
const auto loading = flow.getStatus();

// 场景异步加载回调必须带回同一个 generation。
flow.completeLoading(loading.loadingGeneration);
```

`OnlineFlowLoadRequestedEvent` 只携带 topology、Lobby/ticket/session/allocation ID 和加载
generation，不携带 bearer、信令 token 或 Dedicated reservation token。若场景加载先于
网络完成，流程继续停留在 `LoadingSession`；若网络先完成，也会等待场景确认。旧回调的
generation 不匹配时会被拒绝，避免取消后重匹配时误进入旧世界。

`leaveLobby()`、`cancelMatchmaking()`、`leaveSession()` 和 `returnToMainMenu()` 都进入统一
清理路径；已提交 assignment/活跃传输优先于可能残留的 queued ticket。`signOut()` 在资源
清理完成后才清空凭据。连接/加载和清理分别由 `loadingTimeoutMs`、`cleanupTimeoutMs` 限时；
清理超时会 fail-closed 并保留玩家 token，使应用仍有机会重试后端撤销。access token 续签
使用 `refreshCredentials()`，无需重建流程、HTTP 客户端或会话。

## HTTP 接入

`HttpOnlineServices` 实现三个客户端接口；同一组路由可选挂载到
`HttpP2PSessionServer`，因此 authority session 与在线 API 可以共享 TCP 端口、请求大小
限制、来源限流和无秘密审计。玩家请求只发送 bearer，服务端通过
`playerAuthenticator` 派生可信 `PeerId`，不会读取 body 中的 actor。Party 请求还必须通过
`partyAuthorizer`；未安装 party 认证器时只允许单人 ticket，防止客户端冒充队友。

主要路由：

| 方法 | 路径 | 身份 |
|---|---|---|
| POST/GET | `/v1/lobbies` | Player Bearer |
| GET | `/v1/lobbies/{id}` | Player Bearer |
| POST | `/v1/lobbies/{id}/join|leave|update|launch-p2p` | Player Bearer |
| POST/GET | `/v1/matches`、`/v1/matches/{id}` | Player Bearer |
| POST | `/v1/matches/{id}/cancel` | Player Bearer |
| POST | `/v1/matches/run` | Fleet control token |
| POST/GET | `/v1/dedicated/servers` | Fleet control token |
| POST | `/v1/dedicated/servers/{id}/heartbeat|drain|unregister` | Server Bearer |
| POST | `/v1/dedicated/allocations` | Fleet control token |
| POST | `/v1/dedicated/allocations/{id}/release` | Reservation Bearer |

参考 `AYNetwork_SessionServer` 可通过以下环境变量启用开发模式 API：

```powershell
$env:AY_ONLINE_ENABLE = "1"
$env:AY_ONLINE_CREDENTIALS_FILE = "D:\AYNetwork\players.txt"
$env:AY_ONLINE_SERVER_TOKEN = "<至少 32 字符的 fleet/orchestrator token>"
```

凭证文件每行是 `<PeerId> <token>`，PeerId 与 token 都不可重复，token 至少 32 字符。
该文件仅供 E2E 和受控联调使用。生产模式使用后端中立的 HMAC 玩家凭证：账号服务调用
`issuePlayerAccessToken()` 签发短期 bearer，SessionServer 通过 `AY_ONLINE_AUTH_KEY` 验证并
取得可信 `PeerId`。它不依赖 Steam 或其他平台账号；平台登录、封禁和 refresh token 仍由
应用账号服务负责。`AY_ONLINE_SERVER_TOKEN` 只用于受信 fleet 操作，Dedicated 实例注册
后会获得自己的 server bearer。HTTP party ticket 还需要应用安装 `partyAuthorizer`；参考
SessionServer 的安全默认值只允许单人 ticket，避免客户端伪造队友。

## SQLite 持久化实现

`InMemoryOnlineServices` 是线程安全、有限容量的第一版参考业务实现，并真实接入现有 P2P
session backend。默认最多 64 人 Lobby/Match、4096 容量 Dedicated Server，防止单请求
产生无界 grant 响应。它适合引擎集成测试、单进程服务和规则原型。

`SqliteOnlineServices` 是可部署的同机持久实现：

- Lobby、成员、匹配 ticket/party、Dedicated server 和 allocation 使用规范化表；写操作
  运行在 `BEGIN IMMEDIATE` 事务中，容量预留不会超卖。
- WAL、`synchronous=FULL` 和 schema version 门禁保证重启恢复并拒绝未知格式；数据库必须
  位于本地磁盘，不能放 SMB/NFS 共享目录。
- Lobby launch 与 matchmaking 使用带过期时间的持久 claim，多进程 worker 只会有一个
  提交外部 P2P/Dedicated 副作用；进程崩溃后 claim 会回到可执行状态。
- server token、allocation token 和包含 Join Ticket/信令 token 的匹配 assignment 使用
  XChaCha20-Poly1305 静态加密；错误的 storage key 会在启动时被拒绝。
- server lease、allocation 和已完成 ticket 都有回收期限。默认已完成 ticket 保留 24 小时，
  使客户端可在服务重启后继续轮询结果。

外部 P2P 会话创建与 SQLite 提交不是分布式原子事务。若进程恰好在 P2P 创建成功后、结果
提交前崩溃，claim 会恢复，但旧 P2P 会话可能存活到自身 lease 过期；它没有可取回的 assignment，
不能授权新玩家。跨主机部署或需要严格 outbox/补偿审计时，应在三个服务接口后接应用事务
数据库和任务队列。

生产参考配置如下；`AY_ONLINE_DB` 未设置时会复用 `AY_SESSION_DB`：

```powershell
$env:AY_SESSION_PRODUCTION = "1"
$env:AY_SESSION_DB = "D:\AYNetwork\state.sqlite3"
$env:AY_SESSION_STATE_KEY = "<64 个 hex 字符；同时加密 session/online state>"
$env:AY_SESSION_TICKET_KEY_FILE = "D:\AYNetwork\ticket-signing.key"
$env:AY_SESSION_ADMISSION_TOKEN = "<至少 32 字符>"
$env:AY_SESSION_AUDIT_FILE = "D:\AYNetwork\audit.jsonl"
$env:AY_SESSION_HTTP_BIND = "127.0.0.1"

$env:AY_ONLINE_ENABLE = "1"
$env:AY_ONLINE_AUTH_KEY = "<账号服务与 SessionServer 共享的 64 hex HMAC key>"
$env:AY_ONLINE_SERVER_TOKEN = "<至少 32 字符的 fleet/orchestrator token>"
```

生产 HTTP 必须监听回环并由反向代理终止 TLS。当前 token verifier 接受单一 signing key；
轮换时应先滚动部署支持旧/新 key 的应用认证适配器，或在短 token 生命周期后切换。不要把
`AY_ONLINE_AUTH_KEY` 或 fleet token 分发给游戏客户端。

## 扩展边界

HTTP server 只依赖 `ILobbyService`、`IMatchmakingService`、
`IDedicatedServerService`，没有依赖 SQLite，这就是应用后端替换边界。参考 SessionServer
在生产模式下默认要求持久 online state 和签名玩家凭证；
`AY_ONLINE_ALLOW_EPHEMERAL=1`、`AY_ONLINE_ALLOW_STATIC_CREDENTIALS=1` 仅用于 staging。
尚未内置玩家技能评分、跨区延迟测量、party 邀请/隐私、Dedicated 进程拉起或云厂商 API。
