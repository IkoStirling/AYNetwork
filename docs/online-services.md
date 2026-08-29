# AYNetwork Online Services

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

`ILobbyService` 支持创建、metadata 精确筛选、凭据化加入、离开、读取、按 revision CAS
更新、创建限时/限次邀请，以及 `launchLobbyP2P`。Public Lobby 可被列出，Unlisted 只能
直接加入，Private 必须持邀请；密码使用 Argon2id verifier 保存，不保留明文。Owner 离开时
所有权确定性转给成员列表中的下一人；最后一人离开删除
Lobby，并向该次 leave 返回一个无成员的 `Closed` 墓碑，随后读取该 Lobby 得到 NotFound。
启动过程先把状态从 `Open` CAS 到 `Launching`，在锁外创建 P2P session 并为每个
成员签发 grant，成功后进入 `InSession`；失败会关闭已创建的 Host session 并重新开放
Lobby。启动期间不允许成员变化。

## Matchmaking

`IMatchmakingService` 接受单人、直接 Party 或 Lobby-backed Party ticket。Lobby-backed 请求会
在服务端重新读取 Lobby、校验 Owner/revision，并以规范成员表覆盖客户端输入；只有 Owner
可以代表 Party 入队。兼容性按 queue、region、build/content、topology、目标/最小人数、
virtual port、互相可接受的 ping 上限和技能容差计算。参考实现保持优先级/FIFO 顺序，不拆
Party，并以 Party 为单位做贪心队伍平衡。匹配 worker 通过显式 `runMatchmaking()` 驱动：

- `P2P`：最早 ticket 的首位玩家成为初始 Host，后端创建会话并给每个 ticket 只返回其
  party 成员自己的 grant。
- `Dedicated`：只从同 region/build、有活租约且非 draining 的 server 分配；无容量时
  ticket 保持排队。
- `Any`：优先 Dedicated，无可用容量时回退 P2P。

Ticket 在 `Matching` 状态不可取消，避免分配与取消竞态；只有 ticket party 成员可以读取
结果。启用 `requireAcceptance` 后，全部玩家确认前状态停在 `AwaitingAcceptance`，不会创建
P2P 会话或占用 Dedicated 容量；拒绝者被取消，其余票据重新排队，超时则整组重新排队。
`allowBackfill + minimumPlayers` 当前表示“达到最小人数后允许欠员开局”，尚不表示向已运行
比赛补入新玩家。

## Dedicated Server

`IDedicatedServerService` 支持 server 注册、凭证化 heartbeat、draining、注销、容量预留
和释放。注册返回独立 64 字符 server token；分配返回 address/port、allocation ID、短期
reservation master token、match ID、成员名单、逻辑内容描述与过期时间。master 只供后端
释放 allocation 和专服派生凭据；玩家调用 `getMatch()` 时会按认证 `PeerId` 得到独立的
admission token，同局其他 ticket 无法用自己的 token 冒充该玩家。专服可用自身 bearer 调用
`listServerAllocations()` 拉取被分配的比赛，而不需要获得 fleet control token。过期 server
lease 会连同其 allocation 一起 fencing；
draining server 不再接新局，仍有 allocation 时注销会进入排空并返回冲突，释放后可重试。

参考选择器优先选择占用比例最低的 server，再以 server ID 打破平局，结果可复现且不会
超卖声明容量。

### Headless Dedicated Runtime

`DedicatedServerRuntime` 把目录契约接到实际 Headless 游戏进程：启动时注册并监听游戏端口，
后台执行 heartbeat 与 allocation polling；新 allocation 到达后调用
`IDedicatedWorldHost::startAuthoritativeWorld()`，客户端连接时先校验有界 admission payload
中的 allocation ID、`PeerId` 和按玩家派生的 admission token，再把连接交给世界。最后一名成员断开后
停止世界并释放 allocation。SIGINT/SIGTERM 或应用调用 `beginDrain()` 时先标记 draining，
不再接受新分配，现有世界清空后注销并退出。

仓库提供可部署壳 `AYNetwork_DedicatedServer`：

```powershell
$env:AY_ONLINE_SERVER_TOKEN = "<fleet control token>"
$env:AY_ONLINE_BACKEND_TLS = "1" # 反向代理提供 HTTPS 时
.\AYNetwork_DedicatedServer.exe api.example.com 443 ds-sg-01 asia build-42 `
    198.51.100.20 7350 64
```

该工具中的 `ReferenceWorldHost` 只记录内容加载和玩家进入/离开，用于部署联调；实际游戏应
实现 `IDedicatedWorldHost`，按 `contentId/contentVersion/contentSeed` 调用自己的场景加载器、
创建 authority world，并在 `tickAuthoritativeWorlds()` 驱动模拟。客户端可直接使用
`NetworkDedicatedSessionConnector`，它会安装 assignment 的 admission payload 并建立 GNS
IP 连接。

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

Lobby-backed 匹配只能从 `InLobby` 由当前 Owner 发起。需要确认时状态进入
`AwaitingMatchAcceptance`，应用调用 `respondToMatch(true/false)`；接受人数、Party 人数、
本地接受状态和截止时间都可从协调器状态读取。

完整 Lobby 更新通过 `updateLobby()` 提交，协调器会覆盖 actor、Lobby ID 和 revision，避免
UI 伪造上下文。Owner 可异步调用 `createLobbyInvitation()`；成功 token 不进入状态事件，必须
用一次性的 `takeLobbyInvitation()` 显式取走并交给受邀玩家。

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

匹配确认期间流程状态为 `MatchAcceptance`。UI 可直接读取 ticket 状态、已接受/所需 Party
人数、本地是否已接受和过期时间，并调用 `respondToMatch()`；确认完成后才发布加载请求。

`leaveLobby()`、`cancelMatchmaking()`、`leaveSession()` 和 `returnToMainMenu()` 都进入统一
清理路径；已提交 assignment/活跃传输优先于可能残留的 queued ticket。`signOut()` 在资源
清理完成后才清空凭据。连接/加载和清理分别由 `loadingTimeoutMs`、`cleanupTimeoutMs` 限时；
清理超时会 fail-closed 并保留玩家 token，使应用仍有机会重试后端撤销。access token 续签
使用 `refreshCredentials()`，无需重建流程、HTTP 客户端或会话。

## HTTP 接入

`HttpOnlineServices` 实现三个客户端接口；同一组路由可选挂载到
`HttpP2PSessionServer`，因此 authority session 与在线 API 可以共享 TCP 端口、请求大小
限制、来源限流和无秘密审计。玩家请求只发送 bearer，服务端通过
`playerAuthenticator` 派生可信 `PeerId`，不会读取 body 中的 actor。直接提交成员列表的
Party 请求还必须通过 `partyAuthorizer`；Lobby-backed Party 则由服务端重新读取成员并校验
Owner/revision，不能由客户端伪造成员。

主要路由：

| 方法 | 路径 | 身份 |
|---|---|---|
| POST/GET | `/v1/lobbies` | Player Bearer |
| GET | `/v1/lobbies/{id}` | Player Bearer |
| POST | `/v1/lobbies/{id}/join|leave|update|launch-p2p` | Player Bearer |
| POST | `/v1/lobbies/{id}/invitations` | Player Bearer (Owner) |
| POST/GET | `/v1/matches`、`/v1/matches/{id}` | Player Bearer |
| POST | `/v1/matches/{id}/cancel|respond` | Player Bearer |
| POST | `/v1/matches/run` | Fleet control token |
| POST/GET | `/v1/dedicated/servers` | Fleet control token |
| POST | `/v1/dedicated/servers/{id}/heartbeat|drain|unregister` | Server Bearer |
| GET | `/v1/dedicated/servers/{id}/allocations` | Server Bearer |
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
后会获得自己的 server bearer。直接提交 Party 成员的 HTTP ticket 还需要应用安装
`partyAuthorizer`；参考 SessionServer 的安全默认值只允许单人或经 Lobby 后端验证的 Party，
避免客户端伪造队友。

## SQLite 持久化实现

`InMemoryOnlineServices` 是线程安全、有限容量的第一版参考业务实现，并真实接入现有 P2P
session backend。默认最多 64 人 Lobby/Match、4096 容量 Dedicated Server，防止单请求
产生无界 grant 响应。它适合引擎集成测试、单进程服务和规则原型。

`SqliteOnlineServices` 是可部署的同机持久实现：

- Lobby、成员、匹配 ticket/party、Dedicated server 和 allocation 使用规范化表；写操作
  运行在 `BEGIN IMMEDIATE` 事务中，容量预留不会超卖。
- schema v5 持久化 Lobby metadata/可见性/密码 verifier、邀请 token 摘要、匹配规则、
  接受状态、match ID、队伍 placement，以及 Dedicated allocation 的成员和逻辑内容；邀请和
  密码明文不会写入数据库。
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
$env:AY_SESSION_ADMISSION_PREVIOUS_TOKENS = "<轮换窗口内仍接受的旧 token，逗号分隔>"
$env:AY_SESSION_AUDIT_FILE = "D:\AYNetwork\audit.jsonl"
$env:AY_SESSION_HTTP_BIND = "127.0.0.1"
$env:AY_SESSION_SIGNALING_TRANSPORT = "websocket"
$env:AY_SESSION_METRICS_TOKEN = "<监控抓取 bearer>"

$env:AY_ONLINE_ENABLE = "1"
$env:AY_ONLINE_AUTH_KEY = "<账号服务与 SessionServer 共享的 64 hex HMAC key>"
$env:AY_ONLINE_AUTH_PREVIOUS_KEYS = "<旧 HMAC key，逗号分隔，最多 8 个>"
$env:AY_ONLINE_SERVER_TOKEN = "<至少 32 字符的 fleet/orchestrator token>"
$env:AY_ONLINE_SERVER_PREVIOUS_TOKENS = "<轮换窗口内的旧 fleet token，逗号分隔>"
```

生产 HTTP 必须监听回环并由反向代理终止 TLS。轮换顺序是先把旧 key/token 放入 previous
列表并部署新主密钥，再等待旧凭证最大寿命过去后移除 previous；列表只用于有界滚动窗口，
不是长期密钥仓库。不要把 `AY_ONLINE_AUTH_KEY` 或 fleet token 分发给游戏客户端。

## 扩展边界

HTTP server 只依赖 `ILobbyService`、`IMatchmakingService`、
`IDedicatedServerService`，没有依赖 SQLite，这就是应用后端替换边界。参考 SessionServer
在生产模式下默认要求持久 online state 和签名玩家凭证；
`AY_ONLINE_ALLOW_EPHEMERAL=1`、`AY_ONLINE_ALLOW_STATIC_CREDENTIALS=1` 仅用于 staging。
SQLite 的多进程能力限于同机和同一本地数据库；它不是跨主机生产数据库。多节点部署应在
这些接口后使用 PostgreSQL/MySQL 等事务存储和共享任务队列，保留 revision/epoch CAS、
allocation 容量事务与 claim 恢复语义。尚未内置外部技能评分源、真实跨区延迟探测、运行中
比赛回填、专服进程编排或云厂商 API。第一版技能和 ping 数值由可信应用后端提供；公网
客户端自报数据不能直接用于生产匹配。
