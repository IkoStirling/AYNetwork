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
Lobby。启动过程先把状态从 `Open` CAS 到 `Launching`，在锁外创建 P2P session 并为每个
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

参考 `AYNetwork_SessionServer` 可通过以下环境变量启用该 API：

```powershell
$env:AY_ONLINE_ENABLE = "1"
$env:AY_ONLINE_CREDENTIALS_FILE = "D:\AYNetwork\players.txt"
$env:AY_ONLINE_SERVER_TOKEN = "<至少 32 字符的 fleet/orchestrator token>"
```

凭证文件每行是 `<PeerId> <token>`，PeerId 与 token 都不可重复，token 至少 32 字符。
该文件仅是可执行 E2E 和受控联调使用的认证适配器；正式账号服务应直接设置
`playerAuthenticator` / `partyAuthorizer`。`AY_ONLINE_SERVER_TOKEN` 只用于受信 fleet
操作，Dedicated 实例注册后会获得自己的 server bearer。

## 当前实现与持久化边界

`InMemoryOnlineServices` 是线程安全、有限容量的第一版参考业务实现，并真实接入现有 P2P
session backend。默认最多 64 人 Lobby/Match、4096 容量 Dedicated Server，防止单请求
产生无界 grant 响应。它适合引擎集成测试、单进程服务和规则原型。

HTTP server 只依赖 `ILobbyService`、`IMatchmakingService`、
`IDedicatedServerService`，没有依赖内存实现，这就是持久化替换边界。参考 SessionServer
在生产模式下默认拒绝启用临时 online state；`AY_ONLINE_ALLOW_EPHEMERAL=1` 只允许 staging
明确放行。正式部署应注入数据库实现，并保持 ticket/allocation 状态、revision CAS 和
容量预留事务化。尚未内置玩家技能评分、跨区延迟测量、party 邀请/隐私、Dedicated
进程拉起或云厂商 API。
