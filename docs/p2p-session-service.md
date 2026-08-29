# AYNetwork P2P 会话后端 v1

## 1. 边界

会话后端负责“谁属于哪个会话、当前谁是 Host、当前 authority epoch 是多少”。它不
转发游戏消息，也不参与复制、RPC、预测或回放。

参考部署把两个逻辑服务放进同一进程：

1. TCP/HTTP 会话 API：创建/加入、成员凭证、Host lease、epoch CAS。
2. 鉴权信令：生产默认在同一 HTTPS 入口使用 WebSocket；开发模式可选 UDP。两者都只转发
   GNS 建连所需的 opaque signaling blob。

GNS 建立 ICE 路径后，游戏消息直接在玩家间传输，不经过 SessionServer。

## 2. 构建与运行

打开 `AYNETWORK_BUILD_SESSION_SERVER`（打开
`AYNETWORK_BUILD_SIGNALING_SERVER` 时默认一并打开），构建以下目标：

- `AYNetwork_SessionServer`
- `AYNetwork_SessionProbe`

服务器命令：

```powershell
.\AYNetwork_SessionServer.exe 0.0.0.0 18080 203.0.113.10 28080
```

开发默认需要开放 TCP 18080 和 UDP 28080。第四个参数是 UDP 模式的监听端口；第三个参数
是写入客户端 grant 的公网信令地址，可以是公网 IP 或域名。

开发模式默认使用内存目录。生产模式显式启用 SQLite WAL、持久签名身份、静态准入
认证、HTTP 限流和 JSONL 审计；HTTP 应只监听回环地址并由反向代理终止 TLS：

```powershell
$env:AY_SESSION_PRODUCTION = "1"
$env:AY_SESSION_DB = "D:\AYNetwork\state\sessions.db"
$env:AY_SESSION_STATE_KEY = "<64 个十六进制字符，来自 secret manager>"
$env:AY_SESSION_TICKET_KEY_FILE = "D:\AYNetwork\secrets\ticket.key"
$env:AY_SESSION_ADMISSION_TOKEN = "<至少 32 字符的准入秘密>"
$env:AY_SESSION_AUDIT_FILE = "D:\AYNetwork\logs\session-audit.jsonl"
$env:AY_SESSION_HTTP_BIND = "127.0.0.1"
$env:AY_SESSION_SIGNALING_TRANSPORT = "websocket"
.\AYNetwork_SessionServer.exe 127.0.0.1 18080 wss://session.example.com/v1/signaling 28080
```

生产模式未显式设置 `AY_SESSION_SIGNALING_TRANSPORT` 时默认 `websocket`；HTTP API、
`/v1/signaling` WebSocket 和在线服务路由共享同一端口，由 Caddy/nginx/IIS 终止 TLS。
开发默认仍为 `udp`，便于与早期公网打洞探针兼容。

`AY_SESSION_RATE_PER_MINUTE` 与 `AY_SESSION_RATE_BURST` 必须成对设置；生产默认分别为
600 和 100。`AY_SESSION_ALLOW_PUBLIC_PLAINTEXT=1` 只用于明确接受风险的隔离测试，
不应出现在公网部署。

端到端探针：

```powershell
.\AYNetwork_SessionProbe.exe 203.0.113.10 18080
```

成功输出 `AY_SESSION_RESULT state=passed ...`。探针会真实走 HTTP，并用服务签发的
两份独立信令凭证完成所选信令传输的注册和转发。

## 3. HTTP 契约

所有响应均为：

```json
{"ok":true,"value":{}}
```

或：

```json
{"ok":false,"error":"epoch_conflict","message":"authority epoch changed"}
```

| 方法 | 路径 | 鉴权 | 作用 |
|---|---|---|---|
| POST | `/v1/sessions` | `X-AY-Admission-Token`（生产） | 创建会话并返回 Host grant |
| POST | `/v1/sessions/{id}/join` | `X-AY-Admission-Token`（生产） | 加入并返回该成员自己的 grant |
| POST | `/v1/sessions/{id}/heartbeat` | Member Bearer | 当前 Host 按 expected epoch 续租 |
| POST | `/v1/sessions/{id}/claim-host` | Member Bearer | 优雅转移或租约过期后的 self-claim |
| POST | `/v1/sessions/{id}/leave` | Member Bearer | 成员离开；有效 Host 离开则结束会话 |
| GET | `/v1/sessions/{id}` | v1 无 | 读取不含秘密的会话状态 |
| GET | `/livez` | 无 | 进程存活探针 |
| GET | `/readyz` | 无 | 接流量/排空状态 |
| GET | `/metrics` | 可选 Bearer | Prometheus 文本指标 |
| WS | `/v1/signaling` | Peer/room/signaling Bearer | 房间内 opaque 信令转发 |

Member Bearer 是 create/join 返回的 64 字符成员 token，只用于 HTTP 会话操作。安全 UDP
信令 token 是另一份独立秘密，二者不能互换。Host claim 只返回公开会话状态，不返回
目标成员的 token。

HTTP 429 映射为 `SessionServiceError::RateLimited`，协调器和 Host lease keeper 将其视为
可重试故障。审计事件只含时间、方法、路径、远端地址与状态码，不记录请求体、Admission
Token、Member Bearer、信令 token 或 Join Ticket。

## 4. 客户端接入

推荐由 `P2PSessionCoordinator` 统一编排 create/join、grant 安装、GNS 启动、Host lease、
离开和 Host Migration 的后端 epoch 授权：

```cpp
#include <AYNetwork/Session/HttpSessionService.h>
#include <AYNetwork/Session/P2PSessionCoordinator.h>

auto sessions = std::make_shared<ayt::net::HttpP2PSessionService>(
    ayt::net::HttpP2PSessionClientConfig{
        .serverAddress = "203.0.113.10",
        .serverPort = 18080,
    });

ayt::net::P2PSessionCoordinatorConfig config;
config.p2p.localPeerId = ayt::net::PeerId{"account-42"};
config.p2p.virtualPort = 7350; // Host 创建房间时使用；join 后以后端 grant 为准
config.p2p.icePolicy = ayt::net::P2PIcePolicy::DirectOnly;
config.p2p.stunServers = {"stun.example.net:3478"};
config.p2p.allowPrivateCandidates = false;

network->initialize();
ayt::net::P2PSessionCoordinator session(*network, sessions, config);

// Host：
session.createSession();
// Client 改为：session.joinSession(sessionId);

// 常规主循环；这两个调用都不执行 HTTP 阻塞 I/O。
network->update(deltaSeconds);
session.update();

const auto status = session.getStatus();
// Hosting / Connecting / Active / Migrating / Failed
```

应用在退出房间时调用 `session.leaveSession()`，继续 pump 到 `Idle`；若只是本地故障恢复，
可在 `Failed` 后调用 `reset()`，但它不会代替后端 leave。协调器拥有该本地 P2P 会话，
析构会取消迁移授权任务、停止 lease 并断开网络；`INetworkSubSystem` 和 service 必须比它
活得更久。低层的 `applyP2PSessionGrant()`、validator、Join Ticket 与
`P2PSessionLeaseKeeper` 仍公开，供需要自定义大厅状态机的项目分别编排。

## 5. Host Migration 与 epoch

会话后端只接受精确的 `expectedEpoch`：

- 有效租约内，只有当前 Host 能转移给另一个已入房成员。
- 租约过期后，旧 Host 不再能转移或删除会话。
- 过期后，每个成员只能为自己 claim；并发候选中只有一个 CAS 成功。
- 成功后 epoch 恰好加一，旧 epoch 的 heartbeat、claim 和 Join Ticket 全部失效。

`P2PSessionCoordinator` 把 CAS 接入 AYNetwork 的非阻塞 authority gate：

- 优雅迁移在所有 Prepare ACK 到齐后冻结于 `AwaitingAuthority`；旧 Host 的工作线程先
  向后端 transfer/CAS，成功后 AYNetwork 才发送 Commit。
- 崩溃迁移在 Promotion 前冻结；确定性候选等待旧 lease 过期后 self-claim，其他成员只
  观察后端 epoch。并发候选即使产生不同判断，也只有一次 CAS 能成功。
- HTTP 传输错误按有界间隔重试；拒绝和超时 fail closed，不推进本地 engine epoch。
- 新 Host 获批后启动新 epoch lease，旧 Host 停止续租。

后端 CAS 与 AYNetwork Prepare/Commit 仍不可能成为跨进程原子事务。例如后端 CAS 已成功
而旧 Host 在发送 Commit 前崩溃，幸存者需要读取后端权威状态并重新加入。因此仍必须遵守：

1. 后端返回的 epoch 是最终权威，不能回退。
2. 后端目标 Host 必须与 AYNetwork 确定性选举结果相同。
3. 任一事务失败或结果不一致时，关闭旧会话并按后端状态重新加入；不要让旧 Host 继续
   提供权威模拟。
4. 未计划迁移只有在候选成功 self-claim 后才会进入 Promotion；应用也不应提前把它视为
   正式权威。

## 6. 票据与默认时限

- Host lease：10 秒。
- Host heartbeat：3 秒（keeper 默认值）。
- Join Ticket：60 秒，Ed25519 签名，绑定 session ID、epoch、PeerId、签发/过期时间和
  随机 nonce；最大 1024 字节。
- 安全信令 token：24 小时；服务端默认每 5 秒回查成员目录，因此成员离开后不必等待
  24 小时才撤销。

## 7. 持久化与部署边界

`SqliteP2PSessionService` 使用 WAL、`synchronous=FULL` 与 `BEGIN IMMEDIATE`。所有 Host
claim 都在事务内按精确 epoch CAS，因此同一台机器上共享数据库的多个进程最多一个能
提交。Member 与信令 token 以 ChaCha20-Poly1305 加密后存盘；数据库同时绑定 storage
key 校验值与 Ed25519 公钥，使用错误密钥或错误签名身份会拒绝启动。签名 key 文件采用
版本化定长格式并通过临时文件原子安装，重启后既有 Join Ticket 验证身份保持不变。

以下仍是部署层责任：

- 内置服务监听明文回环；公网入口必须由 Caddy、nginx、IIS 等反向代理提供 HTTPS/WSS、
  证书、请求大小/超时策略和真实客户端地址治理。客户端配置 `useTls=true` 后使用 HTTPS，
  grant 中的信令地址应是 `wss://.../v1/signaling`。
- 内置 Admission Token 是部署级最小门禁，不是玩家账号系统。正式项目应在代理或自定义
  `IP2PSessionService` 适配器中验证账号 JWT/平台票据，并把认证后的 `PeerId` 传入服务。
- SQLite WAL 只支持同机多进程，不支持网络共享盘或跨主机多活。跨主机部署需用具备事务
  CAS 的 PostgreSQL/MySQL 等后端实现同一接口。
- `AY_SESSION_ADMISSION_PREVIOUS_TOKENS`、`AY_ONLINE_AUTH_PREVIOUS_KEYS` 和
  `AY_ONLINE_SERVER_PREVIOUS_TOKENS` 支持滚动密钥窗口；旧凭证最大寿命过去后必须移除。
- `/metrics` 暴露 HTTP 状态码、限流/封禁和 WebSocket 连接/转发计数；
  `AY_SESSION_METRICS_TOKEN` 可保护抓取。`AY_SESSION_BLOCKLIST_FILE` 每行一个精确来源地址。
  SIGINT/SIGTERM 会先令 `/readyz` 返回 503，并按 `AY_SESSION_DRAIN_SECONDS` 排空后退出。
- 告警规则、日志收集、证书自动续期、跨区复制和自动备份恢复属于部署平台责任。
- TURN 仍是独立 ICE relay 服务，与 SessionServer 持久化无关。

故障矩阵覆盖总分区、慢后端不阻塞网络线程、CAS 已提交但响应丢失的重读收敛、leave
传输丢失后的凭证保留，以及旧 Host 分区恢复后被新 epoch fencing。SQLite 测试另覆盖
重启、密文存储、双实例并发 CAS 和错误密钥拒绝。

同机可运行多个 SessionServer 共享 SQLite WAL，但不能把数据库放到 SMB/NFS，也不能据此
宣称跨主机多活。跨主机必须用实现同一接口的事务数据库后端。当前 WebSocket peer 目录是
进程内状态：多实例时负载均衡器必须按 signaling room 一致路由，使同房成员落到同一实例；
若无法提供 room affinity，则应把转发层替换为共享 broker。仅有普通连接粘性并不足够。
