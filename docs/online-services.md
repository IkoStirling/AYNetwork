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

## 当前实现与后续适配

`InMemoryOnlineServices` 是线程安全、有限容量的第一版参考实现，并真实接入现有 P2P
session backend。它适合引擎集成测试、单进程服务和规则原型；当前不包含 HTTP wire、
数据库 schema、玩家技能评分、跨区延迟测量、party 邀请/隐私、Dedicated 进程拉起或云
厂商 API。生产服务可分别实现三个接口，保持引擎侧 Lobby/匹配流程与 GNS 传输解耦。
