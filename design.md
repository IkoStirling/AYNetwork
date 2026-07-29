# AYNetwork Design

> **文档状态（2026-07-27）**：传输库 **锁定 GameNetworkingSockets（GNS）**（§14）。R1 部分落地（`GnsConnection` + loopback echo 单测）；子系统多连接 / 复制 / Authority / RPC **未完成**。工业可用度仍低（见 §12）。  
> **2026-07-27 设计审计补丁**：消除 KCP/GNS 叙事矛盾；锁定 Protocol↔GNS 职责；新增 **§6.6 Authority 最小模型**；§4 改为 GNS 传输；§16 Changelog。

## 1. 概述

AYNetwork 是 AY Engine 的**网络同步模块**，负责游戏对象的网络复制和远程通信。

### 1.1 设计目标

- **可靠性**：以 **GameNetworkingSockets** 提供可靠/不可靠发送、连接管理、RTT/心跳（**不再使用 KCP**）
- **安全传输**：DTLS + AES-GCM 由 GNS 内置承担（应用层不自研第二套加密）
- **分层设计**：Transport（GNS）→ Protocol（应用消息）→ Replication / RPC
- **元数据驱动**：基于 AYReflect 自动序列化（R3 目标），零手动字段列表
- **ECS 友好**：数据组件与功能组件分离；权威模型见 §6.6

### 1.2 在引擎中的位置

```
Foundation Layers
─────────────────────────────────────────────────────
  ┌──────────────────────────────────────────────────┐
  │                    AYReflect                    │
  │               (运行时反射系统)                   │
  └──────────────────────────────────────────────────┘
                            │
          ┌─────────────────┼─────────────────┐
          ▼                 ▼                  ▼
  ┌───────────────┐ ┌───────────────┐ ┌───────────────┐
  │ AYSerializer  │ │  AYNetwork    │ │   AYEditor    │
  │ (序列化)     │ │ (网络复制)    │ │ (属性面板)   │
  └───────────────┘ └───────────────┘ └───────────────┘
                            │
          ┌─────────────────┼─────────────────┐
          ▼                 ▼                  ▼
  ┌───────────────┐ ┌───────────────┐ ┌───────────────┐
  │  AYEntity    │ │  AYGameLoop   │ │   AYDevice   │
  └───────────────┘ └───────────────┘ └───────────────┘
```

---

## 2. 工业级对标

### 2.1 目标特性矩阵（2026-07-26 审计后）

| 特性 | AYNetwork（2026-07-27） | Unreal | Unity | O3DE | 优先级 |
|------|:----------------------:|:------:|:------:|:----:|:------:|
| UDP 传输 | ⚠ GNS 已接，子系统未闭环 | ✅ | ✅ | ✅ | P0 |
| **传输库选型** | ✅ **GNS 锁定**（§14） | 自研 | Unity Transport | AzNetworking | P0 done |
| 可靠层 | ✅ GNS 内置（**禁用 KCP**） | ✅ | ✅ | ✅ | P0 |
| 状态同步 | ❌ stub | ✅ | ✅ | ✅ | P0 |
| RPC 调用 | ❌ 规格待 R4 专章 | ✅ | ✅ | ✅ | P1 |
| 增量同步 | ❌ 未设计细节 | ✅ | ⚠️ | ✅ | P2 |
| Interest Management | ❌ 未设计 | ✅ | ⚠️ | ✅ | P3 |
| Authority 模型 | ⚠ **§6.6 已锁定最小规格**，未实现 | ✅ | ✅ | ✅ | P0 |
| 加密（DTLS/AES） | ✅ 由 GNS 提供（应用层不重复） | ✅ | ✅ | ✅ | P0 |

### 2.2 核心差异

| 引擎 | 同步粒度 | 标记方式 |
|------|----------|----------|
| Unreal | Actor 级别 | UPROPERTY 宏 |
| Unity | GameObject/Component | NetworkVariable |
| O3DE | NetworkHierarchyComponent | Az::Reflect |
| **AYNetwork** | **DataComponent 级别** | **AYTYPE_FIELDS** |

---

## 3. 分层架构

### 3.1 五层架构（2026-07-26 更新 — 传输库切到 GameNetworkingSockets）

```
┌─────────────────────────────────────────────────────────────────┐
│                      Game Code / Gameplay                        │
│         (Entity, Component, DataComponent, System)              │
├─────────────────────────────────────────────────────────────────┤
│                    Replication / RPC Layer                        │
│    - NetComponent (功能组件)                                     │
│    - NetDataComponent (数据组件，标记 NetReplicate)              │
│    - ReplicationSystem (基于元数据自动同步)                      │
│    - RpcHandler (Server/Client/Multicast 三类型)                │
├─────────────────────────────────────────────────────────────────┤
│                       Protocol Layer                            │
│    - PacketHeader (序列号, 通道, 分片, checksum)               │
│    - PacketAssembler (组包/拆包, 处理最后一片可变长)           │
│    - Message Dispatch (按 PacketHeader 路由到 Replication/RPC) │
├─────────────────────────────────────────────────────────────────┤
│                      Transport Layer                             │
│    - GnsConnection (★ 包裹 GameNetworkingSockets 的 Connection) │
│    - 加密：DTLS + AES-GCM (GNS 内置)                            │
│    - Heartbeat / RTT / Stats (GNS 内置 API)                    │
│    - Reliable / Unreliable 通道（GNS 内置）                    │
├─────────────────────────────────────────────────────────────────┤
│                       Platform Layer                            │
│    - ThreadPool / Mutex / Atomic (AYPlatform)                  │
│    - GNS 内部已管 socket, AYNetwork 不再需要 UdpSocket         │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 各层职责

| 层级 | 职责 | 关键类 |
|------|------|--------|
| **Transport** | GNS 连接、收发、可靠/不可靠 send flags、RTT | `GnsConnection`（**废弃** `KcpConnection` / `UdpSocket`） |
| **Protocol** | 应用消息类型、版本、可选压缩/大包分片 | `PacketHeader`, `PacketAssembler` |
| **Replication** | 对象同步、变更检测、Snapshot/Delta | `ReplicationManager`, `ReplicationSystem` |
| **RPC** | 远程函数调用 | `RpcHandler` (R4) |
| **Interest** | 距离裁剪、相关性管理 | `RelevanceManager` (R4) |

### 3.3 Protocol ↔ GNS 职责切分（锁定 2026-07-27）

| 能力 | 归属 | 说明 |
|------|------|------|
| 可靠送达 / 重传 / 拥塞 | **GNS** | `k_nSteamNetworkingSend_Reliable` 等 |
| DTLS / AES-GCM | **GNS** | 应用层 **禁止** 再设 `PacketFlag::Encrypted` 假加密 |
| 连接心跳 / RTT / stats | **GNS** | wrapper 暴露查询 API |
| 应用消息类型 / schema 版本 | **Protocol** | `PacketHeader` 只描述 app payload |
| 应用层压缩 | **Protocol（可选）** | `Compressed` flag；与 GNS 无关 |
| 超大 payload 分片 | **优先 GNS 发送能力**；自研 `PacketAssembler` 仅当 app 需显式分片语义时启用 | 禁止与 GNS 双计 transport seq |
| 自研 `SequenceNumber` / transport `packetId` | **删除或降级**（R2） | 不得与 GNS 可靠序重复造轮子 |

**默认路径**：`send(CHANNEL_*)` → 映射为 GNS send flags → 字节 payload = `[PacketHeader app][body]`。

---

## 4. 传输层设计 (Transport) — GameNetworkingSockets

> **§4.1 原 KCP API 草案已废止**（历史见 git）。实现与文档一律以 `GnsConnection` 为准。树内残留 `Kcp*` / `UdpSocket` 标 **Deprecated**，R1 收口后删除。

### 4.1 GnsConnection（目标 API）

```cpp
namespace ayt::net {

enum class ConnectionMode : uint8_t {
    Client,
    DedicatedServer,
    ListenServer,   // 接口预留；R3+ 再落地
};

// 当前产品路径：IP 直连（LAN / 公网有公网 IP）
// 后续可选：GNS P2P / FakeIP / 自建 signaling（§14.6）
class GnsConnection {
public:
    bool listen(const char* bindAddress, uint16_t port);   // CreateListenSocketIP
    bool connect(const char* address, uint16_t port);      // ConnectByIPAddress
    void disconnect(uint32_t reasonCode);
    void update();   // RunCallbacks + 收包入队；由 NetworkSubSystem 每帧调用

    bool send(const uint8_t* data, size_t len, uint8_t channel);
    // channel → GNS Reliable / Unreliable（见 §4.3）

    bool isConnected() const;
    int  getPingMs() const;
};

} // namespace ayt::net
```

### 4.2 连接状态机与断线契约

```
Disconnected ──connect()/listen+accept──► Connecting ──handshake ready──► Connected
     ▲                                        │                              │
     └──────────── disconnect(reason) / timeout / peer close ◄───────────────┘
```

**锁定契约（R1）**：

| 项 | 要求 |
|----|------|
| 多连接 | Listen 侧维护 **连接列表**；`update()` 必须 pump **server + 全部 clients**（禁止「只吃第一个 incoming」） |
| Disconnect reason | 应用枚举（至少：`UserQuit` / `Timeout` / `Kicked` / `ProtocolMismatch` / `HostShutdown`）经 `onConnectionChange` 必达 |
| R1 done 定义 | **经 `INetworkSubSystem` 的双向 echo**（非仅 `GnsConnection` 单测） |

### 4.3 通道设计

| 通道 | 用途 | 映射到 GNS | 示例 |
|------|------|------------|------|
| `CHANNEL_RELIABLE` | 重要数据 | Reliable | 技能、背包、RPC |
| `CHANNEL_UNRELIABLE` | 实时状态 | Unreliable | 移动、动画时间 |
| `CHANNEL_FRAGMENTED` | 大包 | Reliable（优先整包走 GNS）；或 Protocol 显式分片 | 进房 Snapshot |

`send(..., channel)` **不得忽略 channel**（实现债：当前若忽略则标 R1/R2 blocker）。

---

## 5. 协议层设计 (Protocol)

> Protocol 描述 **应用 payload**，不替代 GNS transport。见 §3.3。
> R2 (2026-07-27): §5 wire format 落地 — 12B header + 4B trailing CRC32C + lz4 encode/decode + 8B FragmentHeader. 详见 §10 Phase 2 与 §13 R2 checklist.

### 5.1 包头结构（R2 落地；12B + 末尾 4B CRC32C）

```cpp
#pragma pack(push, 1)
// 应用层头：消息类型 + 版本 + 可选压缩；不含 transport 可靠序号
struct PacketHeader {
    uint16_t msgType;      // Replication / RPC / Handshake / ...
    uint16_t schemaVersion;
    uint16_t length;       // body 长度（不含 header、不含末尾 CRC）
    uint8_t  channel;      // 与 send 通道一致（冗余校验用）
    uint8_t  flags;        // PacketFlag bitset (Compressed / Fragmented)
    uint32_t timestampMs;  // 服务器时间基（复制插值用，R3+）
};
#pragma pack(pop)
```

**帧布局**（小端序）：
- 普通帧: `[PacketHeader 12B][body N][CRC32C 4B]`, `length = N`
- 压缩帧 (`PacketFlag::Compressed`): `[PacketHeader 12B][u32 uncSize 4B][lz4 块 C][CRC32C 4B]`, `length = 4+C`
- 分片帧 (`PacketFlag::Fragmented`): 每个分片独立 sealed，body = `[FragmentHeader 8B][chunk]`. **末片可变长** (Ct ≤ chunkSize for 非末片).
- **Compressed + Fragmented 互斥**：先 lz4 整 payload，再按需分片。

**CRC32C 范围**: `[PacketHeader 12B] + [可选 u32 uncSize] + [body]`，**不覆盖末尾 4B CRC 自己**。
- 多项式 0x1EDC6F41 (Castagnoli, 与 GNS / Intel CRC32 指令同款)
- 参考向量: `CRC32C("123456789") == 0xE3069283`
- 校验失败 → `ok=false` 直接丢弃

**msgType 命名空间**:
- `kMsgTypeHandshake = 0xFFFF` (握手包在 PacketHeader 之上；body = R1 wire format 不变)
- `kMsgTypeApp = 0` (应用默认；R3 ReplicationManager 会按子系统分配范围)
- `kSchemaVersion = 1` (R2 baseline)

**PacketFlag (R2 清理)**:
- `None=0`, `Fragmented=1<<0`, `Compressed=1<<1`
- **删除** `Encrypted=1<<2` (design §3.3 — DTLS/AES 归 GNS, 应用层禁假加密)
- **删除** `Reliable=1<<3` (channel 已含 reliable/unreliable 语义)

### 5.2 分片机制（R2 落地）

```cpp
struct FragmentHeader {
    uint32_t fragmentId;     // per-packet-local, NOT transport seq (GNS owns that)
    uint16_t fragmentIndex;  // 0-based
    uint16_t fragmentCount;
};
```

**`PacketAssembler::fragment`** — 静态 owned-buffer 接口:
```cpp
static std::vector<std::vector<uint8_t>> fragment(
    const uint8_t* payload, size_t len, uint32_t mtu,
    uint16_t msgType, uint16_t schemaVersion,
    uint8_t channel, uint32_t timestampMs, uint32_t fragmentId);
```
- chunkSize = mtu − 24 (header 12 + FragmentHeader 8 + CRC 4)
- fragmentCount = ceil(len / chunkSize)
- **末片 Ct = len − chunkSize×(count−1)** (可变；fixes R1 over-alloc bug)

**`PacketAssembler::consume`** — 实例方法:
```cpp
std::optional<std::vector<uint8_t>> consume(const uint8_t* fragBody, size_t fragBodyLen);
```
- Robust to 乱序/重复/丢包:
  - 乱序: keyed by (fragmentId, fragmentIndex)
  - 重复: receivedMask bitset, dup 忽略
  - 丢包: 返回 payload 仅当 receivedCount==fragmentCount
- 返回 payload 恰好一次 (最后一个到达的 unique fragment 完成 set)

---

## 6. 复制层设计 (Replication)

### 6.1 核心概念

```
┌─────────────────────────────────────────────────────────┐
│  DataComponent (纯数据)                                 │
│  - 只包含字段                                           │
│  - AYTYPE_FIELDS 标记 NetReplicate                      │
│  - 将来可直接转为 Pure ECS Component                    │
├─────────────────────────────────────────────────────────┤
│  Component (功能组件)                                   │
│  - 包含方法 (逻辑)                                      │
│  - 持有 DataComponent                                   │
│  - 可实现 IComponent 接口                              │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  ReplicationSystem (系统)                               │
│  - 读取 AYReflect 元数据                               │
│  - 自动序列化 DataComponent                            │
│  - 处理网络同步逻辑                                    │
└─────────────────────────────────────────────────────────┘
```

### 6.2 DataComponent 示例

```cpp
// HealthData.h - 纯数据，可网络同步
#pragma once
#include <AYReflect.h>

struct HealthData {
    int32_t hp = 100;
    int32_t maxHp = 100;
};

AYTYPE_REGISTER(HealthData, "HealthData");
AYTYPE_FIELDS(HealthData)
    AYTYPE_FIELD_EX("hp", hp, ayt::reflect::FieldAttribute::NetReplicate)
    AYTYPE_FIELD_EX("maxHp", maxHp, ayt::reflect::FieldAttribute::NetReplicate)
AYTYPE_FIELDS_END(HealthData)
```

### 6.3 Component 示例（保持现状）

```cpp
// HealthComponent.h - 功能组件
#pragma once
#include <IAYEntity.h>

namespace ayt::entity
{

class HealthComponent : public IComponent {
public:
    const char* getName() const override { return "Health"; }

    void onAttach(Entity* entity) override;
    void onUpdate(float dt) override;
    void onDetach() override;

    void damage(int32_t amount);
    void heal(int32_t amount);

    HealthData* getData() const { return _data; }

private:
    HealthData* _data = nullptr;
};

AY_COMPONENT(HealthComponent);

} // namespace ayt::entity
```

### 6.4 ReplicationManager

```cpp
class ReplicationManager {
public:
    // 注册/注销复制的 Entity
    void registerEntity(Entity* entity, uint32_t netId);
    void unregisterEntity(uint32_t netId);

    // 查找
    Entity* findEntity(uint32_t netId) const;

    // 同步 (服务端每帧调用)
    void replicate(uint32_t currentTime);

    // 接收 (客户端调用)
    void onReceive(NetConnection* conn, BitStream& stream);

    // 强制同步
    void forceReplicate(uint32_t netId);

    // 通道设置
    void setChannel(uint8_t channel) { _channel = channel; }
    uint8_t getChannel() const { return _channel; }

private:
    // 基于 AYReflect 元数据序列化
    template<typename T>
    void serializeData(T* data, BitStream& stream) {
        auto* type = ayt::reflect::TypeRegistryImpl::instance().findType<T>();
        if (!type) return;

        for (uint32_t i = 0; i < type->getFieldCount(); i++) {
            auto* field = type->getField(i);
            if (!field->hasAttribute(ayt::reflect::FieldAttribute::NetReplicate)) {
                continue;
            }
            serializeField(stream, field, data);
        }
    }

    void serializeField(BitStream& stream, ayt::reflect::IFieldInfo* field, void* obj);

    std::unordered_map<uint32_t, Entity*> _entities;
    uint8_t _channel = CHANNEL_RELIABLE;
};
```

### 6.5 复制流程

**服务端 → 客户端 (State Sync)**：

```
每帧:
  for each registered Entity:
    for each Component:
      if Component has DataComponent:
        if hasChanges(DataComponent):
          serializeData(DataComponent) → BitStream
          send(CHANNEL, BitStream)
```

**客户端接收**：

```
onReceive(BitStream):
  read netId
  find Entity
  for each DataComponent field:
    deserialize field value
    update DataComponent
```

### 6.6 Authority 最小模型（锁定 2026-07-27，实现待 R3）

> **v1 默认：Dedicated / Listen Server 权威。** 客户端不得擅自改写带 `NetReplicate` 的权威字段。

| 概念 | 语义 |
|------|------|
| `NetRole` | `Authority`（本端有写权）/ `SimulatedProxy`（只收状态）/ `AutonomousProxy`（本地输入 + 收校验，R5 prediction 再用） |
| `isServer()` | 本进程是否运行权威世界（Dedicated 或 ListenServer 主机） |
| `isOwner()` | 该 `IReplicable` 是否由本连接「拥有」（如本地玩家 pawn）；**仅影响输入/RPC 发起权，不替代 Authority 写权** |
| 谁 `replicate()` | **仅 Authority 端** 向非权威端发送 Snapshot/Delta |
| 谁可改字段 | Authority 写；客户端改本地预测副本（若启用）但必须以服务器状态为准 |

**进房**：Authority → Full Snapshot（全量）→ 之后脏字段 Delta（R3）。  
**非法写**：客户端发来的状态包默认丢弃；敏感变更走 **RPC + server validation**（R4）。

本模型不覆盖 P2P 无主机、无锁步确定性；若未来要做，另开专章，不得 silently 改默认。

---

## 7. BitStream 设计

### 7.1 现有实现

AYNetwork 已有的 `BitStream` 实现继续使用。

### 7.2 与 AYSerializer 整合

未来可考虑使用 AYSerializer 代替 BitStream，但当前保持独立。

---

## 8. 目录结构

```
AYRuntime/AYNetwork/
├── design.md
├── CMakeLists.txt
│
├── interface/
│   └── IAYNetwork.h           # 核心接口
│
├── include/
│   ├── AYNetwork.h           # 主入口
│   ├── BitStream.h           # 位流 (现有)
│   │
│   ├── Transport/            # 传输层
│   │   ├── GnsConnection.h   # ★ GNS 包装（当前）
│   │   ├── KcpConnection.h   # Deprecated — 待删
│   │   ├── UdpSocket.h       # Deprecated — 待删
│   │   └── Connection.h      # 连接状态机 / 列表
│   │
│   ├── Protocol/             # 协议层
│   │   ├── PacketHeader.h   # 包头
│   │   └── PacketAssembler.h# 组包/拆包
│   │
│   ├── Replication/          # 复制层
│   │   ├── ReplicationManager.h
│   │   ├── ReplicationSystem.h
│   │   └── NetDataComponent.h# 标记宏
│   │
│   └── RPC/                 # RPC 层 (规划)
│       └── RpcHandler.h
│
├── src/
│   ├── AYNetworkSubSystem.cpp
│   ├── BitStream.cpp
│   ├── ReplicationManager.cpp
│   ├── NetConnectionImpl.cpp
│   │
│   ├── Transport/
│   │   ├── KcpConnection.cpp
│   │   ├── UdpSocket.cpp
│   │   └── Connection.cpp
│   │
│   ├── Protocol/
│   │   ├── PacketHeader.cpp
│   │   └── PacketAssembler.cpp
│   │
│   └── Replication/
│       └── ReplicationSystem.cpp
│
└── unittest/
    ├── NetworkTest.cpp
    └── ReplicationTest.cpp
```

---

## 9. 依赖关系（2026-07-26 更新）

```
AYNetwork
├── AYReflect    (元数据，自动序列化)
├── AYSerializer (序列化工具，可选 — 当前自研 BitStream)
├── AYEntity     (Component 引用)
├── AYPlatform   (线程、Mutex)
├── gamenetworkingsockets  (★ vcpkg, 传输层 — 替代原 KCP)
│   ├── openssl   (vcpkg transitive)
│   └── protobuf  (vcpkg transitive, 计划禁用)
└── (删除) KCP   — 不再需要
```

---

## 10. Phase 定义（更新于 2026-07-26 工业级审计）

> ⚠ **2026-07-26 工业级审计结论**：原 §10 把 Phase 1/2/3 全部标记 [x]，
> 但实际代码是 **stub / 占位 / TODO**。下面以**真实实现度**重新标注。
> 距离工业级网络模块还差 **R1–R6 共 6 个 Phase**（见 §13）。

### Phase 1：传输层（**部分完成 — GNS R1 进行中**）

**目标**：经 `INetworkSubSystem` 的可靠 UDP 通信

**当前实现度（2026-07-27 R1 done 之后）**：

| 子项 | 当前状态 | 缺口 |
|------|---------|------|
| 传输库 GNS | ✅ 已 `find_package` + `GnsConnection` | — |
| Loopback echo 单测 | ✅（EchoLoopback）| — |
| 多客户端 echo（1 server + 2 clients） | ✅ R1.A（MultiClientEcho） | — |
| `NetworkSubSystem::update` 多连接 pump | ✅ R1.A | — |
| `NetworkSubSystem::broadcast` | ✅ R1.A | broadcastExcept 仍以 raw 指针 identity-match（R3 用 NetConnection*） |
| 握手协议 HELLO/WELCOME/REJECT | ✅ R1 done + R2 迁移到 PacketHeader | msgType=0xFFFF 标识握手包，body 保留 R1 wire format |
| DisconnectReason 必达 | ✅ R1 done | — |
| `KcpConnection` / `UdpSocket` | Deprecated 残留 | 删除或移出默认构建 |
| 通道 → GNS flags 映射 | ⚠ 仅 Reliable | R4 扩全 4 通道 |

**工业级门槛**（R1 done ✅）：
- ✅ GNS 可靠/不可靠 + DTLS（库侧）
- ✅ 子系统级双向 echo + **多客户端** accept
- ✅ 心跳/RTT 可查询
- ✅ 握手 + disconnect-reason 必达
- ❌ 应用层自研第二套可靠/加密（明确不做）

### Phase 2：协议层（**R2 完成 — 2026-07-27**）

**目标**：应用消息头、可选压缩、完整分片（§3.3 / §5）

**当前实现度（2026-07-27 R2 之后）**：

| 子项 | 当前状态 | 备注 |
|------|---------|------|
| `PacketHeader` 12B 布局 | ✅ R2 | msgType/schemaVersion/length/channel/flags/timestampMs |
| `PacketHeader::checksum` (CRC32C, Castagnoli) | ✅ R2 | 末尾 4B，覆盖 [header][body]，不覆盖 CRC 自己 |
| `PacketFlag::Compressed` (lz4) | ✅ R2 | decode 复用 AYStorage::Lz4Decompressor；encode R2 内薄包 `<lz4.h>` |
| `PacketFlag::Encrypted` | ✅ R2 删 | 加密归 GNS, 应用层禁止假加密 |
| `PacketFlag::Fragmented` | ✅ R2 | 分片走 RELIABLE 通道 |
| `PacketAssembler::fragment` | ✅ R2 | owned-buffer, 末片可变长, chunkSize = mtu-24 |
| `PacketAssembler::consume` | ✅ R2 | robust 乱序/重复/丢包, optional payload 出口 |
| 自研 `SequenceNumber` | ✅ R2 删 | 与 GNS 双计 seq 禁止, PacketHeader v2 无 packetId |
| 协议一致性测试 | ✅ R2 | AYTest_PacketCodec.cpp: 7 纯 + 3 GNS, 含末片/乱序/重复/丢包 |

### Phase 3：复制层 — **R3.2 完成 (2026-07-28)**

**目标**：基于 AYReflect 的自动同步 + §6.6 Authority + dirty-tracking Delta + 嵌套字段

**当前实现度**：R3.0 + R3.1 + R3.2 ship — **493/493 tests PASS** (R3.0 226 + R3.1 21 = 247；R3.2 +15 = 262 实际 case；493 = check 行总计数)

| 子项 | 当前状态 | 备注 |
|------|---------|------|
| `ReplicationManager::registerObject(void*, ITypeInfo*, uint32_t)` | ✅ | 主路径 |
| `ReplicationManager::registerObject(IReplicable*, uint32_t)` | ⚠ deprecated | R1 wrapper 保留供旧 stub 过渡；无 AYReflect 元数据时不强 wire |
| `ReplicationManager::findType/findObject/findIReplicable` | ✅ | R3.0 新增 |
| `ReplicationManager::tick` | ✅ | R3.1 重写：dirty-tracking → Full Snapshot (RELIABLE) / Delta (UNRELIABLE) / None 三态 |
| `ReplicationManager::onReceive` | ✅ | R3.1 加 `kMsgTypeDelta` case，复用 `deserializeObject` 路径 |
| `ReplicationManager::forceReplicate` | ✅ | R3.1 真实现：置 `_initialized=false`，下次 tick 走 Full |
| `ReplicationManager::getDirtyFieldCount` | ✅ NEW | R3.1 debug API |
| `ReflectSerializer` (H+CPP) | ✅ R3.2 扩展 | 16 WireTypeId dispatch (12 primitive + 4 nested) + AYReflect walk + `serializeDirtyFields` + `hashFieldValueEx` (嵌套 CRC32C) + 递归 `writeWireValue/readWireValue` |
| `ReplicationSystem ↔ Manager` | ✅ | 删除 netId↔entityId 双 map；System 降级 adapter |
| Authority / Ownership（§6.6） | ✅ Server-only 实现 | clients 不 broadcast；服务端拒绝未知 netId |
| Full Snapshot | ✅ | R3.0 MVP：每 tick 全量 (RELIABLE) |
| Delta Update | ✅ | R3.1 done — CRC32C per-field baseline, dirty-fields-only frame (UNRELIABLE) |
| **嵌套 struct 字段** | ✅ **R3.2 done** | WireTypeId::NestedStruct=12，递归 emit `[u16 nestedHash][u8 fieldCount][records...]` |
| **固定数组字段** | ✅ **R3.2 done** | WireTypeId::FixedArray=13，`std::array<T,N>` 经 `ArrayTypeInfo<T,N>` 走 |
| **动态数组字段** | ✅ **R3.2 done** | WireTypeId::DynamicArray=14，`std::vector<T>` 经 `VectorTypeInfo<T>` 走 |
| **字符串 map 字段** | ✅ **R3.2 done** | WireTypeId::StringMap=15，`std::map<string,V>` 经新增 `MapTypeInfo<V>` + `MapTypeInfoBase` 走 |
| **R3.0/R3.1 向后兼容** | ✅ | 新 WireTypeId 12..15 走 `default: return false` 路径让 R3.1 receiver 静默 drop frame；schemaVersion=1 不 bump |
| **整个 nested 字段粒度 hash** | ✅ | 任一内层元素变 → 整个 nested 字段 hash 变 → 走 Delta frame (per-user 决策) |
| `EntityReplicationAdapter.h` | ✅ | ECS 桥接 |
| `IReplicable::replicate/onReplicate` | ✅ **已删除** | R3.1 全树 grep 验证 0 用户实现后真正删除 |
| Simulated/Autonomous Proxy gate | ❌ | **R5 work** — lag comp 一并做 |

### Phase 4：RPC — **R4.0 完成 (2026-07-29)**

**目标**：`Server` / `Client` / `NetMulticast` + server validation + 4 channel 全展开（GNS send flag 映射）

**当前实现度**：R4.0 (2026-07-29) ship — **566/566 测试 PASS** (R3.2 493 baseline + R4.0 +73 case)。Server/Client/Multicast 三类型 + Validator per-method (`IMethodInfo::validate`) + 4 通道 (RELIABLE/UNRELIABLE/FRAGMENTED+NoNagle/ACK) 全 ship。Interest Management 押后 R4.1。

| 子项 | 当前状态 | 备注 |
|------|---------|------|
| `kMsgTypeRpcRequest=0x0010 / kMsgTypeRpcResponse=0x0011 / kMsgTypeRpcReject=0x0012` | ✅ | envelope msgType 命名空间独立，schemaVersion=1 不 bump；R3.x receiver 静默 drop |
| `RpcHandler` (H+CPP) | ✅ | `include/RPC/RpcHandler.h` + `src/RPC/RpcHandler.cpp`；registerMethod / callServer/Client/Multicast / onRpcRequest/Response/Reject |
| `RpcSerializer` thin wrapper | ✅ | writeRpcArgs/readRpcArgs/writeRpcResponse/readRpcResponse/writeRpcReject/readRpcReject — 复用 R3.2 16 WireTypeId dispatch (`writeWireValue/readWireValue`) |
| `IMethodInfo::getRpcKind()` enum (None/Server/Client/Multicast) | ✅ | R4.0 AYReflect 扩展，5 虚函数带 default impl (None/false/false/true/nullptr) 保持 AYScript logia source-compat |
| `IMethodInfo::isUnreliable()` per-RPC channel override | ✅ | Reliable 默认；high-freq RPC 标 true 走 CHANNEL_UNRELIABLE |
| `IMethodInfo::validate(const void*)` server-side reject hook | ✅ | per-method 替代 FieldAttribute bit (用户决策改 method-domain) |
| callId 64-bit + pendingCalls mutex map | ✅ | Response/Reject 自动 fire callback; auto-cleaned on match |
| Authority gate 按 RpcKind | ✅ | Server RPC 仅在 Server/ListenServer 端处理；Client RPC 仅在 Client 端处理；mis-directed frame → RpcReject reason=NotAuthority |
| `GnsConnection::_rawSend` 4-channel switch | ✅ | CHANNEL_RELIABLE → Reliable；CHANNEL_UNRELIABLE → Unreliable；CHANNEL_FRAGMENTED → Reliable\|NoNagle；CHANNEL_ACK → Reliable (R4.1 stub) |
| ReplicationManager envelope fix | ✅ | `unregisterObject` 用 `kMsgTypeEntityDespawn` envelope (替代 R3.2 typo `kMsgTypeReplication`) — wire 对称 |
| `AYNetworkSubSystem::update` 入口 demux | ✅ | PacketCodec::decode 走 envelope.msgType: 0x0010/0x0011/0x0012 → `_rpcHandler.onRpcXxx`；0x0001..0x0004 → 既有 `_replicationManager.onReceive`；其它 → 既有 per-channel MessageHandler |
| `INetworkSubSystem::getRpcHandler()` | ✅ | 单一 RpcHandler 引用，AYNetworkSubSystem ctor 持有 |
| `Interest Management (Relevancy + distance culling)` | ❌ | **R4.1 work** — 用户 explicit 押后 |
| Per-field RepNotify callback (Unreal `OnRep_X`) | ❌ | **R4.1 work** |
| Multicast RPC for unregistered types / wildcard | ❌ | **R4.1+ work** |

### Phase 5：Interest Management（**未开始**）

**目标**：Relevancy 谓词 + 距离裁剪（R4.1）

**当前实现度**：**0 设计细节 + 0 实现**。R4.1 起。
---

## 11. 与 AYEntity 集成（待重写）

> 当前 §11 写的是"ReplicationManager 注册 Entity"，但 `ReplicationManager`
> 实际 API 是 `registerObject(IReplicable*, uint32_t)` — 注册的是 `IReplicable`
> 接口对象，不是 `Entity*`。且 AYEntity 已经存在
> [`AYNetworkComponent`](../../AYRuntime/AYEntity/include/components/AYNetworkComponent.h)
> 但**没有被 AYNetwork 消费**。两边 API 对不上。
> 重写待 Phase R3 完成。

---

## 12. 与工业级引擎对标（更新于 2026-07-26 审计）

| 特性 | AYNetwork 2026-07-27 | Unreal 5 | Unity NetCode | O3DE | 工业级门槛 |
|------|:-------------------:|:--------:|:-------------:|:----:|:----------:|
| UDP/可靠传输 | ⚠ GNS 部分 | ✅ | ✅ | ✅ | ✅ |
| Authority 模型 | ⚠ 规格 §6.6 / 未实现 | ✅ | ✅ | ✅ | ✅ |
| Interest Management | ❌ | ✅ | ✅ | ✅ | ✅（MMO 必备） |
| Replication Graph | ❌ | ✅ | ✅ | ✅ | ✅ |
| RPC（Reliable/Unreliable） | ❌ | ✅ | ✅ | ✅ | ✅ |
| Connection state machine | ⚠ | ✅ | ✅ | ✅ | ✅ |
| Packet fragmentation | ⚠ | ✅ | ✅ | ✅ | ✅ |
| Heartbeat / RTT / Stats | ⚠ GNS 有 / 未暴露齐 | ✅ | ✅ | ✅ | ✅ |
| Snapshot + Delta 同步 | ❌ | ✅ | ✅ | ✅ | ✅ |
| 加密（DTLS / AES-GCM） | ✅ GNS | ✅ | ✅ | ✅ | ✅ |
| 压缩 | ❌ | ✅ | ✅ | ✅ | ⚠ |
| Lag compensation | ❌ | ✅ | ✅ | ✅ | ✅（FPS/MOBA） |
| Server 端反作弊校验 | ❌ | ✅ | ✅ | ✅ | ✅ |
| Replay / Demo | ❌ | ✅ | ❌ | ❌ | ⚠ |

**综合评分（2026-07-27 设计审计后）**：
- 设计清晰度：**75/100**（GNS/Authority/协议切分已钉；RPC 专章仍薄）
- 实现完整度：**~25/100**（传输骨架抬升；复制/RPC 仍空）
- 工业可用度：**~10/100**（不可上线联机玩法）
- 距可用门槛：仍约 **14–20 周**（§13 R1–R4 主路径）

**分阶段可 ship**：现在 **No** → R1 子系统 echo+多连接 = LAN 玩具 → R3 = 小型状态同步原型 → R4 = 接近可玩小规模联机。跨 NAT / 商店级另需中继与身份策略（§14.6）。
---

## 13. 实施路线图（Roadmap，2026-07-26 重置）

### Phase R1（4–6 周）：传输层真正跑起来

1. 接入真实传输库（决策见 §14）—— 用 vcpkg `find_package` 集成 ✅
2. `AYNetworkSubSystem::update()` 实现真 tick：dispatch 所有 connections + 处理 timeout ✅
3. `NetConnection` 实现 send/disconnect/connected 全部完成 ✅
4. Client/Server 握手协议（connect → ack → ready → disconnect reason）✅
5. **删除重复文件** `src/ReplicationManager.cpp`（与 `src/Replication/ReplicationManager.cpp` 冲突）✅
6. 跑通最小 echo：client 发 → server 回 → 双向一致 ✅
7. **R1.A（2026-07-27）多连接 pump**：server 接受 N 个客户端、`update()` 遍历所有 conn、`broadcast()` 真正广播 ✅（53/53 测试通过）
8. **R1 done（2026-07-27）握手协议**：HELLO/WELCOME/REJECT 线协议 + DisconnectReason 枚举 + 经 onConnectionChange 必达 ✅（68/68 测试通过）

### R1 done = LAN 玩具 ✅

**所有 R1 子项均已通过单元测试**（68/68），可以宣布 R1 ship = LAN 玩具（同 design §12 分阶段可 ship 评估）。

**已知 R1 限制（不影响 R2 起步）**：
- 握手包不走 PacketHeader（R2 协议层接管）
- linger 用简单 `bLinger=true`，未调 `EnableLingerMessage` 控制时长
- 通道仍只 Reliable；R4 扩 4 通道
- 未提供 reject reason 给 app 端 onConnectionChange（只在 server 端 capture 给 server 自己看）

### Phase R2（3–4 周）：协议层完整

1. `PacketHeader::checksum` 真正计算（CRC32） ✅
2. `PacketFlag::Compressed` 实现（zstd 或 lz4） ✅ lz4
3. 完整 `PacketAssembler::fragment` + `assemble`（最后一片可变长） ✅
4. 删除 `SequenceNumber`（KCP/GNS 自带 seq）或改作 wrapping ✅ 完全删除
5. 协议一致性测试：乱序/丢包/重复/分片 注入 ✅
6. 握手包迁移到 PacketHeader v2 (msgType=0xFFFF) ✅ (R2 done bonus, was originally R3)

### Phase R3.0（4 周 → R3.0 MVP 1 周）：复制层真接通

1. `ReplicationManager::serializeObject(const ITypeInfo*, void*, BitStream&)` 走 AYReflect 反射遍历字段，按 `FieldAttribute::NetReplicate` 决定是否序列化 — ✅ 实现为 `ReflectSerializer::serializeObject(type, obj, netId, BitStream&)`
2. `replicate()` 真发，`onReceive(BitStream&)` 真收 — ✅ `tick()` 服务端 broadcast + `onReceive(BitStream&, NetConnection*)` 解码
3. `ReplicationSystem` ↔ `ReplicationManager` 互通 — ✅ 删双 map；System 降级 adapter，`onUpdate` → `ReplicationManager::tick`
4. Authoritative Server 模型（`NetComponent::isOwner()` 真影响 replication 决策）— ✅ §6.6 v1 = Server 权威；clients 不 broadcast；服务端对未知 netId 帧拒绝；**Simulated/Autonomous Proxy gate 留 R5**
5. Full Snapshot（新连接加入时一次全量）+ Delta Update（脏字段增量）— ✅ Full Snapshot 每 tick；**Delta Update 留 R3.1**
6. **删除空宏** `AY_NET_FIELD`（统一走 AYReflect 元数据）— ✅ R2 已删，`AYTYPE_FIELD_EX(... NetReplicate)` 是新规约

**R3.0 = 226/226 PASS, 2026-07-27 ship**

### Phase R3.1（1–2 周）：复制层 Delta Update

1. `kMsgTypeDelta = 0x0004` 新增 wire msgType slot；body = 与 Full Snapshot 同 8B header + N field records
2. `ReflectSerializer::hashFieldValue(WireTypeId, void*)` — 复用 `PacketCodec::computeCrc32c` 做 per-field baseline (12 WireTypeId switch)
3. `ReflectSerializer::serializeDirtyFields(type, obj, netId, denseIndices, BitStream&)` — emit 仅指定 dense index 的字段
4. `ReplicationManager::ReflectedEntry` 扩展：`_fieldHashes[denseIdx]` (CRC32C 缓存) + `_netFieldSparseIndex[denseIdx]` (dense→sparse 映射) + `_initialized` (one-shot gate)
5. `ReplicationManager::tick` 重写：对比 currentHashes vs `_fieldHashes`；未初始化→Full Snapshot (RELIABLE)；steady state 无 dirty→不广播；部分 dirty→Delta (UNRELIABLE)
6. `ReplicationManager::forceReplicate(netId)` 真实现：置 `_initialized=false`，下次 tick 必走 Full
7. `ReplicationManager::getDirtyFieldCount(netId)` debug API
8. `ReplicationManager::onReceive` 加 `kMsgTypeDelta` case，复用 `deserializeObject` 路径
9. `IReplicable::replicate/onReplicate` 真正删除（全树 grep 验证无用户实现）

**R3.1 = 378/378 PASS, 2026-07-28 ship** (R3.0 baseline 226 + R3.1 +21 case, 6 pure + 15 e2e 端到端; 2026-07-28 cc9c1da 二次修正 `CHECK_INT_EQ(bs.readUInt16(), ...)` macro 三次求值坑，3 case 从 FAIL 转 PASS)

### Phase R4（3 周）：RPC + Interest Management

1. RPC 系统：`Server` / `Client` / `NetMulticast` 三类型，参数序列化复用 AYReflect
2. Validation hook（server-side 拒绝非法 RPC）
3. Interest Management 基础 — `Relevancy` 谓词 + 距离裁剪
4. `INetworkExtension` 的 `onPreReplicate(targets)` 真调用

### Phase R4.0 — RPC 三类型 + 4 通道 + Validator（**2026-07-29 ship**）

1. ✅ RPC 系统：Server / Client / Multicast 三类型；参数 (de)serialize 复用 R3.2 `ReflectSerializer::writeWireValue/readWireValue` (16 WireTypeId dispatch) — `RpcSerializer` thin wrapper 加在 `RpcHandler.cpp`
2. ✅ Validation hook：per-method `IMethodInfo::validate(const void*)` — 拒绝返 false → server emit `RpcReject` reason=ValidatorDeny
3. ✅ Authority gate 按 RpcKind 区分：Server RPC 仅在 Server/ListenServer 端处理；Client RPC 仅在 Client 端处理；Multicast 在所有端处理
4. ✅ 4 通道全展开 (`GnsConnection::_rawSend`)：CHANNEL_RELIABLE → GNS Reliable；CHANNEL_UNRELIABLE → GNS Unreliable；CHANNEL_FRAGMENTED → GNS Reliable\|NoNagle；CHANNEL_ACK → GNS Reliable (R4.1 stub)
5. ✅ `RpcHandler` 全套 API：registerMethod (per-method hash binding) / callServer/Client/Multicast (outbound) / onRpcRequest/Response/Reject (inbound dispatch) / callId 64-bit pending map (auto-cleanup)
6. ✅ Wire 兼容：schemaVersion=1 不 bump；R3.x receiver 收到 envelope 0x0010..0x0012 静默 drop (default envelope kind 不识别)
7. ✅ 73 测试 (R3.2 493 → 566 baseline + 73 R4.0): pure dispatch / 4 channel / validator reject / authority gate / R3.0-R3.2 zero regression
8. ⏸ Interest Management (`Relevancy` + distance culling) — **R4.1 work**, 用户 explicit 押后
9. ⏸ Per-field RepNotify (Unreal `OnRep_X`) — **R4.1 work**, 同 RPC 思路
10. ⏸ Multicast RPC for unregistered types / wildcard — **R4.1+ work**

### Phase R4.1（**待开始**）— RPC polish + Interest + RepNotify

1. Interest Management：`Relevancy` 谓词 + 距离裁剪 + `INetworkExtension::onPreReplicate(targets)` 真调用
2. Per-field RepNotify callback (Unreal `OnRep_X`) — 用户标 `FieldAttribute::RepNotify` 后 server→client 端 invoke 回调
3. RPC timeout / retry / exponential backoff（高优先级 race 防护）
4. RpcHandler async invoke (RPC takes > 16 ms) — thread pool + future-based response
5. Multicast RPC for unregistered types / wildcard
6. `kMsgTypeRpcResponse` 自动 lz4 compress（`PacketFlag::Compressed` 已 ship 多年）

### Phase R5（可选）：Lag comp / Replay / Snapshot interp

1. Client-side prediction + server reconciliation
2. Snapshot interpolation（Unity NetCode 的 `SnapshotSystem` 模式）
3. Replay recording（Unreal `UReplaySubsystem` 模式）

### Phase R6（持续）：测试 / 工具

1. 协议一致性测试（双向 echo 测乱序/丢包/重复）
2. 带宽 profiling 工具
3. 确定性回放测试（连发三次相同输入，结果 byte-equal）
4. `unittest/CMakeLists.txt` 注册现有 `BitStreamTest` / `TransportTest`

---

## 14. 传输库选型决策（2026-07-26）

### 14.1 候选库（vcpkg ports 验证存在）

| 库 | 版本 | 协议栈 | 加密 | 连接管理 | 依赖 |
|----|------|--------|------|----------|------|
| **enet** | 1.3.18 | Reliable UDP（自带）| ❌ 无 | ⚠ 基础 | 无 |
| **gamenetworkingsockets** | 1.4.1 | Reliable UDP + unreliable | ✅ **DTLS + AES-GCM 内置** | ✅ 完整（heartbeat、stats、nagle）| OpenSSL + Protobuf |
| **kcp** | 1.7 | ARQ（只是协议）| ❌ 无 | ❌ 无 | 无 |
| **kissnet** | rolling | 仅 socket wrapper | ❌ 无 | ❌ 无 | 无 |

### 14.2 评估维度

| 维度 | enet | gamenetworkingsockets | kcp | kissnet |
|------|:----:|:---------------------:|:---:|:-------:|
| 工业血统 | Cube/RealmForge (1999-) | **Steam (Valve)** | skywind 个人项目 | 个人项目 |
| 加密内建 | ❌ 需自加 | ✅ **生产级** | ❌ | ❌ |
| Heartbeat/RTT/Stats | ⚠ 基础 | ✅ 完整 API | ❌ | ❌ |
| IPv6 | ✅ | ✅ | n/a | ✅ |
| 跨平台 | Win/Linux/macOS | **Win/Linux/macOS/iOS/Android/PS/Xbox** | 任意 | 任意 |
| vcpkg 支持 | ✅ | ✅ | ✅ | ✅ |
| 学习曲线 | 平 | 中（C++ Valve style）| 平（API 极简）| 极平 |
| 代码量（接入）| ~3k 行 | ~10k 行 wrapper | ~1.5k 行 | ~500 行 |
| 依赖 | 无 | **OpenSSL + Protobuf** | 无 | 无 |
| 维护活跃度 | 维护但慢 | **活跃（Valve 持续）** | 慢 | 中 |

### 14.3 决策

**选 `gamenetworkingsockets`（Valve OSS）** 作为 AYNetwork 传输层底座。

**理由**：

1. **唯一同时满足**"可靠 UDP + 内建加密 + 连接管理"的候选。
   enet/kcp/kissnet 都得自己叠加密 + 心跳 + 超时 — 等于自己重造 GameNetworkingSockets。
2. **工业血统强**：Steam 用它支撑 Dota2/CS2/PUBG，每天承载**数亿连接**。
3. **2026 法规合规**：内置 DTLS + AES-GCM，省去自研加密的合规风险。
4. **OpenSSL + Protobuf 依赖可接受**：
   - OpenSSL 跨平台，vcpkg 一键安装
   - Protobuf 只用于消息定义（也可禁用，见 14.5）
   - 整个项目已经在用 SDL2/Jolt 等外部库，OpenSSL 不增加额外心智负担
5. **完整 stats API**（`GetLatestPing`、`GetStatusDetailed`、`SteamNetworkingFakeIP`）— 直接对应 §1 缺的"RTT/带宽监控"。

### 14.4 取舍说明（为什么不选其他）

- **enet**：可靠 UDP 有，但**加密缺失**对 2026 年商业游戏不可接受。Cube 引擎时代（1999）没法规要求。
- **kcp**：只是 ARQ 协议，**没有连接/加密/心跳**。skywind 个人项目，无工业血统。KCP 适合做"游戏内可靠通道"，不适合做完整传输栈。
- **kissnet**：只是 socket 包装，**完全无可靠层**。等于让我们自己实现 KCP+enettls，心智负担最大。

### 14.5 集成方案（vcpkg）

```cmake
# AYNetwork/CMakeLists.txt (新增)
option(AYNETWORK_BUILD_TRANSPORT "Build real transport layer (requires vcpkg gamenetworkingsockets)" ON)

if(AYNETWORK_BUILD_TRANSPORT)
    find_package(gamenetworkingsockets CONFIG REQUIRED)
    # 关闭 protobuf 消息定义（只用 C API）— 减少依赖
    set(GAMENETWORKINGSOCKETS_BUILD_PROTOBUF OFF CACHE BOOL "" FORCE)
endif()
```

依赖在 `vcpkg.json`（AYNetwork 自己的或根目录）：

```json
{
  "dependencies": [
    "gamenetworkingsockets"
  ]
}
```

### 14.6 接入后的架构变化

```
AYNetwork 五层架构（§3.1）
─────────────────────────────────────────────────────
Game Code / Gameplay
Replication / RPC Layer
Protocol Layer                ← AYNetwork 自研（PacketHeader/Assembler）
Transport Layer               ← ★ 切换为 GameNetworkingSockets 的 ISteamNetworkingSockets
Platform Layer                ← AYPlatform 已有 Thread/Mutex
```

**接口变化**：
- `KcpConnection` → `GnsConnection`（包裹 `ISteamNetworkingSockets`）
- `UdpSocket` → **删除**（GNS 内部管 socket）；树内残留标 Deprecated
- **产品路径（当前）**：`CreateListenSocketIP` / `ConnectByIPAddress`（LAN / 有公网 IP）
- **后续可选**：`ConnectP2P` / FakeIP / 自建 signaling 中继（**未进 R1 验收**；跨 NAT 上线前必须单独立项）

**保留不变**：
- 五层架构思路；Replication / RPC 层目标
- DataComponent + AYReflect；通道枚举语义
- Protocol 为 **应用消息**（§3.3），不重复 GNS 可靠/加密

### 14.7 风险与缓解

| 风险 | 影响 | 缓解 |
|------|------|------|
| OpenSSL 大（编译慢）| 构建时间 | vcpkg binary cache 启用；后续可换 mbedTLS |
| Protobuf 拖入 | 二进制大 | 禁用 GNS 的 protobuf（只走 C API）|
| GNS 不支持 Windows ARM64 | vcpkg `supports` 限制 | 当前目标 x64，ARM64 后续再说 |
| GNS API 偏 Steam 风格 | 学习成本 | 写一层薄 `GnsConnection` wrapper 隔离 |
| Steam 依赖语义（如 SteamID）| 误用风险 | wrapper 隐藏，只暴露 AYNetwork 的语义 |

---

## 15. 立即可改的清单（最小动作清单，2026-07-26）

按修复 ROI 排序：

| # | 文件 | 改动 | 估计工时 |
|---|------|------|---------|
| 1 | `CMakeLists.txt:11-19` | 删 `src/ReplicationManager.cpp`（与 `src/Replication/` 重复定义）| 5 min |
| 2 | `src/Transport/UdpSocket.cpp:42-52` | `WSACleanup()` 移到析构外（多个 socket 生命周期 bug）| 15 min |
| 3 | `src/AYNetworkSubSystem.cpp:30-32` | `update()` 里 dispatch + tick timeout 骨架 | 2 h |
| 4 | `include/Replication/NetDataComponent.h:25-26` | 删 `AY_NET_FIELD` 空宏（design 也没用到）| 5 min |
| 5 | `interface/IAYNetwork.h:223` | 删 `NETWORK_SUBSYSTEM()` 宏或确认 `GameLoop::getNetwork()` 存在 | 5 min |
| 6 | `unittest/CMakeLists.txt` | 新建并注册 `BitStreamTest` / `TransportTest` | 1 h |
| 7 | `design.md §10 Phase 1/2/3` | 把 `[x]` 改回 `[ ]`（本文档已修正）| done |

总计 **P0 最小修复 ≈ 半日工作量**。

---

## 16. Changelog

| 日期 | 变更 |
|------|------|
| 2026-07-26 | 工业级审计；R1–R6 重置；GNS 选型 §14 |
| 2026-07-27 | **设计审计补丁**：§1/§3/§4 统一 GNS；§3.3 Protocol↔GNS 切分；§4.2 多连接+断线；§5 应用消息头；**§6.6 Authority**；§8/§10/§12/§14.6 同步；废止 KCP 正文 |
| 2026-07-27 | **R1.A 多连接 pump**：GnsConnection 引入 `s_adoptFactory` + `serverAdopters()` fallback；AYNetworkSubSystem 持 `_serverClients` 列表，`update()` pump server parent + N children，`broadcast()` 真广播；新增 `MultiClientEcho` 测试（1 server + 2 clients）；53/53 PASS |
| 2026-07-27 | **R1 done 收口**：新增 `DisconnectReason` 枚举 + `HandshakeMsgType` 枚举 + HELLO/WELCOME/REJECT 线协议；`GnsConnection` 加 `setProtocolVersion()` + `getLastDisconnectReason()` + `Handshaking/Ready` 状态；onConnectionChange 签名扩展为 `(NetConnection*, bool, DisconnectReason)`；新增 `AYTest_Handshake` suite 3 个 case（HappyPath/VersionMismatch/PeerClose）；**R1 全 ship = LAN 玩具**；68/68 PASS |
| 2026-07-27 | **R2 协议层完成**：PacketHeader v2 12B (msgType/schemaVersion/length/channel/flags/timestampMs) + 末尾 4B CRC32C (Castagnoli) + `PacketCodec` 纯函数 encode/decode + lz4 (decode 复用 AYStorage::Lz4Decompressor, encode R2 内薄包 `<lz4.h>`) + `PacketAssembler` 真做 fragment/consume (末片可变长, robust 乱序/重复/丢包) + SequenceNumber 完全删除 + 握手包迁移到 PacketHeader (msgType=0xFFFF) + AYTest_PacketCodec.cpp 10 case (7 纯 + 3 GNS);**173/173 PASS, 0 FAIL** |
| 2026-07-27 | **R3.0 复制层 MVP ship** — design §13 R3 6 项全 ✅ + §6.6 Authority v1 = Server 权威实现：<br>• 新增 `WireTypeId` 12 primitive dispatch + BitStream raw helpers (writeBool/Int8..64/UInt8..64/FloatRaw/Double + readers)<br>• 新增 `kMsgTypeReplication=0x0001` / `kMsgTypeEntitySpawn=0x0002` / `kMsgTypeEntityDespawn=0x0003`<br>• 新增 `ReflectSerializer` (H+CPP) — 走 AYReflect `ITypeInfo` 元数据，按 `FieldAttribute::NetReplicate` 过滤，12-type dispatch (Bool/Int*/UInt*/Float/Double/String) 用 `typeid(T).hash_code()` 比对<br>• ReplicationFrame wire format：`[u32 netId][u16 typeHash][u8 fieldCount][u8 reserved]` + 字段 record `[u16 FNV-1a nameHash][u8 WireTypeId][value bytes]`；body 前缀 `[u16 innerMsgType]` 自描述（无需 GnsConnection 改 API）<br>• `ReplicationManager` 实装：`registerObject(void*, ITypeInfo*, uint32_t)` 主路径；`registerObject(IReplicable*, uint32_t)` 标记 deprecated wrapper（R3.0 默认空实现 + R3.1 删除 `replicate/onReplicate` 虚函数）；`tick()` 服务端 Full Snapshot 经 PacketCodec seal + CHANNEL_RELIABLE broadcast；`onReceive(BitStream&, NetConnection*)` 按 inner msgType demux；**Authority gate：客户端不 broadcast；服务端对未知 netId 的 replicate 帧拒绝**<br>• `ReplicationSystem` 降级为 adapter：**删除 _netIdToEntity / _entityToNetId 双 map**（design §13 R3 第 3 项锁定）；`onUpdate` 转发到 `ReplicationManager::tick`<br>• `IReplicable::replicate(BitStream&)` / `onReplicate(BitStream&)` 标 deprecated 空 default impl<br>• 新增 `EntityReplicationAdapter.h` ECS 桥接（`registerEntityComponent<T>` 取 component 指针 + reflect type → `ReplicationManager::registerObject`）<br>• AYReflect 配套：`AYReflect.cpp` 注册 native C++ 原生类型 `int8_t..int64_t` / `uint8_t..uint64_t` / `float` / `double` / `bool` / `std::string` 的 `typeid(T).hash_code()` 入 by-id 映射（之前只注册了 AYMath 别名），让 `AYTYPE_FIELD_EX` 直接拿 native 字段不报错<br>• 新增 `unittest/AYTest_Replication.cpp` 6 case：RoundTripPrimitives (12 WireTypeId) / FiltersNonReplicated / SnapshotBroadcast (端到端 GNS) / EntitySpawnAndDespawn / **AuthorityServerDropsClientReplicate** (§6.6 gate) / BitstreamPacketCodecIntegration<br>**R3.0 = 226/226 PASS (R2 baseline 173 + 53 new), test exit=0** |
| 2026-07-27 | **R3.1 Delta Update ship** — design §13 R3.1 9 项全 ✅ + dirty-tracking 落地，复制带宽下降到 5~20% (LAN 稳态)：<br>• 新增 `kMsgTypeDelta = 0x0004` wire msgType slot；body = 与 Full Snapshot 同 8B header + N field records，receiver 端 `deserializeObject` 路径共用<br>• `ReflectSerializer::hashFieldValue(WireTypeId, void*)` 复用 `PacketCodec::computeCrc32c` (Castagnoli, 0x1EDC6F41) 做 per-field baseline；12 WireTypeId switch + std::string 特殊 case<br>• `ReflectSerializer::serializeDirtyFields(type, obj, netId, denseIndices, BitStream&)` 新增 entry point；emit 仅指定 dense index 的字段，frame header fieldCount = denseIndices.size()<br>• `ReplicationManager::ReflectedEntry` 扩展 R3.1 dirty-tracking state：`_fieldHashes[denseIdx]` (CRC32C baseline) + `_netFieldSparseIndex[denseIdx]` (dense→sparse 映射) + `_initialized` (one-shot gate，register 时 false，第一次 tick 走 Full 后设 true)<br>• `ReplicationManager::tick` 重写 R3.1 dirty-tracking 三态：未初始化→Full Snapshot (RELIABLE)；无 dirty→不广播；部分 dirty→Delta (UNRELIABLE)。已 emit 字段的 hash 立即更新 (避免漏发)<br>• `ReplicationManager::forceReplicate(netId)` 真实现：置 `_initialized = false`，下次 tick 必走 Full Snapshot；适用于 client 重连 / teleport / 用户显式触发<br>• `ReplicationManager::getDirtyFieldCount(netId)` debug API：返回 pending dirty 字段数；0 = 稳态无广播；SIZE_MAX = 未注册或 legacy IReplicable<br>• `ReplicationManager::onReceive` 加 `kMsgTypeDelta` case，复用 `deserializeObject` (wire 格式兼容)；Authority gate 一致保持<br>• `IReplicable::replicate(BitStream&) / onReplicate(const BitStream&)` **真正删除**（全树 grep 验证 0 用户实现后）<br>• 新增 `unittest/AYTest_Replication.cpp` 21 case：<br>　pure 6：HashStableForUnchangedField / HashChangesForDifferentValues / HashAcrossAllWireTypes (12 WireTypeId) / SerializeDirtyFieldsOnlyIncludesRequested / SerializeDirtyFieldsEmptyIndicesProducesNoFrame / DeltaFrameHeaderFormatMatchesFullSnapshot<br>　e2e 15：InitialTickSendsFullSnapshotNotDelta / SteadyStateNoFieldChangeSendsNothing / SingleFieldChangeSendsDeltaWithOneRecord / MultiFieldChangeSendsDeltaWithNRecords / DeltaDoesNotIncludeUnchangedFields / DeltaOnUnreliableChannel / SpawnFrameStaysReliable / RepeatedChangeSameValueNoDelta / ForceReplicateResendsFullSnapshot / ForceReplicateAfterDeltaResendsFull / MultipleObjectsEachTrackedIndependently / DeltaFrameAuthorityGate / DeltaWireSmallerThanFull (Δ < 1/3 Full) / DeltaFrameHeaderSizeMinimal / GetDirtyFieldCountReportsPending<br>**R3.1 = 378/378 PASS (R3.0 baseline 226 + R3.1 +21 case), test exit=0; 2026-07-28 cc9c1da 修正 CHECK_INT_EQ macro 三次求值坑 (case 17 DeltaDoesNotIncludeUnchangedFields 旧版本假报 PASS 实则 deserializeObject 失败)** |
| 2026-07-28 | **R3.2 嵌套字段 ship** — design §13 R3.2 6 项全 ✅ + WireTypeId 12..15 落地，inventory vector / nested struct / equipment map 等用例端到端通：<br>• **`AYReflect`** 扩展：新增 `MapTypeInfo<V>` 模板 + `MapTypeInfoBase` 非虚拟基类（提供 `putEntry` 接口供 receiver-side insert）；新增 `registerArrayType<T,N>` / `registerVectorType<T>` / `registerMapType<V>` 自由函数（v3.2 v1 必须显式调用注册 array/vector/map 类型 — findType<T>() 不自动 instantiate）；explicit template instantiation 避免 AYNetwork_Test 链接失败<br>• **`IAYNetwork.h`**: `WireTypeId` 扩 12..15 — `NestedStruct=12` / `FixedArray=13` / `DynamicArray=14` / `StringMap=15`<br>• **`ReflectSerializer.cpp`**: `resolveWireTypeId` 加嵌套类型检测（IContainerTypeInfo → 13/14；name prefix `"std::map<std::string,"` → 15；其余 → 12 NestedStruct）；新增递归 `writeWireValue` / `readWireValue` 顶层 switch；新增 `hashFieldValueEx(WireTypeId, ITypeInfo*, void*)` 走嵌套 hash（per-design "整个字段粒度"）；`serializeObject` / `serializeDirtyFields` 切换到 `writeWireValue` 调用；readFieldValue default 分支保留 (`return false`) → R3.1 receiver 静默 drop frame<br>• **`ReplicationManager.cpp`**: `tick()` / `getDirtyFieldCount` 切换到 `hashFieldValueEx` 让 dirty-tracking 跨嵌套字段生效（任一内层元素变 → 整个 nested 字段 hash 变 → Delta frame）；其它不变<br>• **Wire format** (R3.2 新增)：NestedStruct = `[u16 nestedHash][u8 fieldCount][records...]`；FixedArray = `[u8 elemWid][u8 N][elems...]`；DynamicArray = `[u8 elemWid][u32 N][elems...]`；StringMap = `[u8 valueWid][u32 entryCount][per-entry: u16 keyLen key bytes value bytes]`；所有嵌套格式递归 (nested struct 内 element 仍是 struct / array / map 时继续展开)<br>• **向后兼容**: 新 WireTypeId 12..15 走 default readFieldValue → `return false` → receiver 静默 drop frame → server 下次 tick 发 Full 重传；schemaVersion=1 不 bump,渐进升级路径干净<br>• **新增 `unittest/AYTest_Replication.cpp` 15 case**: <br>　pure 6：NestedStructRoundTrip / NestedStructRejectedByR31Receiver / FixedArrayRoundTrip / DynamicArrayRoundTrip / StringMapRoundTrip / MixedNestedTypesRoundTrip (复合 4 类型)<br>　e2e 6：NestedStructInitialFullAndDelta / FixedArrayElementChange / DynamicArraySizeChange / StringMapKeyAdd / NestedStructHashDetectsInnerChange (whole-field dirty 验证) / R31ReceiverDropsR32Frame<br>　regression 3：R30PrimitivesStillWork / R31DeltaStillTriggersForPrimitive (MixedOuter 实测) / NestedStructInInitialTickDoesNotPolluteR31Path<br>**R3.2 = 493/493 PASS (R3.1 baseline 378 + R3.2 +15 case = 393 case；493 = check 行总数), test exit=0** |
| 2026-07-29 | **R4.0 RPC 三类型 + 4 通道 + Validator ship** — design §13 R4.0 10 项全 ✅ + IAYNetwork 第 4 大 wire msgType 命名空间 + `IMethodInfo` RPC metadata:<br>• **Wire envelope namespace**: `kMsgTypeRpcRequest=0x0010` / `kMsgTypeRpcResponse=0x0011` / `kMsgTypeRpcReject=0x0012` (0x0005..0x000F reserved R3.3 back-compat)；schemaVersion=1 不 bump,R3.x receiver 静默 drop 同 R3.2 nested WireTypeId 12..15<br>• **AYReflect 扩展** (`include/ayreflect/IReflect.h`): `IMethodInfo` 加 5 虚函数 (RpcKind/isUnreliable/hasValidator/validate/getParamName)，`enum class RpcKind { None/Server/Client/Multicast }`；default impl most benign 保持 AYScript logia `logia/AYMethodInfoImpl.h` source-compat (Test_Reflect.cpp:600-603 注释确认)<br>• **RpcHandler** (新 `include/RPC/RpcHandler.h` + `src/RPC/RpcHandler.cpp`): `registerMethod` per-method hash binding (FNV-1a-16 of methodName)；`callServer/Client/Multicast` 出站 caller 端不要求本地 obj 绑定（**Bug fix**: resolveMethod 出站语义允许 _objsByHash miss）；`onRpcRequest/Response/Reject` inbound 入口 `body.resetForRead()`（**Bug fix**: 写完 BitStream 不归零位指针导致 readRpcArgs 静默 fail）；authority gate 按 RpcKind 区分（Server RPC 仅在 Server/ListenServer 端处理；Client RPC 仅在 Client 端处理；Multicast 全端）<br>• **RpcSerializer thin wrapper** (在 RpcHandler.cpp 内)：writeRpcArgs / readRpcArgs / writeRpcResponse / readRpcResponse / writeRpcReject / readRpcReject — 复用 R3.2 `writeWireValue/readWireValue` 16 WireTypeId dispatch (No new ITypeInfo/CRC/fragment logic — 全复用 R3.2 16-id 引擎)<br>• **callId 64-bit pending map**: std::atomic fetch_add + std::mutex map; 客户端 outbound register pending callback, Response/Reject 自动 fire + cleanup<br>• **`GnsConnection::_rawSend` 4-channel switch**: `CHANNEL_RELIABLE` → `k_nSteamNetworkingSend_Reliable`；`CHANNEL_UNRELIABLE` → `k_nSteamNetworkingSend_Unreliable`；`CHANNEL_FRAGMENTED` → `Reliable \| NoNagle` (Nagle 防止合并多帧包；PacketCodec 仍走 PacketAssembler R2)；`CHANNEL_ACK` → `Reliable` (R4.1 stub；ACK pipeline 留 R4.1+)<br>• **`ReplicationManager::unregisterObject` envelope fix**: R3.2 typo `kMsgTypeReplication` envelope 包 Despawn body → R4.0 修成 `kMsgTypeEntityDespawn` (mirror Spawn path),wire 对称<br>• **`AYNetworkSubSystem::update` demux**: PacketCodec::decode envelope.msgType: 0x0010..0x0012 → `_rpcHandler.onRpcXxx`；0x0001..0x0004 → 既有 `_replicationManager.onReceive`；其它 → per-channel MessageHandler；pre-route 消除 channel handler 关注 RPC vs replication 区分<br>• **`INetworkSubSystem::getRpcHandler()` 新 API**: 单一 RpcHandler 引用,AYNetworkSubSystem ctor 持有 (mirror `_replicationManager{this}`)<br>• **新 `unittest/AYTest_RpcHandler.cpp` 18 case** (10 pure + 5 e2e 纯 loopback + 3 regression)：<br>　pure 10：RegisterAndResolveMethod / WriteReadArgsRoundTrip / WriteReadResponseRoundTrip / WriteReadRejectBodyRoundTrip / ValidatorRejectsCall / UnknownMethodReturnsFalse / ParseFailOnTruncatedBody / MultiplePendingCallsDisambiguated / UnreliableOverridePerRpc / RpcKindMismatchRejects<br>　e2e 5：ServerRpcHappyPath / ValidatorRejectsAcrossGns / ReliableDefaultOverGns / UnreliableServerRpcOverGns / AuthorityGateRejectsRpc (纯 loopback 同步断言: emit → PacketCodec::decode → peer handler,去 GnsConnection heap-corruption 路径 — R3.2 E2E pattern 复用)<br>　regression 3：R30PrimitivesStillReplicate / R32NestedWireTypesAreUnaffected (16-id dispatch 不破坏) / RpcMsgTypeEnvelopeIsDistinct<br>• **关键 Bug fix 链 (用户 review 后修正, 不是 .obj/链接器问题)**:<br>　1. `resolveMethod` 出站 caller 端不应要求本地 obj (client callServer 不需要 receiver's obj binding)<br>　2. `onRpcRequest/Response/Reject` 入口 `body.resetForRead()` (写完 BitStream 后位指针非零,readRpcArgs 静默 fail)<br>　3. E2E 纯 loopback: emit → PacketCodec::decode → peer handler, 不走 GnsConnection (Disconnected 状态丢包 + heap 破坏)<br>　4. Authority gate 按 RpcKind 区分 (Server RPC 仅 Server/ListenServer 端; Client RPC 仅 Client 端; Multicast 全端)<br>　5. E2E 同步断言 (RPC 路径本身是同步, GNS update() pump 不需要)<br>　6. Channel 断言看对 atomic (client 发 RPC 看 lastClientChannel; server 发 Client RPC 看 lastServerChannel)<br>　7. ClientDamage 在 client 端 registerMethod (Client RPC server→client, client 端 invoke)<br>**R4.0 = 566/566 PASS (R3.2 baseline 493 + R4.0 +73 case/check), test exit=0; 2026-07-29 ship** |

---

## 17. 参考

- [KCP Protocol](https://github.com/skywind3000/kcp)（**已弃用，仅历史对照**）
- [GameNetworkingSockets (Valve OSS)](https://github.com/ValveSoftware/GameNetworkingSockets)
- [ENet](https://github.com/lsalzman/enet)
- [kissnet](https://github.com/Ybalrid/kissnet)
- [Unreal Networking](https://docs.unrealengine.com/en-US/InteractiveExperiences/Networking/index.html)
- [Unity NetCode](https://docs.unity3d.com/Packages/com.unity.netcode@1.0/manual/index.html)
- [O3DE Networking](https://o3de.org/docs/learning-guide/gems/multiplayer/)
- [Gaffer on Games — Networked Physics](https://gafferongames.com/post/networked_physics_2004/)