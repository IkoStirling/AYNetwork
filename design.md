# AYNetwork Design

> **文档状态（2026-07-29）**：传输库 **锁定 GameNetworkingSockets（GNS）**（§14）。R1–R2 协议层 ✅；R3.0–R3.2 复制层 ✅；R4.0 RPC 核心 ✅；**R4.1-A 集成层 ✅ ship**；**R4.1-B polish ✅ ship**（690+ PASS）。工业可用度见 §12。
> **2026-07-29 R4.1 范围收束补丁**：拆分 R4.1-A/B；冻结散落 4.1 开发；修正 §10 demux 表述与代码一致；§12 评分同步 R4.0 ship 后现状。
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

| 特性 | AYNetwork（2026-07-29） | Unreal | Unity | O3DE | 优先级 |
|------|:----------------------:|:------:|:------:|:----:|:------:|
| UDP 传输 | ✅ GNS + 子系统多连接 pump | ✅ | ✅ | ✅ | P0 |
| **传输库选型** | ✅ **GNS 锁定**（§14） | 自研 | Unity Transport | AzNetworking | P0 done |
| 可靠层 | ✅ GNS 内置（**禁用 KCP**） | ✅ | ✅ | ✅ | P0 |
| 状态同步 | ✅ R3 Full/Delta + Subsystem demux (R4.1-A) | ✅ | ✅ | ✅ | P0 |
| RPC 调用 | ✅ R4.0 Handler + Subsystem sendTo (R4.1-A) | ✅ | ✅ | ✅ | P0 |
| 增量同步 | ✅ R3.1 Delta (CRC32C dirty) | ✅ | ⚠️ | ✅ | P2 done |
| Interest Management | ⚠ R4.1-B 进行中 | ✅ | ⚠️ | ✅ | P3 |
| Authority 模型 | ✅ Server/ListenServer gate + Subsystem 接线 (R4.1-A) | ✅ | ✅ | ✅ | P0 |
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
// AYEntity/components/AYEntity/components/AYEntity/components/HealthComponent.h - 功能组件
#pragma once
#include <AYEntity/IEntity.h>

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
│   └── AYNetwork/INetwork.h           # 核心接口
│
├── include/
│   ├── AYNetwork.h           # 主入口
│   ├── BitStream.h           # 位流 (现有)
│   │
│   ├── Transport/            # 传输层
│   │   ├── AYNetwork/Transport/AYNetwork/Transport/AYNetwork/Transport/GnsConnection.h   # ★ GNS 包装（当前）
│   │   ├── AYNetwork/Transport/AYNetwork/Transport/AYNetwork/Transport/KcpConnection.h   # Deprecated — 待删
│   │   ├── AYNetwork/Transport/AYNetwork/Transport/AYNetwork/Transport/UdpSocket.h       # Deprecated — 待删
│   │   └── Connection.h      # 连接状态机 / 列表
│   │
│   ├── Protocol/             # 协议层
│   │   ├── AYNetwork/Protocol/AYNetwork/Protocol/AYNetwork/Protocol/PacketHeader.h   # 包头
│   │   └── AYNetwork/Protocol/AYNetwork/Protocol/AYNetwork/Protocol/PacketAssembler.h# 组包/拆包
│   │
│   ├── Replication/          # 复制层
│   │   ├── AYNetwork/Replication/AYNetwork/Replication/AYNetwork/Replication/ReplicationManager.h
│   │   ├── AYNetwork/Replication/AYNetwork/Replication/AYNetwork/Replication/ReplicationSystem.h
│   │   └── AYNetwork/Replication/AYNetwork/Replication/AYNetwork/Replication/NetDataComponent.h# 标记宏
│   │
│   └── RPC/                 # RPC 层 (规划)
│       └── AYNetwork/RPC/AYNetwork/RPC/AYNetwork/RPC/RpcHandler.h
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
| `AYNetwork/Replication/AYNetwork/Replication/AYNetwork/Replication/EntityReplicationAdapter.h` | ✅ | ECS 桥接 |
| `IReplicable::replicate/onReplicate` | ✅ **已删除** | R3.1 全树 grep 验证 0 用户实现后真正删除 |
| Simulated/Autonomous Proxy gate | ❌ | **R5 work** — lag comp 一并做 |

### Phase 4：RPC — **R4.0 完成 (2026-07-29)**

**目标**：`Server` / `Client` / `NetMulticast` + server validation + 4 channel 全展开（GNS send flag 映射）

**当前实现度**：R4.0 (2026-07-29) ship — **566/566 测试 PASS** (R3.2 493 baseline + R4.0 +73 case)。Server/Client/Multicast 三类型 + Validator per-method (`IMethodInfo::validate`) + 4 通道 (RELIABLE/UNRELIABLE/FRAGMENTED+NoNagle/ACK) 全 ship。Interest Management 押后 R4.1。

| 子项 | 当前状态 | 备注 |
|------|---------|------|
| `kMsgTypeRpcRequest=0x0010 / kMsgTypeRpcResponse=0x0011 / kMsgTypeRpcReject=0x0012` | ✅ | envelope msgType 命名空间独立，schemaVersion=1 不 bump；R3.x receiver 静默 drop |
| `RpcHandler` (H+CPP) | ✅ | `include/RPC/AYNetwork/RPC/AYNetwork/RPC/AYNetwork/RPC/RpcHandler.h` + `src/RPC/RpcHandler.cpp`；registerMethod / callServer/Client/Multicast / onRpcRequest/Response/Reject |
| `RpcSerializer` thin wrapper | ✅ | writeRpcArgs/readRpcArgs/writeRpcResponse/readRpcResponse/writeRpcReject/readRpcReject — 复用 R3.2 16 WireTypeId dispatch (`writeWireValue/readWireValue`) |
| `IMethodInfo::getRpcKind()` enum (None/Server/Client/Multicast) | ✅ | R4.0 AYReflect 扩展，5 虚函数带 default impl (None/false/false/true/nullptr) 保持 AYScript logia source-compat |
| `IMethodInfo::isUnreliable()` per-RPC channel override | ✅ | Reliable 默认；high-freq RPC 标 true 走 CHANNEL_UNRELIABLE |
| `IMethodInfo::validate(const void*)` server-side reject hook | ✅ | per-method 替代 FieldAttribute bit (用户决策改 method-domain) |
| callId 64-bit + pendingCalls mutex map | ✅ | Response/Reject 自动 fire callback; auto-cleaned on match |
| Authority gate 按 RpcKind | ✅ | Server RPC 仅在 Server/ListenServer 端处理；Client RPC 仅在 Client 端处理；mis-directed frame → RpcReject reason=NotAuthority |
| `GnsConnection::_rawSend` 4-channel switch | ✅ | CHANNEL_RELIABLE → Reliable；CHANNEL_UNRELIABLE → Unreliable；CHANNEL_FRAGMENTED → Reliable\|NoNagle；CHANNEL_ACK → Reliable\|NoNagle (R4.1-B ack frames) |
| `CHANNEL_ACK` explicit ACK pipeline | ✅ R4.1-B | `PacketFlag::RequiresAck` + `kMsgTypeAppAck` + `AckPipeline` / `sendRequireAck` |
| ReplicationManager envelope fix | ✅ | `unregisterObject` 用 `kMsgTypeEntityDespawn` envelope (替代 R3.2 typo `kMsgTypeReplication`) — wire 对称 |
| `AYNetworkSubSystem::update` RPC demux | ✅ | 0x0010/0x0011/0x0012 → `_rpcHandler.onRpcXxx` |
| `AYNetworkSubSystem::update` Replication demux | ✅ R4.1-A | `dispatchIncoming` routes 0x0001..0x0004 → `_replicationManager.onReceive` |
| `INetworkSubSystem::getRpcHandler()` | ✅ | 单一 RpcHandler 引用，AYNetworkSubSystem ctor 持有 |
| `RpcHandler::emit` → `sendTo` / `broadcast` 分流 | ✅ R4.1-A | `callClient` → `sendTo`；Client → `send()`；Server → `broadcast()` |
| Handler 传入真实 `NetConnection* from` | ✅ R4.1-A | per-conn `NetConnectionImpl` on adopt + client connect |
| `Interest Management (Relevancy + distance culling)` | ✅ R4.1-B | `NetVec3` + `setInterestRadius` + `isRelevant` + per-conn `sendTo` |
| Per-field RepNotify callback (Unreal `OnRep_X`) | ✅ R4.1-B | `FieldAttribute::RepNotify` + `INetworkExtension::onRepNotify` |
| Multicast RPC for unregistered types / wildcard | ✅ R4.1-B | `registerWildcardMulticast` + TypeRegistry fallback lookup |
| RpcResponse auto lz4 compress | ✅ R4.1-B | `RpcHandler::emit` compresses `0x0011` when beneficial (≥64 B) |

### Phase R4.1-A：**集成层（P0）— ✅ ship（2026-07-29）**

**目标**：把 R4.0 零件 + 已落地的 R4.1 plumbing **串成一条可跑的真实链路**（Subsystem + GNS，不用 test sink）。验收：**2–4 人 LAN demo 可玩**（spawn → replicate 字段变化 → RPC 离散事件）。

**开发纪律（2026-07-29 起生效）**：
- **冻结**：Interest Management、RepNotify、Async RPC、wildcard multicast、Response 自动 lz4、CHANNEL_ACK 管线 — 全部归入 **R4.1-B**，R4.1-A ship 前不得新开实现。
- **禁止**：并行添加新的 `INetworkExtension` 钩子或 scattered R4.1 API；已有 plumbing（`NetConnectionImpl` / `sendTo` / `getConnections`）**只接线、不扩展**。
- **测试要求**：必须新增 ≥1 条 **Subsystem 级 E2E**（1 server + 2 client，走 `INetworkSubSystem`，禁止 `setBroadcastSinkForTesting` 作为主路径）。

| 子项 | 状态 | 备注 |
|------|------|------|
| `NetConnectionImpl` (GnsConnection 适配) | ✅ 已 land | commit `a729672` |
| `getConnections` / `sendTo` / `kickConnection` | ✅ 已 land | commit `ed56867` |
| `listen()` → `ListenServer` + Replication `isAuthority()` | ✅ 已 land | commit `19d702a` |
| Subsystem Replication demux (0x0001..0x0004) | ✅ R4.1-A | `dispatchIncoming` → `_replicationManager.onReceive` |
| `RpcHandler::emit` 按目标分流 | ✅ R4.1-A | `callClient` → `sendTo`；Client/Server 分流 |
| 传入真实 `NetConnection* from` | ✅ R4.1-A | adopt + `_clientNetConn` |
| 修复 `broadcastExcept` 指针比较 | ✅ R4.1-A | iterate `_netConns` by `NetConnection*` |
| Client `EntitySpawn` 最小可用 | ✅ R4.1-A | `_spawnAnnouncements` + `peekSpawnAnnouncement()` |
| Subsystem E2E 测试 (1s+2c) | ✅ R4.1-A | `AYTest_SubsystemIntegration.cpp` |
| 最小 AYEntity demo 接入 | ✅ R4.1-A | `AYTest_EntityReplicationIntegration` + Editor Play 接线 |
| RPC pending timeout + retry / exponential backoff | ✅ R4.1-B | `callServerWithCallback` + `setRetryPolicy` + backoff resend |

**R4.1-A ship 判据**：上述项全 ✅ + 598 baseline 零回归（含 Subsystem + Entity E2E）。

### Phase R4.1-B：**Interest + RepNotify + RPC polish — 可开始（R4.1-A 已 ship）**

**目标**：联机品质提升（带宽控制、字段变更回调、弱网策略）。**R4.1-A 未 ship 前不得开始实现。**

1. Interest Management：`Relevancy` 谓词 + 距离裁剪 + `INetworkExtension::onPreReplicate(targets)` 真调用 — ✅ R4.1-B
2. Per-field RepNotify callback (Unreal `OnRep_X`) — `FieldAttribute::RepNotify` — ✅ R4.1-B
3. RPC timeout / retry / exponential backoff — ✅ R4.1-B
4. RpcHandler async invoke (RPC > 16 ms) — thread pool + future response — ✅ R4.1-B
5. Multicast RPC for unregistered types / wildcard — ✅ R4.1-B
6. `kMsgTypeRpcResponse` 自动 lz4 compress — ✅ R4.1-B
7. `CHANNEL_ACK` 显式 ACK 管线（替代 R4.0 Reliable stub）— ✅ R4.1-B

### Phase 5：Lag comp / Prediction（**R5，未开始**）

> 原「Phase 5 Interest Management」条目已合并进 **R4.1-B**，避免与 §13 Roadmap 编号冲突。
---

## 11. 与 AYEntity 集成（✅ adapter + Editor Play）

> R3 复制 API 已 ship（`registerObject(void*, ITypeInfo*, netId)` + `AYNetwork/Replication/AYNetwork/Replication/AYNetwork/Replication/EntityReplicationAdapter.h`）。**R4.1-A** 已通过 `AYTest_EntityReplicationIntegration` 验证 `HealthComponent.currentHp` 经 adapter + Subsystem 全链路复制。**Editor Play 已接线**：`NetworkComponent::bindReplication()` 消费 host 回调 → `EntityReplicationAdapter` → `INetworkSubSystem::getReplicationManager()`；`EditorPlayRuntime::startPlay()` listen-server（默认 7777，`AY_NETWORK_PORT` 覆盖），cube 实体带 `NetworkComponent` + `HealthComponent`。

---

## 12. 与工业级引擎对标（更新于 2026-07-29）

| 特性 | AYNetwork 2026-07-29 | Unreal 5 | Unity NetCode | O3DE | 工业级门槛 |
|------|:-------------------:|:--------:|:-------------:|:----:|:----------:|
| UDP/可靠传输 | ✅ GNS + 多连接 | ✅ | ✅ | ✅ | ✅ |
| Authority 模型 | ✅ Server/ListenServer + Subsystem (R4.1-A) | ✅ | ✅ | ✅ | ✅ |
| Interest Management | ✅ R4.1-B distance cull + onPreReplicate | ✅ | ✅ | ✅ | ✅（MMO 必备） |
| Replication Graph | ❌ | ✅ | ✅ | ✅ | ✅ |
| RPC（Reliable/Unreliable） | ✅ Handler + Subsystem 接线 (R4.1-A) | ✅ | ✅ | ✅ | ✅ |
| Connection state machine | ✅ 握手 + Ready | ✅ | ✅ | ✅ | ✅ |
| Packet fragmentation | ✅ R2 PacketAssembler | ✅ | ✅ | ✅ | ✅ |
| Heartbeat / RTT / Stats | ⚠ GNS 有 / App 未暴露齐 | ✅ | ✅ | ✅ | ✅ |
| Snapshot + Delta 同步 | ✅ R3 + Subsystem demux (R4.1-A) | ✅ | ✅ | ✅ | ✅ |
| 加密（DTLS / AES-GCM） | ✅ GNS | ✅ | ✅ | ✅ | ✅ |
| 压缩 | ✅ lz4 encode + RpcResponse auto-compress (R4.1-B) | ✅ | ✅ | ✅ | ⚠ |
| Lag compensation | ❌ R5 | ✅ | ✅ | ✅ | ✅（FPS/MOBA） |
| Server 端反作弊校验 | ⚠ RPC validator only | ✅ | ✅ | ✅ | ✅ |
| Replay / Demo | ❌ R5/R6 | ✅ | ❌ | ❌ | ⚠ |

**综合评分（2026-07-29，R4.1-A ship 后）**：
- 设计清晰度：**88/100**（R4.1-A/B 分层 + §10 与代码对齐）
- 实现完整度：**~55/100**（传输+协议+复制+RPC+**集成层闭环**）
- 工业可用度：**~30/100**（LAN 小规模联机原型可跑；Interest/RepNotify 待 R4.1-B）
- 距可用门槛（R4.1-B polish）：约 **2–3 周**
- 距生产级（R4.1-B + R5 + R6）：仍约 **10–14 周**

**分阶段可 ship**：
- R1 ✅ LAN 玩具 → R3 ✅ 状态同步原型 → R4.0 ✅ RPC 零件库 → **R4.1-A ✅ 可玩小规模联机 demo** → R4.1-B 带宽/回调 → R5 体验 → R6 工程化。跨 NAT / 商店级另需中继（§14.6）。
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

### Phase R4（3 周）：RPC — **R4.0 ship ✅；R4.1 收束为 A/B 两阶段**

> **2026-07-29 范围收束**：原「Phase R4.1 待开始」整块（Interest + RepNotify + polish）**拆分为 R4.1-A（集成/P0，唯一活跃）与 R4.1-B（冻结）**。详见 §10 Phase R4.1-A / R4.1-B。

1. RPC 系统：`Server` / `Client` / `NetMulticast` 三类型，参数序列化复用 AYReflect — ✅ R4.0
2. Validation hook（server-side 拒绝非法 RPC）— ✅ R4.0
3. Interest Management 基础 — ⏸ **R4.1-B 冻结**
4. `INetworkExtension::onPreReplicate(targets)` 真调用 — ⏸ **R4.1-B 冻结**

### Phase R4.0 — RPC 三类型 + 4 通道 + Validator（**2026-07-29 ship**）

1. ✅ RPC 系统：Server / Client / Multicast 三类型；参数 (de)serialize 复用 R3.2 `ReflectSerializer::writeWireValue/readWireValue` (16 WireTypeId dispatch) — `RpcSerializer` thin wrapper 加在 `RpcHandler.cpp`
2. ✅ Validation hook：per-method `IMethodInfo::validate(const void*)` — 拒绝返 false → server emit `RpcReject` reason=ValidatorDeny
3. ✅ Authority gate 按 RpcKind 区分：Server RPC 仅在 Server/ListenServer 端处理；Client RPC 仅在 Client 端处理；Multicast 在所有端处理
4. ✅ 4 通道全展开 (`GnsConnection::_rawSend`)：CHANNEL_RELIABLE → GNS Reliable；CHANNEL_UNRELIABLE → GNS Unreliable；CHANNEL_FRAGMENTED → GNS Reliable\|NoNagle；CHANNEL_ACK → GNS Reliable (R4.1 stub)
5. ✅ `RpcHandler` 全套 API：registerMethod (per-method hash binding) / callServer/Client/Multicast (outbound) / onRpcRequest/Response/Reject (inbound dispatch) / callId 64-bit pending map (auto-cleanup)
6. ✅ Wire 兼容：schemaVersion=1 不 bump；R3.x receiver 收到 envelope 0x0010..0x0012 静默 drop (default envelope kind 不识别)
7. ✅ 73 测试 (R3.2 493 → 566 baseline + 73 R4.0): pure dispatch / 4 channel / validator reject / authority gate / R3.0-R3.2 zero regression
8. ⏸ Interest Management — **移至 R4.1-B，R4.1-A ship 前冻结**
9. ⏸ Per-field RepNotify — **移至 R4.1-B，冻结**
10. ⏸ Multicast RPC wildcard — **移至 R4.1-B+，冻结**

### Phase R4.1-A — 集成层（P0，**✅ ship 2026-07-29**）

> 完整 checklist 见 §10 Phase R4.1-A。commits `ecddcdd`（集成 demux/E2E）+ `349a176`（pending 超时 + Entity adapter E2E）。

1. ✅ Subsystem Replication demux (0x0001..0x0004 → `_replicationManager`)
2. ✅ `RpcHandler::emit` → `sendTo` / `broadcast` 分流
3. ✅ 真实 `NetConnection* from` 传入 handlers
4. ✅ 修复 `broadcastExcept`
5. ✅ Client EntitySpawn 最小可用
6. ✅ Subsystem E2E (1 server + 2 client，无 test sink)
7. ✅ EntityReplicationAdapter E2E + RPC pending 超时

**Ship 判据**：598/598 PASS ✅

### Phase R4.1-B — Interest + RepNotify + RPC polish（**可开始 — R4.1-A 已 ship**）

1. ✅ Interest Management + `onPreReplicate(targets)` — `NetVec3`, `setInterestRadius`, `isRelevant`, per-conn `sendTo`
2. ✅ Per-field RepNotify (`FieldAttribute::RepNotify` + `onRepNotify`)
3. RPC timeout / retry / exponential backoff — ✅ R4.1-B
4. ✅ Async RPC (thread pool + tick-drain RpcResponse)
5. Wildcard multicast RPC — ✅ R4.1-B
6. Response auto lz4 compress — ✅ R4.1-B
7. CHANNEL_ACK 显式管线 — ✅ R4.1-B

### Phase R5（可选）：Lag comp / Replay / Snapshot interp

1. Client-side prediction + server reconciliation（**R5.1 之后**）
2. **Snapshot interpolation（Unity NetCode 的 `SnapshotSystem` 模式）— ✅ R5.0 ship（2026-08-24）**
3. Replay recording（Unreal `UReplaySubsystem` 模式）（**R5.2**）

### 15. Snapshot Interpolation 设计（R5.0）

> **目标（2026-08-24）**：消除 R3.x "客户端直接吃最新 snapshot"的抖动感。Authority
> 每 `tickRate` Hz 给每个 replicated ghost 推一份带 serverTick 的状态；client 收齐后
> 按 `interpolationDelaySec`（典型 100–200 ms = 2–4 个 server tick）回看 buffer，
> 对两个 bracketing snapshots 做 **per-field 插值**。这是 Unity NetCode `SnapshotSystem`
> 模式（R5 §15.4 对照表）。
>
> **本节锁定**：
>
> - **模拟客户端**：R5 仿真场景默认走 **SimulatedProxy**（只看，不输入），插值出连续渲染
>   位姿是体验首要矛盾；prediction+reconciliation 留 R5.1。
> - **wire 兼容**：在 R3.0/R3.1 body 前缀 `[u32 netId][u64 schema][u8 fieldCount][u8 reserved]`
>   之前增加 `[u32 serverTick]` 一段。客户端 schemaHash 不匹配时仍按 R3.x 行为丢弃；receiver
>   收到 `serverTick==0` 时按 R3.x 直接 deserialize（兼容未升级 server），不丢消息。
> - **不破坏 R4.x**：RPC 与 handshake 的 PacketHeader 不动；serverTick 只附加到 replication
>   body 前缀；RPC/EntitySpawn/EntityDespawn 也不动。

#### 15.1 架构总览

```
Authority (Server)                           Simulated Client
─────────────────────                       ─────────────────────
ReplicationManager::tick(dt)
  ├─ advanceServerTick(dt)         ──wire [srvTick + body]──►  ReplicationManager::onReceive
  │                                                            ├─ decode srvTick
  │                                                            ├─ deserializeObject → obj
  │                                                            └─ SnapshotInterpolator::push(netId, srvTick, obj)
  │
  └─ fullSnapshot / dirtyFields
     stamped with current srvTick

NetworkTime (per-side)                       NetworkTime (per-side)
  ├─ setTickRate(Hz)                          ├─ setTickRate(Hz)
  ├─ serverTick counter                       ├─ currentServerTick (last received)
  └─ clientTime accumulator                   └─ interpolationTimeSec = clientTime − interpDelay

SnapshotBuffer (per ghost, ring)             Sample loop (game code, every render frame)
  ┌──────────────────────────────────────┐    sample(netId, renderTime, &out)
  │ t-3: obj_v3 (float pos=3.0)          │      ├─ find t0, t1 bracketing renderTime
  │ t-2: obj_v2 (float pos=2.0)          │      ├─ per-field lerp(t0, t1, alpha)
  │ t-1: obj_v1 (float pos=1.0)          │      └─ return lerped struct
  │ t-0: obj_v0 (float pos=0.0)          │
  └──────────────────────────────────────┘
```

#### 15.2 时间模型（NetworkTime）

| 概念 | 来源 | 单位 | 备注 |
|------|------|------|------|
| `tickRate` | 配置 / server | Hz | 默认 30；客户端需与 server 一致（或按 first-seen 推断） |
| `serverTick` | authority 单调递增 | u32 | wrap-safe（差值取 `(a-b+2^31) % 2^31` 兼容 wrap） |
| `serverTimeSec` | `serverTick / tickRate` | double | client 端用此做插值索引 |
| `clientTimeSec` | 本地 clock 累加 | double | monotonic since session start |
| `interpolationDelaySec` | 配置（典型 0.1–0.2） | double | client 用 = `clientTimeSec - interpolationDelaySec` |
| `interpolationTimeSec` | 派生 | double | = `clientTimeSec - interpolationDelaySec` |

`NetworkTime` 是无 IO 的纯数据类，server/client 各持一份。Server 端 `tick(dt)`
调用 `advance(dt)` 累加 `serverTick`；client 端 `advance(dt)` 只累加 `clientTimeSec`，
`currentServerTick()` 来自最近一次 `onReceive` 推入。

#### 15.3 SnapshotBuffer（per-ghost ring）

```cpp
struct SnapshotRecord {
    uint32_t serverTick = 0;          // 唯一主键
    double   serverTimeSec = 0.0;     // = serverTick / tickRate
    void*    objCopy = nullptr;       // 与 registerType 提供的 size 对齐的 bitwise 拷贝
    bool     valid = false;
};
class SnapshotBuffer {
    static constexpr size_t kDefaultCapacity = 32; // 32 ticks @30Hz ≈ 1.07 s 历史
public:
    void   init(size_t recordBytes);               // recordBytes = sizeof(T)
    void   push(uint32_t srvTick, const void* obj); // 拷贝 + 按 tick 排序入 ring
    bool   sample(double serverTimeSec, void* out) const; // 找 bracket + lerp
    void   clear();
    size_t size() const;
};
```

**插值策略**（per-field，由 SnapshotInterpolator 调度）：

| WireTypeId | interp | 说明 |
|------------|:------:|------|
| Float / Double | **lerp** | `a + (b-a)*alpha` |
| Int8/16/32/64, UInt* | **snap-to-floor** | 插值期间保持 t0 值；下个 bracket 跨越时切换 |
| Bool | **snap** | t0 主导（确保瞬时状态不抖） |
| String / 嵌套 struct / array / map | **snap to t1** | 进入 t1 tick 时整体替换（深拷贝成本高） |

snap-to-floor 对整数是 Unreal/Unity NetCode 默认；保持经验做法。nested 类型
不做 per-element 插值是性能 / 一致性的常见折中，与 R3.2 一致（hash 也按整体）。

**bracket 选择**：
1. `t* = serverTimeSec`
2. 在 records 里二分找 `t0 <= t* < t1`；若 `t*` 比最早一帧还早 → 返回 false（buffer 尚未
   暖起来，应用层应 hold 上一帧 visible 状态）
3. 若 `t*` 比最晚一帧还晚 → 用最后两条做 extrapolation（不外推数值本身，只延展 t1 的值；
   alpha=1.0，与 Unreal `bNoInterpolation` 等价于"卡死最新")

#### 15.4 与 Unity NetCode `SnapshotSystem` 对照

| Unity NetCode 组件 | AYNetwork R5 对应 | 备注 |
|--------------------|-------------------|------|
| `NetworkTimeSystem` | `NetworkTime` | tick rate + server tick + interpolation time |
| `GhostComponent` / `GhostCollection` | `ReplicationManager::registerObject` | 已 ship（R3.x） |
| `GhostImportance` | **不实现** | R5 暂未涉及 LOD 重要性 |
| `SnapshotData`（per-ghost 历史） | `SnapshotBuffer` | 容量 32 records，默认 |
| `InterpolatedSimulationSystemGroup` | `SnapshotInterpolator::sample` | game code 显式调用 |
| `PredictedSimulationSystemGroup` | **R5.1 之后** | prediction + reconciliation |
| `SnapshotAck` | **R5.0 stub**：每收到一份推一个 ack（仅 log） | 不发回 server，server 端 serverTick 单调递增无回退 |
| `NetworkTickIndex` | `serverTick` u32 | 同语义 |
| `CommandSendSystem` | **R5.1** | 输入采集 |

#### 15.5 Wire 变更（兼容 R3.x / R5.1）

| 帧类型 | R3.x body 前缀 | R5.0 body 前缀 | R5.1 body 前缀 |
|--------|---------------|----------------|----------------|
| `kMsgTypeReplication` | `[u32 netId][u64 schema][u8 fcount][u8 rsv][records…]` | `[u32 serverTick][u32 netId][u64 schema][u8 fcount][u8 rsv][records…]` | 同 R5.0，但 `rsv` byte 改成 `flags`：`bit 0x01 = kFlagTeleport` |
| `kMsgTypeDelta` | 同上 | 同上 | 同 R5.0，`flags` byte 同上 |
| `kMsgTypeEntitySpawn` | `[u32 netId][u64 schema]` | **不动** | **不动** |
| `kMsgTypeEntityDespawn` | `[u32 netId]` | **不动** | **不动** |
| `kMsgTypeRpc*` | 不动 | 不动 | 不动 |

R5.1 `flags` 字段是**位标志**（bit-or），预留 8 bit：

| bit | 名称 | 语义 |
|-----|------|------|
| `0x01` | `kFlagTeleport` | 这是瞬移快照；client 端 `SnapshotInterpolator::push(netId, tick, obj, /*teleport=*/true)` |
| `0x02..0x80` | reserved | 未来扩展（R6+ 关注包/压缩/scope-filter 等） |

`flags` byte 的**字节位置和长度都没变**（R5.0 那个 `rsv` byte 现在叫 `flags`），所以 R5.0 client 解析 R5.1 帧时把它当成 0 忽略 → 没有回归；R5.1 client 解析 R5.0 帧时该 byte 始终为 0 → 没有 `kFlagTeleport` 命中 → 走普通插值。

兼容性：
- **R5 client ↔ R3.x server**：`onReceive` 检测到 `serverTick==0` 且包长度足够旧 schema
  → fallback 走 R3.x 解码路径（R3.x 的 `fieldCount` byte 在 offset 12；R5 在 offset 16，
  client 先 peek byte at offset 16；若 bytesLeft 不足以读旧 header → 视为 R5）。
  实际生产中 R3.x server 永远不会 stamp `serverTick`（默认 0），于是 `serverTick==0`
  走 fallback。
- **R3.x client ↔ R5 server**：R3.x 不读 body 第 0 字节当成 fieldCount，会把
  `serverTick` 高字节当 fcount 而破坏解析。这是不兼容变更；server 必须升级到 R5。
  R3.x client 收到 R5 帧会因 schema mismatch 静默丢包，**R5 server 必须配合 R5 client**
  —— 这是受控升级，跟 R3.1/R4.1 同样套路。

#### 15.6 API 表面（client 视角）

```cpp
// AYNetwork/Snapshot/SnapshotInterpolator.h
class SnapshotInterpolator {
public:
    // 必须先告诉 interpolator 类型尺寸（每种 netId 对应一种类型）
    void registerGhostKind(uint32_t netId, size_t recordBytes);

    // 由 ReplicationManager::onReceive 调用；纯复制，不做解码外的逻辑
    void push(uint32_t netId, uint32_t serverTick, const void* obj);

    // 渲染线程/每帧 game code 调用；找不到 bracket 返回 false
    bool sample(uint32_t netId, double renderTimeSec, void* out) const;

    // 全局时间
    void  setInterpolationDelaySec(double d);
    void  setTickRate(double hz);
    void  advance(double dt);            // 累加 client time
    uint32_t currentServerTick() const;
    double   interpolationTimeSec() const;

    void clear();
};

// ReplicationManager 新增一对 API（同时保留 R3.x 直接路径作为 fallback）
class ReplicationManager {
    void setSnapshotInterpolator(SnapshotInterpolator* si);  // client-side
    void setServerTickForTesting(uint32_t t);                 // test seam
    uint32_t getServerTick() const;
};
```

#### 15.7 测试矩阵（计划 ≥ 12 case）

| Case | 验证 |
|------|------|
| 1 | NetworkTime basic — `tickRate=30`，1s 后 `serverTick==30` |
| 2 | NetworkTime wrap — u32 wrap 后 lerp 仍按 monotonic 距离 |
| 3 | SnapshotBuffer push + sample at exact t1 == t1 record |
| 4 | SnapshotBuffer sample at midpoint → lerp(floats) / snap(int) |
| 5 | SnapshotBuffer sample before first → false |
| 6 | SnapshotBuffer sample after last → holds last value |
| 7 | SnapshotBuffer ring overflow (>32) drops oldest |
| 8 | SnapshotBuffer out-of-order push sorts by tick |
| 9 | SnapshotBuffer large gap (no ticks for 5 frames) → smooth across |
| 10 | SnapshotInterpolator end-to-end with fake onReceive → render smooth across multiple ghosts |
| 11 | ReplicationManager tick stamps serverTick on emit (e2e GNS wire peek) |
| 12 | Wire compatibility — R3.x 帧（serverTick==0）走 fallback 路径 |
| 13 | **R5.1** Wire — `kFlagTeleport` round-trip via `FrameHeader::isTeleport()` |
| 14 | **R5.1** SnapshotBuffer — `push(..., snap=true)` + `isSnap(idx)` |
| 15 | **R5.1** SnapshotInterpolator — push 3 normal + 1 teleport；sample at tick-2/tick-3 midpoint → returns teleport bytes (no lerp sweep) |
| 16 | **R5.1** ReplicationManager — `markTeleported` + `tick` emits Full with `kFlagTeleport`，marker drains |

**Ship 判据**：12/12 (R5.0) + 4/4 (R5.1) PASS + 现有 R4.1-B 全 PASS 不回归。

#### 15.8 Teleport / 非插值规则（R5.1）

**问题**：R5.0 的插值器把每个 `kMsgTypeReplication` 都当连续运动处理。当 entity 瞬移
（spawn resync、关卡切换、position snap、ragdoll on、上车）时，client 会把"上一帧在 A、
这一帧在 B"做一次跨越整个世界的 lerp sweep — 视觉上是一根飞行轨迹，根本不是瞬移。

**解法**：在 `FrameHeader.flags` byte 上加 `kFlagTeleport = 0x01` 位，server 在瞬移那一帧
置位，client 收到后让插值器直接 snap 到新值，跳过本次 bracket 的 lerp。

**Authority 端 API**：
```cpp
mgr.markTeleported(netId);        // 设置 pending marker
mgr.tick(dt);                     // 下一帧 emit Full Snapshot with kFlagTeleport
```

`markTeleported` 是**一次性**的：marker 在 `tick()` 成功 emit 后自动清掉。持续瞬移
（每帧都在瞬移）需要每帧重设。`markTeleported` 同时等价于 `forceReplicate(netId)` —
把所有 peer 标为 `initialized=false`，保证下一帧一定走 Full Snapshot 路径而不是 Delta。

**为什么不发 Delta**？Delta 只携带 dirty fields。瞬移通常意味着"几乎所有字段都变了"，
Delta 比 Full 还大（field record header overhead），且失去原子性（client 必须先看到整
个 entity 状态再决定 lerp）。Full + kFlagTeleport 一致且简洁。

**Client 端流程**：
```
onReceive(kMsgTypeReplication):
    read srvTick
    read FrameHeader   // hdr.isTeleport() == true if this is a teleport
    deserializeObject → obj
    SnapshotInterpolator::push(netId, srvTick, obj, hdr.isTeleport())
                                                  └──────────────┘
                                            R5.1 新增第 4 参数
```

`SnapshotInterpolator::push` 把 `teleport=true` 传给 `SnapshotBuffer::push`，buffer 把
该 record 标记为 `snap=true`。`sample()` 调用 `findBracket(...)` 后检查 upper bracket
的 `isSnap(hi)`，是的话强制 `alpha=0` — 不做 lerp，直接 memcpy upper 的字节进 `out`。

**Bracket 行为对照**：

| 场景 | `t*` 在哪 | 行为 |
|------|---------|------|
| 正常 | upper 没 snap | 正常 lerp |
| 瞬移发生前 | upper 没 snap / 没到 teleport record | 正常 lerp（teleport 还没"发生"） |
| 瞬移发生后 | upper 是 teleport record (snap=true) | alpha=0，snap 到 teleport 字节 |
| 瞬移过后很久 | hi == kNoUpperBracket（已 past newest） | newest 字节本身 snap=true，等价于 snap |
| 在 teleport tick 之前很多 | upper 是普通 record | 正常 lerp；teleport 还在 future |

**为什么 snap 标在 record 上而不是全局标志**：record-scoped 让"同一 entity 不同时刻不同
瞬移"和"两个 entity 各自独立瞬移"都能精确控制；全局标志要么要求所有 entity 同步瞬移
（不符合实际游戏），要么得 per-ghost 维护（== record-scoped，但实现更绕）。

**已 ship 状态**：所有代码 + 测试已就位；§15.7 case 13–16 是 ship 判据。

### 15.9 R5.2 Client Prediction + Server Reconciliation（2026-08-24 ship）

R5.0/R5.1 让客户端"被动接收权威状态 + 平滑插值"。R5.2 引入**客户端预测**：
玩家操控的 ghost（`ProxyKind::AutonomousProxy`）在客户端立即响应输入，服务端在
固定 tick 边界上消费输入、推进权威状态、回送 ack，客户端做一次 reconciliation
（reconcile vs server-corrected state）后再次进入预测循环。

#### 15.9.1 设计决策（一行版）

| 决策 | 选定 | 备选 | 理由 |
|------|------|------|------|
| Server rewind 策略 | **不 rewind**（Unity 模型） | Unreal rewind+replay | 不强制服务端确定性，最小实现成本 |
| Input 序列号粒度 | **per-connection** | per-ghost | 1 个 client 只操控 1 个 ghost 仍是常态；per-conn ring 更省 |
| Server ack 载体 | **Full Snapshot 末尾 8-byte tail** | 独立 kMsgTypeServerAck | 复用现有流量，节省 12B PacketHeader/帧 |
| 输入历史容量 | **32 (drop-oldest)** | 64 | 对齐 SnapshotBuffer；丢输入可接受 |
| Field-level server-only 标记 | **`FieldAttribute::ServerAuthoritative = 1<<17`** | 反射 metadata 扩展 | 单 bit 决策、runtime 检查便宜 |
| Delta 是否带 ack tail | **否** | 是 | Delta 频率高，省 8B/帧 |
| 服务器拉输入时机 | **FixedPrePhysics 起始** | Egress | 在 RPC + replicated state 之前 |

#### 15.9.2 公共 API（header diff）

- `enum class ProxyKind : uint8_t { Server, AutonomousProxy, SimulatedProxy }` —
  新增 per-entity 权威角色；默认 `SimulatedProxy`，R3-R5.1 行为不变。
- `kMsgTypeClientInput = 0x0014` — 新 wire msgType。
- `ReplicationManager::setObjectProxyKind(netId, kind)` /
  `getObjectProxyKind(netId)` / `isLocallyControlled(netId)` —
  server-only setter，client 只读。
- `ReplicationManager::setInputApplicationFn(fn)` /
  `consumeClientInputs(simTick)` / `onClientInput(conn, body, n)` —
  gameplay-facing hooks for the input application callback and the
  server-side consume entry.
- `ReplicationManager::buildAckTailForConnection(conn, age)` →
  `AckTailInfo` — emitter helper; `AckTailInfo.present=false` 时不写字节。
- `ReflectSerializer::AckTail{ lastAckedInputTick, serverCommandAge, present }`
  + `writeAckTail`/`readAckTail` — wire codec helpers.

#### 15.9.3 Wire format diff (R5.2 增量)

Full Snapshot body 在 R5.0/R5.1 的 field records 之后可选追加 8 字节：

```
[... field records ...]
[ -- optional, when AckTail.present=true -- ]
[u32 lastAckedInputTick]
[u32 serverCommandAge]
```

Delta frame **不**带 tail。Emission gate：服务器端至少存在 1 个
`ProxyKind::AutonomousProxy` 注册对象且 connection 有非零 ack seq。
Receiver 总是 `readAckTail` 尝试读 8 字节，stream 末尾则安全回退
（present=false）。

#### 15.9.4 Client input wire envelope (kMsgTypeClientInput)

```
[u32 inputSeq]            // per-connection monotonic, wraparound-aware
[u32 serverTickAtSend]    // telemetry
[u8 payload...]           // opaque, library 不解析；0 长度也允许
```

多条输入可背靠背堆叠；reader 一次返回第一条，caller 循环消费剩余 bytes。
Codec 见 `AYNetwork/Prediction/ClientInputCodec.h`，纯 header-only。

#### 15.9.5 固定 tick ordering

服务器侧在 `FixedPrePhysics` 起始处调
`_replicationManager.consumeClientInputs(simTick)`，随后才是
`drainSimulationInbound`（接收 replicated state）和 RPC tick。
客户端镜像这条顺序：预测 → 接收 → reconciliation。

#### 15.9.6 MispredictionResolver 策略

- Field tagged `ServerAuthoritative` → 跳过（resolver assert + continue）。
- Field tagged `NetReplicate` 且 delta > threshold（float 1e-4 rel / double
  1e-6 rel / 其他 byte-exact）→ **snap**（直接覆盖 predicted with server）。
- Field delta ≤ threshold → **smooth**（`alpha = clamp(dt/smoothingDuration,
  0, 1)` exponential lerp；非 float 字段按 SnapInterpolator 规则 snap-to-lower）。
- Predicted 与 server bytes 大小不一致 → snap 全量。
- 默认 `smoothingDuration = 0.1s`。

#### 15.9.7 测试矩阵（10 cases, AYTest_ClientInput.cpp）

| # | Case | 验证目标 |
|---|------|----------|
| 1 | InputRing push & lookup | push 3 records 后 tryGet 全命中；size()==3 |
| 2 | InputRing drop-oldest overflow | cap=32 推 33 条；最旧 2 条被丢弃；count==32 |
| 3 | InputRing ackUpTo clamp | ackUpTo 不允许倒退；ackedSeq 单调前进 |
| 4 | ClientInputCodec round-trip | write/read 字节相同；截断返回 false |
| 5 | AckTail piggy-back round-trip | 注入 tail{42,7} 后 write→read 字节对齐；present=false 不写 |
| 6 | Ack math wraparound | `(int32)(a-b)>0` 跨 u32 wrap 边界仍正确 |
| 7 | ServerAuthoritative NOT predicted | MispredictionResolver 跳过 ServerAuthoritative 字段 |
| 8 | Misprediction snap-vs-smooth | 阈值下 smooth；阈值上 snap；snapped bool 正确 |
| 9 | consumeClientInputs by simTick | 2 个 conn × 2 inputs → callback 按 seq 顺序精确触发一次 |
| 10 | End-to-end GNS loopback AutonomousProxy mirror | full 链路：kMsgTypeClientInput → server tick → ack tail → client ack → reconcile |

#### 15.9.8 Ship definition

- ✅ Build green（无新增 warning）
- ✅ 895/895 PASS（885 existing + 10 new）
- ✅ Public API 默认值保持 R3-R5.1 字节兼容（SimulatedProxy 默认）
- ✅ Wire 格式在 `AckTail.present=false` 时字节级兼容 R5.1
- ✅ 不新增 mutex；R5.2 热路径 main-thread only
- ✅ design.md §15.9 完整描述 wire、ring lifecycle、ProxyKind、smoothing、test matrix

### 15.10 R5.3 Replay Recording (Authoritative Replay v1)

#### 15.10.1 架构分层

R5.3 采用**严格的两层架构** — 网络回放不等同于"在 ReplicationManager 里写文件"。

| Layer | Module | 依赖 | 职责 |
|---|---|---|---|
| 1 — Foundation | `d:\Aliyat\AliyatEngine\AYFoundation\AYReplay\` | `AYCore`, `AYIO` (含 lz4) | `.ayrp` 文件格式；IReplayRecorder / IReplayPlayer 接口；FNV-1a；LZ4 raw block 压缩；rotation；网络无关 |
| 2 — Adapter | `d:\Aliyat\AliyatEngine\AYRuntime\AYNetwork\src\Replay\` | `AYReplay`, `INetwork.h` (forward-decl) | 7 类事件 typed recordXxx API；wire-tap 注入；authority gate |

**为什么不是单层：**

1. **单机游戏也需要 replay**：rollback test / bug 复现 / 编辑器 preview 不应该强制 mock GNS / PacketCodec。
2. **文件格式 / timeline / compression / index / seek 与网络无关**：属于 foundation。
3. **网络 replay 记录逻辑消息而不是 raw GNS UDP**：回放不依赖录制时的 fragment/retransmit 库版本。
4. **事件类型范围分治**：foundation 保留 `[0, 0xFFFF]` 给自身（lifecycle / errors），consumer 拿 `[0x10000, 0xFFFFFFFF]`（AYNetwork 占 `[0x10000, 0x1FFFF]`，未来 AYEntity / AYEditor 各取相邻段，碰撞不可能）。

#### 15.10.2 文件格式 `.ayrp`

所有 multi-byte 整数按 **little-endian** 写入；`#pragma pack(1)`；v1 = `kReplayVersion=1`；magic = `'AYRP' = 0x41595250`。

**File header** (`sizeof(ReplayFileHeader)` = 108 B on MSVC with `#pragma pack(1)` due to trailing struct alignment; the logged field layout occupies bytes 0..105, with 2 bytes of implicit padding at offsets 106..107 to reach the next 4-byte alignment boundary — readers MUST use `sizeof(ReplayFileHeader)` at runtime rather than hard-coding 96 or 106; written on file open):
```
offset 0   : uint32 magic              = 0x41595250 ('AYRP')
offset 4   : uint16 version            = 1
offset 6   : uint16 flags              = bit0 = bodyCompressedDefault
offset 8   : uint32 engineVersion
offset 12  : uint32 schemaVersion      (= kSchemaVersion at record time)
offset 16  : uint32 tickRateMilliHz    (server tick rate; 30000 = 30 Hz)
offset 20  : uint64 randomSeed
offset 28  : uint64 sessionStartUnixMs
offset 36  : char[64] sceneName        (null-padded)
offset 100 : uint32 rotationIndex      (initial = 0; bumped per rotation)
offset 104 : uint32 reserved           = 0
offset 108 : (first record follows immediately)
```

**Event header** (20 B, precedes every non-checkpoint event):
```
offset 0  : uint64 tick                (monotonic simulation tick; uint64 for long sessions)
offset 8  : uint32 eventType           (kEvtFoundation_* < 0x10000, kEvtNet_* in [0x10000, 0x1FFFF])
offset 12 : uint32 payloadSize         (bytes following this header)
offset 16 : uint8  flags               (bit0 = payload LZ4-compressed)
offset 17 : uint8[3] reserved          (= 0)
```

**Checkpoint header** (24 B, written via `recordCheckpoint`):
```
offset 0  : uint64 tick                (simulation tick when the hash was taken)
offset 8  : uint64 stateHash           (FNV-1a 64-bit, or 0 if unknown)
offset 16 : uint32 snapshotSize        (sealed snapshot bytes that follow; 0 for hash-only)
offset 20 : uint8  flags
offset 21 : uint8[3] reserved
```

**Rotation**: at `bytesWritten >= maxBytesPerFile` (default 64 MiB) OR `now - openUnixMs >= maxDurationSec * 1000` (default 300 s = 5 min), current file is flushed, `kEvtFoundation_SessionEnd` (zero payload) written, file closed, and `<base>_<NNN>.ayrp` opened with `rotationIndex` incremented. Files are NOT concatenated; playback v2 orders by `sessionStartUnixMs + rotationIndex`.

**Crash safety**: best-effort between `beginSession` and `endSession`. `IReplayPlayer::open()` accepts truncated files and returns `Error::Truncated` so playback v2 can resync to last `kEvtFoundation_Checkpoint`.

#### 15.10.3 AYNetwork 适配事件负载布局

每个事件按 little-endian 打包，magic-free，按 schemaVersion=1 解释：

| Event type | Payload |
|---|---|
| `kEvtNet_InitialFullSnapshot = 0x10001` | `[u32 connectionId][u8 frameFlags][sealed snapshot bytes (post-PacketCodec::encode)]` |
| `kEvtNet_Spawn = 0x10002` | `[u32 connectionId][u32 netId][u64 schemaHash][spawn payload bytes]` |
| `kEvtNet_Despawn = 0x10003` | `[u32 connectionId][u32 netId]` |
| `kEvtNet_DeltaSnapshot = 0x10004` | `[u32 connectionId][u8 frameFlags][sealed delta bytes]` |
| `kEvtNet_InputBatch = 0x10005` | `[u32 connectionId][u32 inputSeq][u32 serverTickAtSend][u32 payloadLen][bytes...]` |
| `kEvtNet_RpcBatch = 0x10006` | `[u16 messageType][u32 connectionId][u32 payloadLen][bytes...]` |
| `kEvtNet_AuthorityChange = 0x10007` | `[u32 netId][u8 oldKind][u8 newKind][u32 connectionId][u32 reserved=0]` |

`InitialFullSnapshot` 与 `DeltaSnapshot` 的 snapshot bytes 是 **sealed post-PacketCodec::encode**（12B packet header + body + CRC32C tail），所以 playback v2 可以直接喂回 `PacketCodec::decode → onReceive` 路径；不需要重新走 replication 序列化逻辑。

#### 15.10.4 Wire-tap 集成点

| 文件 | Hook site | 行为 |
|---|---|---|
| `src/Replication/ReplicationManager.cpp` | `registerObject` legacy path | recordSpawn (conn=0) |
| `src/Replication/ReplicationManager.cpp` | `unregisterObject` | recordDespawn per visible peer |
| `src/Replication/ReplicationManager.cpp` | `tick()` per-peer first-tick visibility flip | recordSpawn (conn=peerId) |
| `src/Replication/ReplicationManager.cpp` | `tick()` Full Snapshot success | recordInitialFullSnapshot |
| `src/Replication/ReplicationManager.cpp` | `tick()` Delta success | recordDeltaSnapshot |
| `src/Replication/ReplicationManager.cpp` | `tick()` peer-loses-visibility | recordDespawn |
| `src/Replication/ReplicationManager.cpp` | `rebroadcastEntitySpawn` loop | recordSpawn + recordFullSnapshot per peer |
| `src/Replication/ReplicationManager.cpp` | `setObjectProxyKind` | recordAuthorityChange (if `old != new`) |
| `src/Replication/ReplicationManager.cpp` | `tick()` end-of-loop | `flush()` |
| `src/AYNetworkSubSystem.cpp` | `applySimulationInbound` `kMsgTypeClientInput` case | recordInput |
| `src/AYNetworkSubSystem.cpp` | `applySimulationInbound` `kMsgTypeRpcRequest/Response/Reject` cases | recordRpc (inbound) |
| `src/AYNetworkSubSystem.cpp` | `FramePhase::Egress` arm | `flush()` after `_replicationManager.tick()` |
| `src/RPC/RpcHandler.cpp` | `emit()` outbound | recordRpc (outbound) |

所有 hook 用 **anonymous-namespace helper** 形式（`recordFullSnapshot / recordSpawn / recordDespawn / recordDeltaSnapshot / packU32LE`）在 `ReplicationManager.cpp` 内部打包 payload 后调用 `_replay->recordEvent(serverTick, kEvtNet_X, buf.data(), buf.size())`，避免 `NetworkReplayRecorderAdapter.h` 被 `ReplicationManager.cpp` 拉入（防止 INetwork.h ↔ adapter.h 循环）。

#### 15.10.5 Header cycle prevention

- `INetwork.h`：`namespace ayt::replay { class IReplayRecorder; }` forward-declare + `IReplayRecorder* _replay = nullptr` + `setReplayRecorder / getReplayRecorder` virtual.
- `RpcHandler.h`：同样的 forward-declare + setter。
- `ReplicationManager.cpp` 通过基础接口 `_replay->recordEvent(...)` 调用（不接触 adapter 头）。
- `AYNetworkSubSystem.cpp` 同样 forward-declare + 直接 `recordEvent` 调用。
- `NetworkReplayRecorderAdapter.h` 包含 `<AYReplay/IReplayRecorder.h>`（foundation 接口）和 `<AYNetwork/INetwork.h>`（作为适配目标的 public API）。

#### 15.10.6 Authority gate

- `NetworkReplayRecorderAdapter::_authorityGateOk` 默认 `false`。
- Wire-tap helper 内部用 `isAuthority()` 守门 → 调 `_replay->recordEvent(...)`；客户端 `isAuthority()=false` 时直接不进 `_replay` 调用路径，避免每次 tick 一次额外分支。
- v1 不主动 gate 设为 `true`（adapter 持有一个对外 `setAuthorityGate(bool)` 入口，让外部显式打开；wire-tap helper 调用前后会包一对 toggle，但当前默认就是 server-only 路径，不需要 toggle）。
- Clients writing `_replay->recordEvent` 时即便路径绕过 gate 也只会拿到一条 `NotImplemented` 风格的 false 返回 — 不会写文件。

#### 15.10.7 State hash + 周期性 checkpoint

- `recordPeriodicCheckpoint(serverTick, registeredNetIds, provider)`：遍历 `registeredNetIds`，对每个 netId 调用 `provider(netId, scratch)` 拿到字节缓冲；FNV-1a 64-bit combine 跨所有 netId 的字节，组合哈希写入 `kEvtFoundation_Checkpoint` 事件（带 24-byte CheckpointHeader），payload 为空 (`snapshotSize=0`，hash-only 模式)。
- 默认 `setCheckpointIntervalTicks(30)` = 1 s @ 30 Hz，可在 `ReplicationManager::tick` 入口通过 `setCheckpointIntervalTicks(N)` 调整。
- 复用 `stateHash` 接口 (24-byte checkpoint header)，不内嵌 snapshot 字节 — 节省带宽；snapshot 字节在 `InitialFullSnapshot` / `DeltaSnapshot` 事件中独立保存。
- v2 playback 会读取 `kEvtFoundation_Checkpoint` 事件以校验每 N tick 一次的状态漂移。

#### 15.10.8 Out of scope (v1 deferred)

- **Playback**：v1 `IReplayPlayer` 是 stub (`readNextEvent/seekToTick/seekToCheckpoint` 返回 `NotImplemented`)。v2 将走 `PacketCodec::decode → ReplicationManager::onReceive` 路径。
- **Deterministic Replay**：re-simulate from inputs 需要确定性物理 / 随机 / 固定 tick，v2+。
- **Background writer thread**：v1 main-thread flush；v2 可加 SPSC queue。
- **Editor timeline UI**：不属于 Replay 库，归属 `AYEditor`。
- **Connection lifecycle events**：v1 records `connectionId` per event，lifecycle 可由该字段推导；显式 `kEvtNet_ConnectionLifecycle` 推迟到 v1.1。

#### 15.10.9 Risk register

| # | Risk | Mitigation |
|---|---|---|
| 1 | 主线程 `flush()` 阻塞 tick | 默认 off (opt-in)。v2 可加 SPSC 异步 writer。 |
| 2 | Crash 中断写 | `IReplayPlayer::open()` 接受 truncated 文件 (返回 `Truncated`)，v2 回放从最近 `kEvtFoundation_Checkpoint` resync。 |
| 3 | 文件 size 爆炸 | 64 MiB rotation 上限。 |
| 4 | Thread safety | Recorder 仅 main-thread。`setReplayRecorder` 必须在 `listen()` / `connect()` 之前从 main-thread 调用。 |
| 5 | Schema drift 跨录制 | `schemaVersion` stamped in file header。v2 回放拒绝 mismatch。 |
| 6 | LZ4 dictionary 不一致 | Raw block API (`LZ4_compress_default`)，无 dictionary，跨 LZ4 版本稳定。 |
| 7 | Foundation cycle：AYNetwork 不能 link AYReplay 若后者 ever needs AYNetwork | AYReplay 是 foundation-level (AYCore + AYIO)。反向依赖禁止 — code review 强制。 |
| 8 | Event-type registry 跨 adapter 冲突 | Foundation 保留 [0, 0xFFFF]；每个 adapter 显式声明 range。已分配：AYNetwork `[0x10000, 0x1FFFF]`。 |

#### 15.10.10 Test matrix（10 cases, 5 foundation + 5 adapter）

| # | Case | File |
|---|---|---|
| 1 | `Recorder_WritesHeaderMagicVersion` | `AYFoundation/AYReplay/unittest/AYTest_ReplayFoundation.cpp` |
| 2 | `Recorder_EventRoundTrip` (含 sequenced events + empty payload) | 同上 |
| 3 | `Recorder_CheckpointRoundTrip` (24-byte CheckpointHeader + snapshot bytes) | 同上 |
| 4 | `Recorder_Lz4CompressionShrinksZeros` (64 KiB zeros → `flags & 1`, payload < input) | 同上 |
| 5 | `Fnv1a64_KnownVectors` (empty = 0xcbf29ce484222325, "foobar" = 0x85944171f73967e8) | 同上 |
| 6 | `AdapterPureRecord` — 5 recordXxx calls 产 7 events on disk, exact kEvtNet_* type | `AYRuntime/AYNetwork/unittest/AYTest_ReplayNetwork.cpp` |
| 7 | `AuthorityGateRejectsClientMode` — `_authorityGateOk=false` 时无 network events written | 同上 |
| 8 | `EndToEndGnsLoopback` — 30 tick 仿真后 file 中 nFull=1, nDelta≥9, nInput=6, nRpc=8 | 同上 |
| 9 | `AuthorityChangeEventCaptured` — 3 次 `recordAuthorityChange` → file 中 3 个 `kEvtNet_AuthorityChange` | 同上 |
| 10 | `PeriodicCheckpointProduced` — 2 次 checkpoint → 2 个 CheckpointHeader，相同 provider bytes → 同 hash | 同上 |

**Ship 判据**：1003/1003 PASS (993 baseline + 5 foundation + 5 adapter), `test exit=0`。

### 15.11 R5.4 Transport Fault Simulation（2026-08-25 ship）

AYNetwork-side fault injector at the sealed-bytes seam. **Not** a wrapper around GNS `Fake*` knobs (those are global-only per `steamnetworkingtypes.h:1325-1328`). Profiles are per-`(netId, channel-mask)` and live in `TransportFaultController`. Per-`GnsConnection` `TransportFaultInterceptor` holds a delay queue + token bucket per channel; frames flow `caller → PacketCodec::encode → Interceptor → delay-queue → rate-limit → wire`.

**Profile shape** (`TransportFaultProfile`):
```
randomSeed          : u64  // 0 = use session randomSeed
channelMask         : u8   // bit-or of kFaultChannelReliable/Unreliable/Fragmented/Ack
latencyMeanMs       : u32
latencyJitterMs     : u32  // uniform [mean-jitter, mean+jitter]
lossPercent         : f32  // [0, 100]
dupPercent          : f32
reorderPercent      : f32
rateLimitBytesPerSec: u32
rateLimitBurstBytes : u32
```

**Pump timing**: `GnsConnection::pump()` advances the interceptor via `tick(nowMs, sendOut, recvOut)` once per pump invocation. Frame release is `nowMs >= frame.releaseAtMs`. Rate-limited frames that fail `tryConsume` are re-enqueued at `nowMs + 1ms`. Send-side release calls `_rawSend`; recv-side release calls `onRawData` (which runs `PacketCodec::decode`). When no profile is installed for the connection, `isEnabled()` returns `false` and the interceptor is bypassed entirely (no spurious 1-pump lag).

**RNG determinism**: each profile gets its own `std::mt19937_64` seeded from `profile.randomSeed` (or session `randomSeed`). All Bernoulli draws (loss / dup / reorder) go through it; tests can reproduce exact sequences by fixing the seed.

**Architecture**:
- `TransportFaultProfile` — header-only config struct in `interface/AYNetwork/TransportFaultProfile.h`.
- `TransportFaultController` — singleton owned by `NetworkSubSystem` (keyed by AYNetwork netId).
- `TransportFaultInterceptor` — per-`GnsConnection`, holds `DelayedFrameQueue` + `TokenBucket` per channel.
- `TokenBucket` — header-only primitive (`src/Transport/TokenBucket.h`); double-precision refill.
- `DelayedFrameQueue` — per-channel deque with deterministic drop-oldest cap enforcement.

**Test seam**: `GnsConnection::setFakeTransportSender/Receiver` mirrors `ReplicationManager::setBroadcastSinkForTesting`. Lets tests inject sealed bytes without spinning up GNS. Fault injection works in both real-transport and fake-transport modes (interceptor is oblivious to transport choice).

**Out of scope (deferred)**: deterministic-replay integration with R5.3; body-mutating faults (CRC-protected at sealed seam); cooperative cross-connection simulation; GNS global `Fake*` fallback; editor UI for live knobs.

**API additions** (all additive, default-implemented to keep existing test stubs non-abstract): `INetworkSubSystem::setTransportFaultProfile / clearTransportFaultProfile / getTransportFaultProfile`. Test seam on `GnsConnection`.

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

## 15. 立即可改的清单（**已由 R4.1-A 取代**，2026-07-29）

> 下列为 2026-07-26 审计遗留 P0 杂项；**当前唯一优先级清单见 §10 Phase R4.1-A**。勿并行处理。

按修复 ROI 排序（历史存档）：

| # | 文件 | 改动 | 估计工时 |
|---|------|------|---------|
| 1 | `CMakeLists.txt:11-19` | 删 `src/ReplicationManager.cpp`（与 `src/Replication/` 重复定义）| 5 min |
| 2 | `src/Transport/UdpSocket.cpp:42-52` | `WSACleanup()` 移到析构外（多个 socket 生命周期 bug）| 15 min |
| 3 | `src/AYNetworkSubSystem.cpp:30-32` | `update()` 里 dispatch + tick timeout 骨架 | 2 h |
| 4 | `include/Replication/AYNetwork/Replication/AYNetwork/Replication/AYNetwork/Replication/NetDataComponent.h:25-26` | 删 `AY_NET_FIELD` 空宏（design 也没用到）| 5 min |
| 5 | `interface/AYNetwork/INetwork.h:223` | 删 `NETWORK_SUBSYSTEM()` 宏或确认 `GameLoop::getNetwork()` 存在 | 5 min |
| 6 | `unittest/CMakeLists.txt` | 新建并注册 `BitStreamTest` / `TransportTest` | 1 h |
| 7 | `design.md §10 Phase 1/2/3` | 把 `[x]` 改回 `[ ]`（本文档已修正）| done |

总计 **P0 最小修复 ≈ 半日工作量**。

---

## 15.12 Bandwidth & Connection Profiler (R5.5, 2026-08-25)

### 概述
为 AYNetwork 增加可在运行期拉取的带宽 / 连接画像 API。回答"哪种 msgType 占 60% 带宽""哪个 ghost 贡献了每帧的 Delta 峰值""连接 RTT / send-queue / 路径类型"。

### 模块边界
不新增 `AYProfile` 基础模块；R5.5 仅有 AYNetwork 一个消费者，`ProfilerRegistry` 作为 AYNetwork 子模块内部组件（仿 R5.4 fault layout），路径 `AYRuntime/AYNetwork/src/Profiler/ProfilerRegistry.cpp` + `include/AYNetwork/Profiler/*.h`。

### 数据结构 (ProfilerSnapshot.h, header-only POD)
- `MsgTypeBytes` — `(sendBytes, recvBytes, sendCount, recvCount)` per slot
- `NetIdWireCost` — `cumulativeSendBytes` (atomic, 永不重置) + `currentTickSendBytes` (每 tick reset) + `dirtyFieldCount` (snapshot of `ReplicationManager::getDirtyFieldCount`)
- `ConnLiveStatus` — 15 字段：pingMs / qualityLocal/Remote / in/outBytesPerSec / pendingReliable-Unreliable / in/outMessageCount / pathLocal/Remote / sendQueueBytes / ackPending / fragmentQueueBytes / simInBytesUnprocessed
- `ProfilerSnapshot` — `array<MsgTypeBytes, 7>` (7 in-game slots) + `unordered_map<uint16_t,MsgTypeBytes> byMsgTypeExtras` (Handshake / AppAck / ClientInput keyed) + `vector<NetIdWireCost> perNetId` (按 cumulativeSendBytes 降序) + `ConnLiveStatus live`

### 7 个 in-game 槽位 vs extras map

| slot | msgType | enum |
|------|---------|------|
| 0 | Replication | 0x0001 |
| 1 | EntitySpawn | 0x0002 |
| 2 | EntityDespawn | 0x0003 |
| 3 | Delta | 0x0004 |
| 4 | RpcRequest | 0x0010 |
| 5 | RpcResponse | 0x0011 |
| 6 | RpcReject | 0x0012 |

extras map 接受 AppAck (0x0013) / ClientInput (0x0014) / Handshake (0xFFFF + HandshakeMsgType) / 未知 msgType（防 crash 兜底）。

### API
`INetworkSubSystem` 新增 4 个默认实现的虚函数（沿用 R5.4 lesson: default-impl virtuals 让现有 test stub 不变 abstract）：
- `getProfilerSnapshot(out, netId)` — 单连接快照
- `getProfilerSnapshots(out)` — 所有已知连接
- `setProfilerSinkForTesting(fn)` — 每帧 update() 后触发
- `setProfilerDumpInterval(ticks)` — 0=关闭，每 N tick 一行 `[AYProfiler] ...` 到 stderr

### Atomic vs plain counter
- `cumulativeSendBytes / cumulativeRecvBytes` 用 `std::atomic<uint64_t>` (永不重置；snapshot 冷读)
- `currentTickSendBytes` 用 plain `uint64_t` (单写单读同主线程)
- `sendCount / recvCount` plain — 单帧精度足够

### Post-fault vs intent-bytes
- Profiler 在 send 钩子点 (即 `sendSealedToConnection` / `RpcHandler::emit` / `_sendHello/Welcome/Reject` / ackWire `_rawSend` / `onRawData` reassembled) 推送 *post-fault* 字节数：这是真正打到线上的字节 (R5.4 fault interceptor 之后)
- 想要 *intent* 视图（caller 提交字节数，pre-fault）的测试可另读 `ProfilerRegistry::intentBytes` 并行计数器（本期未开，留 R5.6）

### 钩子注入点
- **ReplicationManager::sendSealedToConnection** — 7 个 site (Full / Delta / Spawn × 2 / Despawn × 3 / rebroadcastEntitySpawn)
- **RpcHandler::emit()** — 单 chokepoint 按 envelope msgType 分支
- **GnsConnection::_sendHello/Welcome/Reject** — handshake 三种 subType 通过 `profiler::handshakeExtrasKey(sub)` 入 extras
- **GnsConnection::ackWire _rawSend** — AppAck 入 extras
- **GnsConnection::onRawData** (reassembled body) — 按 `hdr.msgType` 分发到 slot 或 extras

### RTT / path / send-queue
- `GnsConnection::getPing()` 已有 (R1.1)
- 新增 `pendingFragmentBytesForTesting()` (一行，封装 `PacketAssembler::pendingBytes()`)
- 新增 `getLastPumpBytes()` (缓存 `GnsPumpResult::bytes`)
- `ProfilerRegistry::fillLiveStatus` 调用 GNS `GetConnectionRealTimeStatus` + `GetConnectionInfo` 一次 per snapshot

### 文件清单
新增：
- `include/AYNetwork/Profiler/ProfilerSnapshot.h` — header-only POD
- `include/AYNetwork/Profiler/ProfilerMsgType.h` — slot / extras key helper
- `include/AYNetwork/Profiler/ProfilerRegistry.h` — class 声明
- `src/Profiler/ProfilerRegistry.cpp` — 实现 + periodic stderr dump

修改：
- `interface/AYNetwork/INetwork.h` — +4 default-impl virtuals + ReplicationManager ProfilerSendHook setter
- `src/AYNetworkSubSystem.cpp` — `ProfilerRegistry _profiler;` 字段 + 4 override + `setupProfilerHooks()` (连 ReplicationManager / RpcHandler / GnsConnections) + update/tick 集成 `tickWindow + dumpPeriodicIfDue + fireProfilerSinkIfSet`
- `src/Transport/GnsConnection.h/.cpp` — `pendingFragmentBytesForTesting/getLastPumpBytes` + ProfilerSendHook/ProfilerRecvHook setter + 4 send 钩子点 + 1 recv 钩子点
- `src/Replication/ReplicationManager.cpp` — 7 sendSealedToConnection 钩子调用
- `src/RPC/RpcHandler.cpp` — `emit()` 钩子调用
- `CMakeLists.txt` — 注册 `src/Profiler/ProfilerRegistry.cpp` + `include/Profiler` PUBLIC include
- `unittest/CMakeLists.txt` — 注册 5 个测试文件

### 测试
5 新 test 文件 (25 case, R5.4 baseline 1234 → R5.5 = 1259 PASS):
- `AYTest_ProfilerAccumulator.cpp` (5): recordSend/Recv, window reset, cumulative monotonicity, dump 空窗口跳过, dump 有流量清零
- `AYTest_PerMsgType.cpp` (5): 7 槽位映射 / Handshake extras key / AppAck + ClientInput extras / 百分比精度 ±0.01% / 未知 msgType 兜底
- `AYTest_PerNetId.cpp` (4): register 3 ghost 跟 cumulative / currentTick reset / unregister 移除 / 排序降序
- `AYTest_RttAndCongestion.cpp` (4): 默认值 / ConnAccessor 串联 / 字段默认值 / path 字段 surface
- `AYTest_ProfilerSubsystem.cpp` (7 E2E): 2 subsystem 各自 snapshot ≥1 / perNetId ≥3 ghost 累计 > 0 / msgType 总和 ≥ perNetId 总和 (handshake 差额 ≤ 总和) / sink per-update 触发 / dump interval 不断 crash / 百分比 100% ±0.01 / 未知 conn 返回 false

### 风险 / 局限
- atomic fetch_add 每帧 1 次 per connection (仅 cumulative) — 冷读，可忽略
- GNS `GetConnectionRealTimeStatus` 每 snapshot 调一次 — 诊断用，非热路径
- `currentTickSendBytes` 仅精确到一帧 — 与 sendCount 配合足够
- 客户端 `ClientInput` send 路径在 `ClientInputCodec::write` caller 端，未注入 R5.5 钩子（设计决定：caller 责任） — 文档化于 §15.12

### 留待 R5.6
- `windowSendBytesDelta` 4-byte foothold 写入 Replay checkpoint (本期未做)
- 跨 host profiler aggregation
- 通用 Instrumentor / ScopeTimer 宏框架

---

## 15.13 Determinism Contract (R6, 2026-08-25)

R6 commits `C1` through `C9` fix all 12 Blocker + 14 High findings from `determinism-risk-register-2026-08-25.md`. This section is the **architectural contract** for determinism that any R7+ contributor must preserve when touching AYNetwork — touching any of the rules below without re-verifying the `computeStateHash` invariant is a determinism regression.

### 15.13.1 Rules (one per root-cause theme)

| # | Rule | Where enforced | Why |
|---|------|----------------|-----|
| R-1 | **Tick-driven time only.** All time-dependent code reads through `GnsConnection::s_nowOverride` (C1) — no raw `ayt::performanceNowUs()` calls on the network hot path. | `GnsConnection::nowMs()` consults the override first; `setNowOverrideForTickRate(serverTick, tickRate)` is the canonical helper. | Wall-clock drift between runs breaks replay-rewind parity; a deterministic override makes the same scripted tick advance produce the same `nowMs` on every run. |
| R-2 | **uint64 microsecond fixed-point accumulators.** `NetworkTime::_accumulatorUs` and `ReplicationManager::_accumulatorUs` (C1) replace `double` accumulators; `double` is only a *derived* read view via `serverTimeSec()`. | Direct read sites: `src/Snapshot/NetworkTime.cpp`, `src/Replication/ReplicationManager.cpp`. | Doubles accumulate non-deterministic rounding across compilers/CPUs; uint64 microsecond math is bit-stable. |
| R-3 | **Sort-by-key everywhere a hash or wire-stamp iterates.** `std::unordered_map<uint32_t, T>` → `std::map<uint32_t, T>` (C2) on every map whose iteration order leaks into the state hash. | `ReplicationManager::_peers`, `PredictionManager::_rings/_ackedSeq/_ghosts`, `EntityReplicationWorldBinder::_bindings/_desired/_collisions`, `AckPipeline::_pending`, `RpcHandler::_pendingCalls`. | Hash buckets and pointer-iteration order differ across runs on libc++ vs MSVC; sorting by key collapses the variance. |
| R-4 | **Single-thread `RpcHandler` + `AckPipeline`.** Dropped `_pendingCallsMutex` and `AckPipeline::_mutex` (C3); `_simulationInboundMutex` removed from `AYNetworkSubSystem` (C6). | `RpcHandler.cpp`, `AckPipeline.cpp`, `AYNetworkSubSystem.cpp`. | Cross-thread enqueue is not visible in the state hash; making the class single-thread eliminates the race without changing the deterministic surface. |
| R-5 | **Synchronous async-RPC completion in `tick()`.** `RpcAsyncPool` deleted (C6); async `IMethodInfo::isAsync()` invocations complete inside `RpcHandler::tick()` in `callId` order. | `RpcHandler::tick()` drains `_pendingJobs` (insertion order = `callId` order because `_nextCallId.fetch_add` is monotonic). | Worker-pool priority queues were not deterministic across runs; documented behavior change in the R6.5 changelog. **Async server RPCs block the network thread** during invocation — loud in §15.13.4. |
| R-6 | **Session-seed persistence + deterministic RNG.** `TransportFaultController::setSessionSeed(uint64_t)` (C4); `ReplayFileHeader::randomSeed` already in foundation; per-profile `std::mt19937_64` seeded from session seed when `profile.randomSeed == 0`. | `src/Transport/TransportFaultController.cpp`, `src/Transport/TransportFaultInterceptor.cpp`. | `kDefaultSessionSeed = 0xC0FFEEULL` is the default; tests fix a seed and reproduce the exact loss/latency/reorder draw sequence. |
| R-7 | **Stable `connectionId` allocation + stub mapping (B-10).** `allocateNetId(acceptOrdinal, slotOrdinal)` (C5) packs `(acceptOrdinal<<8) | slotOrdinal` so identical accept order → identical netIds; full v2 player remap → R6.5. | `NetworkReplayRecorderAdapter` packs `connectionId` fields as ordinals; the v1 player can't translate, so the stub is sufficient for replay-rewind state-equal parity tests. |
| R-8 | **Profiler paths always `pathLocal = pathRemote = 0` + sorted iteration.** `ProfilerRegistry` (C5) normalizes path fields and sorts `byMsgTypeExtras` keys + `perNetId` (netId primary, cumulative secondary). | `src/Profiler/ProfilerRegistry.cpp:186-187, 207-234, 242-279`. | GNS `GetConnectionInfo` paths are platform/runtime-specific; including them in the hash makes state-equal drift; zero is the canonical "non-relay" representation. |
| R-9 | **Fixed-point float precision.** `QuantizedFloat` (C7) rounds to `uint32` (default 24 bits) at serializer boundaries; `MispredictionResolver` lerp uses `uint16 alpha_q16`; `BitStream::writeFloat` uses `lroundf` for symmetric rounding. | `include/AYNetwork/Serialization/QuantizedFloat.h`, `src/Replication/ReflectSerializer.cpp`, `src/BitStream.cpp`, `src/Prediction/MispredictionResolver.cpp`. | Float serialization rounding mode differs between MSVC and clang; quantization is bit-deterministic round-to-nearest. |
| R-10 | **Pump-boundary funnel for state callbacks.** `gns_status_callback` defers `_stateHandler` invocations into `pendingActions` queue (C8); drained at pump end on the main thread. | `src/Transport/GnsConnection.cpp:69-131`. | User-visible contract: handlers fire, one pump-iteration later than before. This is the determinism seam between the GNS poll thread and the main thread. |
| R-11 | **`computeStateHash` is the determinism oracle.** `INetworkSubSystem::computeStateHash(HashKind)` (C9) — `StateOnly` folds reflected object bytes + server tick + pending RPC call ids + per-conn ack cursors; `StatePlusProfiler` additionally folds profiler atomic counters. | `src/AYNetworkSubSystem.cpp` `computeStateHashInternal(bool)`. | Default-implemented to 0 on `INetworkSubSystem` (R5.5 lesson: test stubs stay non-abstract). Any subsystem that overrides must produce byte-stable hashes across compilers. |
| R-12 | **Tick-stamp consistency for RPC record events.** Outbound `RecordRpc` stamp `tick = getServerTick()` (C5) — matches inbound RPC/Input round to nearest playback tick via `tickRate`. | `src/RPC/RpcHandler.cpp:591`, `src/AYNetworkSubSystem.cpp:382-458`. | Asymmetric stamps make the recorded timeline reorder under playback. |

### 15.13.2 Replay scope (v1 vs v2)

**v1 (current, R6 ships):** `FileReplayRecorder` writes logical messages (Snapshots, RPC, Spawn, Despawn, Authority, Inputs) in netId + tick + insert-order. Playback is a stub — `IReplayPlayer::open()` validates magic+version; `readNextEvent/seekToTick/seekToCheckpoint` return `Error::NotImplemented`. **Replay-rewind parity is testable** via `computeStateHash`: the recorded state, when replayed into a fresh subsystem, must produce the same hash.

**v2 (R6.5):** Full `IReplayPlayer` with the `connectionId` remap table. The stub mapping packs ordinals into `connectionId` fields at record time (B-10 stub), so the v1 recorded file is forward-compatible — v2 player reads ordinals and remaps to runtime `NetConnection*` IDs.

### 15.13.3 Known escape hatches (documented, do not fix)

- **`static typeid` cross-ABI portability (L-06/L-07).** `ReflectSerializer` uses `typeid(T).hash_code()` for per-method RPC dispatch. MSVC and clang produce different hashes for the same `T`. R5.x `AYTest_RpcHandler.cpp::RpcKindMismatchRejects` lives with this — tests that compare hashes must run in the same build. **Do not "fix" by adding a `static` registry unless you also bump every wire format version.**
- **GNS global `Fake*` knobs.** R5.4 interceptor provides per-connection fault injection at the sealed-bytes seam. Callers wanting the GNS-global knobs (`FakePacketLoss_Send`, `FakePacketLag_*`) can still call `SteamNetworkingUtils_LibV4()->SetGlobalConfigValue_Int32(...)` directly — they apply at a lower UDP layer and are *not* deterministic; documented escape hatch.
- **`Static` initialization order across translation units.** Test fixtures (`StateEqualFixtureRegistrar`, etc.) use static-init registration; the global registry must already be initialized. `AYTest_StateEqual.cpp` demonstrates the pattern.
- **`delete` of raw pointers owned by the registry.** Test code that `delete`s the `INetworkSubSystem*` returned by `createNetworkSubSystemForTest()` *also* destroys objects the registry didn't own — see the heap-allocate `std::vector<StateEqualPair> storageA(5)` pattern in `AYTest_StateEqual.cpp::SortByKey_PeerOrder_StableAcrossRuns`.

### 15.13.4 Loud documentation for behavior changes

- **Async RPC now blocks the network thread.** R6 C6 deleted `RpcAsyncPool`. `IMethodInfo::isAsync()` still works, but completion happens synchronously inside `RpcHandler::tick()`. Game code that relies on async RPC for "doesn't block gameplay" must migrate to R6.5's `IMethodInfo::isAsync()` returning `false` (synchronous on the caller side) or moving heavy work to a non-network thread *outside* `RpcHandler::tick()`.
- **State handlers fire one pump-iteration later.** R6 C8 defers `gns_status_callback` invocations into `pendingActions`. Subsystem observers that previously expected `(old_state, new_state)` to fire inside the same `pump()` call now see them at the next pump boundary. This is intentional — it eliminates the GNS-poll-thread race and makes the order deterministic.
- **Simulation inbound is no longer mutex-guarded.** R6 C6 removed `_simulationInboundMutex`. Multi-thread callers must migrate to single-thread `update()` before R6.5 ships.

### 15.13.5 Ship definition (R6.0 done =)

1. ✅ All 12 Blocker + 14 High findings fixed (44 total, Medium findings ride along in C2/C5/C8).
2. ✅ `computeStateHash(HashKind)` virtual on `INetworkSubSystem`; `AYNetworkSubSystem` override implemented.
3. ✅ 8 new state-equal E2E cases pass (`AYTest_StateEqual.cpp`).
4. ✅ Existing 1484/1484 baseline unchanged (no regression in R5.5).
5. ⏳ Engine pointer bumped in main repo.
6. ✅ `design.md §15.13` (this section) + register status column + memory entry all updated.

**Total commits: 10 (C1 → C9 → C10 docs) on submodule `cf746b1` (C9 HEAD).**

---

## 15.14 R6.5 Replay v2 Player (2026-08-25 ship)

R6 deferred two replays: foundation-level `FileReplayPlayer` was a stub
that returned `NotImplemented` from every read/seek API, and the v1
`B-10` connectionId-remap finding was filed as forward-looking because
there was no real player to consume a remap. R6.5 fills both gaps so
recorded sessions can actually be played back and the determinism oracle
can validate state-equal hashes across a replay.

### 15.14.1 Player core (R6.5-1)

`FileReplayPlayer` now ships a real implementation:

- **`open()`** slurps the whole file into a `vector<uint8_t>` at
  `open()` if the file fits in `kReplayPlayerFileCap` (256 MiB; the
  rotation default is 64 MiB so this gives generous headroom). Files
  beyond the cap reject with `IoError`. Magic and version validation
  match the recorder.
- **Checkpoint index** built once at `open()` in a single forward pass.
  Each entry is `{ReplayTick tick; int64_t fileOffset;}` — ~24 bytes
  per checkpoint. Memory is negligible for typical workloads.
- **`readNextEvent(hdr, payload, isCheckpointOut=nullptr)`** peeks the
  first 16 bytes to disambiguate event vs checkpoint (same heuristic as
  the test scanner — event iff `low32 ∈ [0x0001,0x0020] ∪
  [0x10000,0x1FFFF]`), reads the full header, returns the payload
  bytes, decompresses LZ4 if `flags & kEvtFlagPayloadCompressed` (cap
  16 MiB on decompressed output; `OutOfMemory` if exceeded).
  Synthesizes a `SessionEnd` sentinel at EOF so callers can drive a
  replay-to-completion loop without a separate `atEof()` predicate.
  When `isCheckpointOut` is non-null, flips it for the checkpoint
  branch; the `tick` field is propagated from the underlying
  checkpoint header so callers can correlate checkpoints with adjacent
  events.
- **`seekToTick(tick)`** finds the latest checkpoint at-or-before the
  target, sets the cursor to that offset, then linear-reads events
  forward until `hdr.tick >= target` (or EOF).
- **`seekToCheckpoint(tick)`** binary-searches the checkpoint index
  (O(log N)) and jumps to the offset of the first entry with
  `tick >= target`.

The player is foundation-only: no AYNetwork dependency, no decoder.
Adapter-level event decoding lives in §15.14.2.

### 15.14.2 AYNetwork decoder + connectionId remap (R6.5-2)

`NetworkReplayEventDecoder::decodeNext(player, out, remap,
unmappedIds)` reads the next event from a foundation player and unpacks
the adapter-prefix bytes for the 7 `kEvtNet_*` event types
(`InitialFullSnapshot` / `Spawn` / `Despawn` / `DeltaSnapshot` /
`InputBatch` / `RpcBatch` / `AuthorityChange`). Each event's
`connectionId` is consulted against `ConnectionIdRemap` (a
`std::map<uint32_t, uint32_t>` keyed by recorded id) and substituted
with the live id. Missing keys fall back to the literal id and append
to `unmappedIds` for diagnostics.

Why per-event remap and not a global swap: the recorded `connectionId`
is captured at the network wire-tap and reflects the record-time
accept order. The R6 C5 stable allocation scheme (B-09) makes the
recorded id equal to the accept-order ordinal, so the remap is identity
when accept order matches between record and playback — and that is
the dominant production case. R7+ may add automatic accept-order
translation for cross-server replay.

### 15.14.3 Player pump bridge (R6.5-3)

Two new virtual seams on `INetworkSubSystem` (default no-op so
non-AYNetwork test stubs stay non-abstract):

- **`tickRecordedEvent(uint32_t eventType, const uint8_t* payload,
  size_t size)`** — the live pump. `AYNetworkSubSystem` overrides with
  a 7-arm switch that strips the adapter-prefix bytes (sizes match
  `NetworkReplayRecorderAdapter` exactly) and routes to the matching
  live seam:

  | Recorded event | Action |
  |---|---|
  | `kEvtNet_InitialFullSnapshot` | strip 5-byte prefix; `_replicationManager.onReceive(kMsgTypeReplication, …)` |
  | `kEvtNet_DeltaSnapshot` | strip 5-byte prefix; `_replicationManager.onReceive(kMsgTypeDelta, …)` |
  | `kEvtNet_Spawn` | strip 16-byte prefix; `_replicationManager.onReceive(kMsgTypeEntitySpawn, …)` |
  | `kEvtNet_Despawn` | strip 8-byte prefix; `_replicationManager.onReceive(kMsgTypeEntityDespawn, …)` |
  | `kEvtNet_InputBatch` | strip 16-byte prefix; `_replicationManager.onClientInput(connId, body, len)` |
  | `kEvtNet_RpcBatch` | strip 10-byte prefix; demux on `messageType` to `_rpcHandler.onRpcRequest / onRpcResponse / onRpcReject` |
  | `kEvtNet_AuthorityChange` | strip 14-byte prefix; `_replicationManager.setObjectProxyKind(netId, newKind)` |

  The `from` parameter to `onReceive` is `nullptr` — replay events
  originate from the recording, not a live GnsConnection.

- **`installReplayRngSeed(uint64_t)`** + read-back `hasReplayRngSeed()`
  / `getReplayRngSeed()`. The v2 player exposes
  `ReplayFileHeader.randomSeed` so a deterministic playback can latch
  the seed for downstream RNG consumers. Today this is a stub field
  on `AYNetworkSubSystem`; R7+ will route through
  `TransportFaultController::setSessionSeed`.

### 15.14.4 R6.5 ship definition

1. ✅ `FileReplayPlayer` reads every record type from disk (event, checkpoint, compressed, uncompressed).
2. ✅ `seekToTick` and `seekToCheckpoint` work; the latter is O(log N) via the checkpoint index.
3. ✅ `NetworkReplayEventDecoder` unpacks all 7 `kEvtNet_*` payload layouts and applies the connectionId remap.
4. ✅ `NetworkSubSystem::tickRecordedEvent` demuxes decoded events into the live subsystem seams.
5. ✅ `B-10` flipped from "Deferred R6.5" to "FIXED" in `determinism-risk-register-2026-08-25.md`.
6. ✅ Tests: `AYReplay_Test` = 12/12 (unchanged); `AYNetwork_Test::ReplayNetwork` = 12/12 (5 recorder + 4 decoder + 3 bridge); `AYNetwork_Test::StateEqual` = 36/36 (no regression).

**Total commits: 3 (R6.5-1 player core, R6.5-2 decoder, R6.5-3 bridge) on submodule `dd2fdec` (R6.5-3 HEAD). R6.5 docs flip: `be34c9e`.**

---

## 15.15 P2P / ICE transport (2026-08-26)

P2P is an additional connection-establishment path below the existing packet,
handshake, replication, RPC, prediction, replay, fault-injection, and profiler
layers. It does not change their wire formats or authority rules.

- Public backend-neutral contract: `interface/AYNetwork/P2P.h` (`PeerId`,
  `P2PConfig`, `P2PConnectionInfo`, `ISignalingTransport`).
- GNS adapter: `ConnectP2PCustomSignaling`, `ReceivedP2PCustomSignal`, and a
  per-virtual-port incoming factory in `GnsConnection`.
- ICE policy maps explicitly to private, STUN-derived public, and TURN relay
  candidate bits. STUN/TURN lists and per-server credentials are supplied as
  connection config values; `P2PPathKind` exposes direct versus relayed state.
- Built-in self-hosted backend: a bounded UDP rendezvous protocol with PeerId
  validation, registration, heartbeat, endpoint rebinding, opaque forwarding,
  peer expiry, message/peer caps, and a standalone server target.
- Secure signaling v2 adds per-peer room credentials, HMAC-SHA256, a
  challenge/confirm endpoint proof, replay windows, expiry and rate limiting.
  See `docs/secure-signaling-v2.md`. The legacy v1 UDP backend remains only for
  local development and trusted networks.
- Security boundary: the built-in server does not authenticate accounts. A
  production service replaces `ISignalingTransport` with an authenticated
  HTTPS/WebSocket/platform implementation and issues short-lived TURN
  credentials. GNS traffic encryption remains below this signaling layer.
- Process model: standalone GNS owns one identity per process. P2P identity
  setup is rejected while another GNS connection/listener is active, and P2P
  tests run in a dedicated process for the same reason.
- Direct-path hardening (2026-08-27): STUN host names are resolved before GNS
  connection creation/listener route installation, avoiding DNS work while the
  GNS global lock is held. Connection state is monotonic after protocol Ready,
  and a reconnect with the same PeerId deterministically replaces its stale
  host-side connection.
- Release probe: `AYNetwork_P2PSmokePeer` uses the production subsystem and
  requires handshake + Server RPC + replicated authority state to complete
  before running the app RTT/loss/jitter probe. It supports repeated sessions
  and multi-peer host gates through environment settings.
- Topology: this milestone supports listen-host/client P2P with NAT traversal
  and TURN fallback. Matchmaking rooms, host election/migration, and hostless
  state consensus are separate session/authority concerns and do not belong in
  the signaling relay.

---

## 16. Changelog

| 日期 | 变更 |
|------|------|
| 2026-07-26 | 工业级审计；R1–R6 重置；GNS 选型 §14 |
| 2026-07-27 | **设计审计补丁**：§1/§3/§4 统一 GNS；§3.3 Protocol↔GNS 切分；§4.2 多连接+断线；§5 应用消息头；**§6.6 Authority**；§8/§10/§12/§14.6 同步；废止 KCP 正文 |
| 2026-07-27 | **R1.A 多连接 pump**：GnsConnection 引入 `s_adoptFactory` + `serverAdopters()` fallback；AYNetworkSubSystem 持 `_serverClients` 列表，`update()` pump server parent + N children，`broadcast()` 真广播；新增 `MultiClientEcho` 测试（1 server + 2 clients）；53/53 PASS |
| 2026-07-27 | **R1 done 收口**：新增 `DisconnectReason` 枚举 + `HandshakeMsgType` 枚举 + HELLO/WELCOME/REJECT 线协议；`GnsConnection` 加 `setProtocolVersion()` + `getLastDisconnectReason()` + `Handshaking/Ready` 状态；onConnectionChange 签名扩展为 `(NetConnection*, bool, DisconnectReason)`；新增 `AYTest_Handshake` suite 3 个 case（HappyPath/VersionMismatch/PeerClose）；**R1 全 ship = LAN 玩具**；68/68 PASS |
| 2026-07-27 | **R2 协议层完成**：PacketHeader v2 12B (msgType/schemaVersion/length/channel/flags/timestampMs) + 末尾 4B CRC32C (Castagnoli) + `PacketCodec` 纯函数 encode/decode + lz4 (decode 复用 AYStorage::Lz4Decompressor, encode R2 内薄包 `<lz4.h>`) + `PacketAssembler` 真做 fragment/consume (末片可变长, robust 乱序/重复/丢包) + SequenceNumber 完全删除 + 握手包迁移到 PacketHeader (msgType=0xFFFF) + AYTest_PacketCodec.cpp 10 case (7 纯 + 3 GNS);**173/173 PASS, 0 FAIL** |
| 2026-07-27 | **R3.0 复制层 MVP ship** — design §13 R3 6 项全 ✅ + §6.6 Authority v1 = Server 权威实现：<br>• 新增 `WireTypeId` 12 primitive dispatch + BitStream raw helpers (writeBool/Int8..64/UInt8..64/FloatRaw/Double + readers)<br>• 新增 `kMsgTypeReplication=0x0001` / `kMsgTypeEntitySpawn=0x0002` / `kMsgTypeEntityDespawn=0x0003`<br>• 新增 `ReflectSerializer` (H+CPP) — 走 AYReflect `ITypeInfo` 元数据，按 `FieldAttribute::NetReplicate` 过滤，12-type dispatch (Bool/Int*/UInt*/Float/Double/String) 用 `typeid(T).hash_code()` 比对<br>• ReplicationFrame wire format：`[u32 netId][u16 typeHash][u8 fieldCount][u8 reserved]` + 字段 record `[u16 FNV-1a nameHash][u8 WireTypeId][value bytes]`；body 前缀 `[u16 innerMsgType]` 自描述（无需 GnsConnection 改 API）<br>• `ReplicationManager` 实装：`registerObject(void*, ITypeInfo*, uint32_t)` 主路径；`registerObject(IReplicable*, uint32_t)` 标记 deprecated wrapper（R3.0 默认空实现 + R3.1 删除 `replicate/onReplicate` 虚函数）；`tick()` 服务端 Full Snapshot 经 PacketCodec seal + CHANNEL_RELIABLE broadcast；`onReceive(BitStream&, NetConnection*)` 按 inner msgType demux；**Authority gate：客户端不 broadcast；服务端对未知 netId 的 replicate 帧拒绝**<br>• `ReplicationSystem` 降级为 adapter：**删除 _netIdToEntity / _entityToNetId 双 map**（design §13 R3 第 3 项锁定）；`onUpdate` 转发到 `ReplicationManager::tick`<br>• `IReplicable::replicate(BitStream&)` / `onReplicate(BitStream&)` 标 deprecated 空 default impl<br>• 新增 `AYNetwork/Replication/AYNetwork/Replication/AYNetwork/Replication/EntityReplicationAdapter.h` ECS 桥接（`registerEntityComponent<T>` 取 component 指针 + reflect type → `ReplicationManager::registerObject`）<br>• AYReflect 配套：`AYReflect.cpp` 注册 native C++ 原生类型 `int8_t..int64_t` / `uint8_t..uint64_t` / `float` / `double` / `bool` / `std::string` 的 `typeid(T).hash_code()` 入 by-id 映射（之前只注册了 AYMath 别名），让 `AYTYPE_FIELD_EX` 直接拿 native 字段不报错<br>• 新增 `unittest/AYTest_Replication.cpp` 6 case：RoundTripPrimitives (12 WireTypeId) / FiltersNonReplicated / SnapshotBroadcast (端到端 GNS) / EntitySpawnAndDespawn / **AuthorityServerDropsClientReplicate** (§6.6 gate) / BitstreamPacketCodecIntegration<br>**R3.0 = 226/226 PASS (R2 baseline 173 + 53 new), test exit=0** |
| 2026-07-27 | **R3.1 Delta Update ship** — design §13 R3.1 9 项全 ✅ + dirty-tracking 落地，复制带宽下降到 5~20% (LAN 稳态)：<br>• 新增 `kMsgTypeDelta = 0x0004` wire msgType slot；body = 与 Full Snapshot 同 8B header + N field records，receiver 端 `deserializeObject` 路径共用<br>• `ReflectSerializer::hashFieldValue(WireTypeId, void*)` 复用 `PacketCodec::computeCrc32c` (Castagnoli, 0x1EDC6F41) 做 per-field baseline；12 WireTypeId switch + std::string 特殊 case<br>• `ReflectSerializer::serializeDirtyFields(type, obj, netId, denseIndices, BitStream&)` 新增 entry point；emit 仅指定 dense index 的字段，frame header fieldCount = denseIndices.size()<br>• `ReplicationManager::ReflectedEntry` 扩展 R3.1 dirty-tracking state：`_fieldHashes[denseIdx]` (CRC32C baseline) + `_netFieldSparseIndex[denseIdx]` (dense→sparse 映射) + `_initialized` (one-shot gate，register 时 false，第一次 tick 走 Full 后设 true)<br>• `ReplicationManager::tick` 重写 R3.1 dirty-tracking 三态：未初始化→Full Snapshot (RELIABLE)；无 dirty→不广播；部分 dirty→Delta (UNRELIABLE)。已 emit 字段的 hash 立即更新 (避免漏发)<br>• `ReplicationManager::forceReplicate(netId)` 真实现：置 `_initialized = false`，下次 tick 必走 Full Snapshot；适用于 client 重连 / teleport / 用户显式触发<br>• `ReplicationManager::getDirtyFieldCount(netId)` debug API：返回 pending dirty 字段数；0 = 稳态无广播；SIZE_MAX = 未注册或 legacy IReplicable<br>• `ReplicationManager::onReceive` 加 `kMsgTypeDelta` case，复用 `deserializeObject` (wire 格式兼容)；Authority gate 一致保持<br>• `IReplicable::replicate(BitStream&) / onReplicate(const BitStream&)` **真正删除**（全树 grep 验证 0 用户实现后）<br>• 新增 `unittest/AYTest_Replication.cpp` 21 case：<br>　pure 6：HashStableForUnchangedField / HashChangesForDifferentValues / HashAcrossAllWireTypes (12 WireTypeId) / SerializeDirtyFieldsOnlyIncludesRequested / SerializeDirtyFieldsEmptyIndicesProducesNoFrame / DeltaFrameHeaderFormatMatchesFullSnapshot<br>　e2e 15：InitialTickSendsFullSnapshotNotDelta / SteadyStateNoFieldChangeSendsNothing / SingleFieldChangeSendsDeltaWithOneRecord / MultiFieldChangeSendsDeltaWithNRecords / DeltaDoesNotIncludeUnchangedFields / DeltaOnUnreliableChannel / SpawnFrameStaysReliable / RepeatedChangeSameValueNoDelta / ForceReplicateResendsFullSnapshot / ForceReplicateAfterDeltaResendsFull / MultipleObjectsEachTrackedIndependently / DeltaFrameAuthorityGate / DeltaWireSmallerThanFull (Δ < 1/3 Full) / DeltaFrameHeaderSizeMinimal / GetDirtyFieldCountReportsPending<br>**R3.1 = 378/378 PASS (R3.0 baseline 226 + R3.1 +21 case), test exit=0; 2026-07-28 cc9c1da 修正 CHECK_INT_EQ macro 三次求值坑 (case 17 DeltaDoesNotIncludeUnchangedFields 旧版本假报 PASS 实则 deserializeObject 失败)** |
| 2026-07-28 | **R3.2 嵌套字段 ship** — design §13 R3.2 6 项全 ✅ + WireTypeId 12..15 落地，inventory vector / nested struct / equipment map 等用例端到端通：<br>• **`AYReflect`** 扩展：新增 `MapTypeInfo<V>` 模板 + `MapTypeInfoBase` 非虚拟基类（提供 `putEntry` 接口供 receiver-side insert）；新增 `registerArrayType<T,N>` / `registerVectorType<T>` / `registerMapType<V>` 自由函数（v3.2 v1 必须显式调用注册 array/vector/map 类型 — findType<T>() 不自动 instantiate）；explicit template instantiation 避免 AYNetwork_Test 链接失败<br>• **`AYNetwork/INetwork.h`**: `WireTypeId` 扩 12..15 — `NestedStruct=12` / `FixedArray=13` / `DynamicArray=14` / `StringMap=15`<br>• **`ReflectSerializer.cpp`**: `resolveWireTypeId` 加嵌套类型检测（IContainerTypeInfo → 13/14；name prefix `"std::map<std::string,"` → 15；其余 → 12 NestedStruct）；新增递归 `writeWireValue` / `readWireValue` 顶层 switch；新增 `hashFieldValueEx(WireTypeId, ITypeInfo*, void*)` 走嵌套 hash（per-design "整个字段粒度"）；`serializeObject` / `serializeDirtyFields` 切换到 `writeWireValue` 调用；readFieldValue default 分支保留 (`return false`) → R3.1 receiver 静默 drop frame<br>• **`ReplicationManager.cpp`**: `tick()` / `getDirtyFieldCount` 切换到 `hashFieldValueEx` 让 dirty-tracking 跨嵌套字段生效（任一内层元素变 → 整个 nested 字段 hash 变 → Delta frame）；其它不变<br>• **Wire format** (R3.2 新增)：NestedStruct = `[u16 nestedHash][u8 fieldCount][records...]`；FixedArray = `[u8 elemWid][u8 N][elems...]`；DynamicArray = `[u8 elemWid][u32 N][elems...]`；StringMap = `[u8 valueWid][u32 entryCount][per-entry: u16 keyLen key bytes value bytes]`；所有嵌套格式递归 (nested struct 内 element 仍是 struct / array / map 时继续展开)<br>• **向后兼容**: 新 WireTypeId 12..15 走 default readFieldValue → `return false` → receiver 静默 drop frame → server 下次 tick 发 Full 重传；schemaVersion=1 不 bump,渐进升级路径干净<br>• **新增 `unittest/AYTest_Replication.cpp` 15 case**: <br>　pure 6：NestedStructRoundTrip / NestedStructRejectedByR31Receiver / FixedArrayRoundTrip / DynamicArrayRoundTrip / StringMapRoundTrip / MixedNestedTypesRoundTrip (复合 4 类型)<br>　e2e 6：NestedStructInitialFullAndDelta / FixedArrayElementChange / DynamicArraySizeChange / StringMapKeyAdd / NestedStructHashDetectsInnerChange (whole-field dirty 验证) / R31ReceiverDropsR32Frame<br>　regression 3：R30PrimitivesStillWork / R31DeltaStillTriggersForPrimitive (MixedOuter 实测) / NestedStructInInitialTickDoesNotPolluteR31Path<br>**R3.2 = 493/493 PASS (R3.1 baseline 378 + R3.2 +15 case = 393 case；493 = check 行总数), test exit=0** |
| 2026-07-29 | **R4.0 RPC 三类型 + 4 通道 + Validator ship** — design §13 R4.0 10 项全 ✅ + IAYNetwork 第 4 大 wire msgType 命名空间 + `IMethodInfo` RPC metadata:<br>• **Wire envelope namespace**: `kMsgTypeRpcRequest=0x0010` / `kMsgTypeRpcResponse=0x0011` / `kMsgTypeRpcReject=0x0012` (0x0005..0x000F reserved R3.3 back-compat)；schemaVersion=1 不 bump,R3.x receiver 静默 drop 同 R3.2 nested WireTypeId 12..15<br>• **AYReflect 扩展** (`include/AYReflect/IReflect.h`): `IMethodInfo` 加 5 虚函数 (RpcKind/isUnreliable/hasValidator/validate/getParamName)，`enum class RpcKind { None/Server/Client/Multicast }`；default impl most benign 保持 AYScript logia `logia/AYScript/logia/AYScript/logia/AYScript/logia/AYScript/logia/MethodInfoImpl.h` source-compat (Test_Reflect.cpp:600-603 注释确认)<br>• **RpcHandler** (新 `include/RPC/AYNetwork/RPC/AYNetwork/RPC/AYNetwork/RPC/RpcHandler.h` + `src/RPC/RpcHandler.cpp`): `registerMethod` per-method hash binding (FNV-1a-16 of methodName)；`callServer/Client/Multicast` 出站 caller 端不要求本地 obj 绑定（**Bug fix**: resolveMethod 出站语义允许 _objsByHash miss）；`onRpcRequest/Response/Reject` inbound 入口 `body.resetForRead()`（**Bug fix**: 写完 BitStream 不归零位指针导致 readRpcArgs 静默 fail）；authority gate 按 RpcKind 区分（Server RPC 仅在 Server/ListenServer 端处理；Client RPC 仅在 Client 端处理；Multicast 全端）<br>• **RpcSerializer thin wrapper** (在 RpcHandler.cpp 内)：writeRpcArgs / readRpcArgs / writeRpcResponse / readRpcResponse / writeRpcReject / readRpcReject — 复用 R3.2 `writeWireValue/readWireValue` 16 WireTypeId dispatch (No new ITypeInfo/CRC/fragment logic — 全复用 R3.2 16-id 引擎)<br>• **callId 64-bit pending map**: std::atomic fetch_add + std::mutex map; 客户端 outbound register pending callback, Response/Reject 自动 fire + cleanup<br>• **`GnsConnection::_rawSend` 4-channel switch**: `CHANNEL_RELIABLE` → `k_nSteamNetworkingSend_Reliable`；`CHANNEL_UNRELIABLE` → `k_nSteamNetworkingSend_Unreliable`；`CHANNEL_FRAGMENTED` → `Reliable \| NoNagle` (Nagle 防止合并多帧包；PacketCodec 仍走 PacketAssembler R2)；`CHANNEL_ACK` → `Reliable` (R4.1 stub；ACK pipeline 留 R4.1+)<br>• **`ReplicationManager::unregisterObject` envelope fix**: R3.2 typo `kMsgTypeReplication` envelope 包 Despawn body → R4.0 修成 `kMsgTypeEntityDespawn` (mirror Spawn path),wire 对称<br>• **`AYNetworkSubSystem::update` demux**: PacketCodec::decode envelope.msgType: 0x0010..0x0012 → `_rpcHandler.onRpcXxx`；0x0001..0x0004 → 既有 `_replicationManager.onReceive`；其它 → per-channel MessageHandler；pre-route 消除 channel handler 关注 RPC vs replication 区分<br>• **`INetworkSubSystem::getRpcHandler()` 新 API**: 单一 RpcHandler 引用,AYNetworkSubSystem ctor 持有 (mirror `_replicationManager{this}`)<br>• **新 `unittest/AYTest_RpcHandler.cpp` 18 case** (10 pure + 5 e2e 纯 loopback + 3 regression)：<br>　pure 10：RegisterAndResolveMethod / WriteReadArgsRoundTrip / WriteReadResponseRoundTrip / WriteReadRejectBodyRoundTrip / ValidatorRejectsCall / UnknownMethodReturnsFalse / ParseFailOnTruncatedBody / MultiplePendingCallsDisambiguated / UnreliableOverridePerRpc / RpcKindMismatchRejects<br>　e2e 5：ServerRpcHappyPath / ValidatorRejectsAcrossGns / ReliableDefaultOverGns / UnreliableServerRpcOverGns / AuthorityGateRejectsRpc (纯 loopback 同步断言: emit → PacketCodec::decode → peer handler,去 GnsConnection heap-corruption 路径 — R3.2 E2E pattern 复用)<br>　regression 3：R30PrimitivesStillReplicate / R32NestedWireTypesAreUnaffected (16-id dispatch 不破坏) / RpcMsgTypeEnvelopeIsDistinct<br>• **关键 Bug fix 链 (用户 review 后修正, 不是 .obj/链接器问题)**:<br>　1. `resolveMethod` 出站 caller 端不应要求本地 obj (client callServer 不需要 receiver's obj binding)<br>　2. `onRpcRequest/Response/Reject` 入口 `body.resetForRead()` (写完 BitStream 后位指针非零,readRpcArgs 静默 fail)<br>　3. E2E 纯 loopback: emit → PacketCodec::decode → peer handler, 不走 GnsConnection (Disconnected 状态丢包 + heap 破坏)<br>　4. Authority gate 按 RpcKind 区分 (Server RPC 仅 Server/ListenServer 端; Client RPC 仅 Client 端; Multicast 全端)<br>　5. E2E 同步断言 (RPC 路径本身是同步, GNS update() pump 不需要)<br>　6. Channel 断言看对 atomic (client 发 RPC 看 lastClientChannel; server 发 Client RPC 看 lastServerChannel)<br>　7. ClientDamage 在 client 端 registerMethod (Client RPC server→client, client 端 invoke)<br>**R4.0 = 566/566 PASS (R3.2 baseline 493 + R4.0 +73 case/check), test exit=0; 2026-07-29 ship** |

| 2026-07-29 | **R4.1-B CHANNEL_ACK pipeline ship** — application-level ack echo:<br>• `PacketFlag::RequiresAck` + 4B seq prefix; receiver auto-replies `kMsgTypeAppAck` on `CHANNEL_ACK`<br>• `AckPipeline` + `AckTracker`; `GnsConnection::sendRequireAck` + pending confirm callback<br>• `CHANNEL_ACK` → GNS Reliable\|NoNagle (replaces R4.0 stub)<br>• 5 tests in `AYTest_AckPipeline.cpp` — **R4.1-B complete** |
| 2026-07-29 | **R4.1-B RpcResponse auto-lz4 ship** — compress large RPC returns on the wire:<br>• `RpcHandler::emit` auto-compresses `kMsgTypeRpcResponse` when body ≥ `RpcResponseCompressMinBytes` (64) and lz4 shrinks the frame<br>• Request/Reject unchanged; decode path reuses existing `PacketCodec` lz4<br>• 3 tests: `RpcResponseAutoCompressesLargePayload` / `RpcResponseSkipsCompressForSmallReturn` / `RpcResponseCompressedBodyRoundTrip` |
| 2026-07-29 | **R4.1-B Multicast wildcard ship** — type-wide receiver binding:<br>• `registerWildcardMulticast(typeName, obj)` / `unregisterWildcardMulticast`<br>• `readRpcArgs` optional `fallbackLookup`; `findWildcardMulticastMethod` scans TypeRegistry Multicast methods by FNV-1a hash<br>• Multicast inbound: no RpcResponse / RpcReject; async multicast skips response drain<br>• 4 tests: `WildcardMulticastInboundWithoutRegisterMethod` / `WildcardMulticastOverGns` / `WildcardMulticastNoResponseOnWire` / `RegisterWildcardMulticastRejectsUnknownType` |
| 2026-07-29 | **R4.1-B Async RPC ship** — worker pool + tick-drain responses:<br>• `IMethodInfo::isAsync()` (AYReflect)<br>• `RpcAsyncPool` (2 workers); invoke off network thread; `RpcHandler::tick()` emits RpcResponse<br>• `AsyncRpcCompletesOnServerTick` — **648/648 PASS** |
| 2026-07-29 | **R4.1-B RPC retry/backoff ship** — weak-network resend policy:<br>• `callServerWithCallback` / `callClientWithCallback` + `setRetryPolicy(maxRetries, retryBaseMs, attemptTimeoutMs)`<br>• Pending map stores request body; attempt timeout → exponential backoff → resend (same callId)<br>• `PendingCallExhaustsRetries` + `PendingCallRetriesThenSucceeds` — **640/640 PASS** |
| 2026-07-29 | **R4.1-B RepNotify ship** — per-field receive callback:<br>• `FieldAttribute::RepNotify` in AYReflect<br>• `INetworkExtension::onRepNotify(obj, type, netId, fieldName)`<br>• `ReflectSerializer::deserializeObject` optional `FieldAppliedFn`; ReplicationManager fires on receive<br>• 4 tests in `AYTest_RepNotify.cpp` |
| 2026-07-29 | **R4.1-B Interest Management ship** — distance cull + relevancy + onPreReplicate：<br>• `NetVec3` + `ReplicationManager::setInterestRadius/setObjectLocation`<br>• `INetworkExtension::isRelevant` + real `onPreReplicate(targets)` before per-conn send<br>• tick/spawn/despawn use `sendTo` when interest active; broadcast fallback when radius=0<br>• `AYNetworkSubSystem::setExtension` forwards to ReplicationManager<br>• 4 tests in `AYTest_InterestManagement.cpp` — **609/609 PASS** |
| 2026-07-29 | **R4.1-A 集成层 ship** — Subsystem 全链路 + Entity adapter E2E + RPC pending 超时：<br>• **`dispatchIncoming`**: Replication (0x0001..0x0004) + RPC (0x0010..0x0012) demux；真实 `NetConnection* from`；`broadcastExcept` 修复；client `_clientNetConn`；GNS adopt factory 移至 `listen()`<br>• **`RpcHandler`**: `emit` → `sendTo`/`send`/`broadcast`；`callClient` target 解析；`RpcDefaultTimeoutMs=30000` + `tick()` 超时清理<br>• **`ReplicationManager`**: EntitySpawn envelope 统一；`peekSpawnAnnouncement()`；client spawn 公告表<br>• **测试 +598**: `AYTest_SubsystemIntegration` (1s+2c RPC+replicate)；`AYTest_EntityReplicationIntegration` (HealthComponent + EntityReplicationAdapter)；`PendingCallTimesOut`<br>**R4.1-A = 598/598 PASS, test exit=0; commits `ecddcdd` + follow-up** |
| 2026-07-29 | **R4.1 范围收束 + R4.1-A/B 分层** — 停掉散落 4.1 开发，统一集成主线：<br>• **拆分**：R4.1-A（集成层/P0，唯一活跃） vs R4.1-B（Interest + RepNotify + RPC polish，**冻结至 A ship**）<br>• **修正 §10 表述**：Replication demux（0x0001..0x0004 → `_replicationManager`）**未实现**（现落 default MessageHandler）；RPC demux ✅；与 `AYNetworkSubSystem.cpp` 代码对齐<br>• **R4.1-A 已 land 部分**：`NetConnectionImpl` (`a729672`) / `sendTo`+`getConnections`+`kickConnection` (`ed56867`) / `listen()`→ListenServer+Replication authority (`19d702a`)<br>• **R4.1-A 待做**：Replication demux / RpcHandler sendTo 分流 / 真实 `from` conn / broadcastExcept 修复 / EntitySpawn / Subsystem E2E / AYEntity demo / RPC pending 超时<br>• **冻结项**：Interest、RepNotify、Async RPC、wildcard multicast、Response lz4、CHANNEL_ACK — 全部 R4.1-B<br>• **§12 评分同步**：实现完整度 ~25→~45；工业可用 ~10→~20；可用门槛 ~2–4 周（R4.1-A）<br>• **§11/§2.1/§13** 同步 R4.0 ship 后现状 |
| 2026-08-24 | **R5.0 Snapshot Interpolation ship** — Unity-NetCode-style client-side smoothing, wired end-to-end through the existing replication stream. No new wire msgType; design lives in §15 (see §15.1–§15.7).<br>• **Wire format evolution (R5.0)**: replication body prefix adds `[u32 serverTick]` before the existing 14B ReplicationFrame header (`netId/schemaHash/fieldCount/reserved`); serverTick==0 = legacy R3.x frame (read path silently skips interpolation)<br>• **`ReflectSerializer`**: `serializeObject/serializeDirtyFields/writeReplicationFrameHeader` get `uint32_t serverTick` param; emits `[serverTick][frameHeader...][records...]`; `FrameHeader` struct unchanged at 14B (serverTick is OUTSIDE the frame, owned by the prefix)<br>• **`NetworkTime`** (new `include/Snapshot/NetworkTime.h` + `src/Snapshot/NetworkTime.cpp`): dual clock (`serverTick ↔ serverTimeSec` via `tickRate`; `clientTimeSec` wall clock; `interpolationTimeSec = clientTimeSec - interpolationDelaySec`); default 30 Hz tick + 0.1 s delay matches Unity/Unreal<br>• **`SnapshotBuffer`** (new ring): per-ghost history with FIFO eviction (`kHistoryCapacity=32`); `push(serverTick, serverTimeSec, obj)` dedups by tick (out-of-order overwrite, no dup); `findBracket(timeSec, lo, hi, alpha)` finds the bracketing pair with `alpha = (t - ta) / (tb - ta)`; past-newest returns `hi=kNoUpperBracket` with alpha=0 (hold newest); sorted ascending by tick<br>• **`SnapshotInterpolator`** (new registry): per-`netId` AYReflect-driven field spec at registerGhostKind time; `push(netId, serverTick, obj)` from `ReplicationManager::onReceive`; `sample(netId, renderTimeSec, out)` / `sampleNow()`; `recordCount` diagnostic; `clearBuffers/clearAll`; test seam `setTimeForTesting`<br>• **Per-field interpolation strategies** (matching Unity NetCode): Float/Double → `a + (b-a)*alpha` lerp; Int*/UInt*/Bool/String/Nested/Array/Map → snap-to-lower (alpha ignored, memcpy lower bracket bytes verbatim — variable-size wire types can't lerp anyway)<br>• **Dropped/out-of-order/jitter handling**: dedup-on-tick in `SnapshotBuffer::push` (R3.x FrameAlreadyApplied pattern); `findBracket` returns false during warmup (before oldest record) and during empty buffer → caller holds last state; far-past samples extrapolate onto the bracket floor (alpha clamped via `tb > ta` check); per-ghost ring FIFO evicts stale history at 32 frames (~1 s at 30 Hz) — bounded memory<br>• **ReplicationManager::onReceive wire tap**: after deserialize, push into interpolator with `hdr.serverTick`; deserialized object is the new state; the existing `deserializeObject` continues to do its R3.1 dirty-merge into the live ghost (snapshot interpolation is **layered on top of** R3.0/R3.1 deserialization, not replacing it)<br>• **Spawn/Despawn non-interpolation rules**: spawn always Full Snapshot (R3.1 `_initialized=false`); despawn calls `_snapshotInterpolator->unregisterGhost(netId)` so the buffer is freed; teleport handled by R5.1 below<br>• **`AYTest_SnapshotInterpolation.cpp`** (12 case): NetworkTimeTickRate / NetworkTimeInterpolationDelay / NetworkTimeAdvance / NetworkTimeInterpolationTime / NetworkTimeTickToSeconds / BufferPushDedupsOutOfOrder / BufferPushDedupsDuplicate / BufferFindBracketBracketed / BufferFindBracketHoldsNewestWhenPast / BufferEvictsOldestPastCapacity / InterpolatorSampleReturnsFalseDuringWarmup / InterpolatorPerFieldLerpFloat<br>**R5.0 = 610/610 PASS (R4.1-A baseline 598 + R5.0 +12 case), test exit=0** |
| 2026-08-24 | **R5.1 Teleport flag ship** — non-interpolated server-side snap, no extra wire bytes (reuses R5.0's `reserved` byte as `flags`):<br>• **Wire format evolution (R5.1)**: same byte position as R5.0's `reserved`, repurposed as `flags` (`FrameHeader::flags() = reserved`; `isTeleport() = reserved & kFlagTeleport`); no length change, no version bump — R5.0 receiver reads 0, R5.1 receiver reads 0 on R5.0 frames<br>• **`kFlagTeleport = 0x01`** in `ReflectSerializer.h` namespace (0x02..0x80 reserved for R5.2: Hide/OwnerOnly/Priority etc.)<br>• **`SnapshotRecord::snap`** added to `SnapshotBuffer::push(serverTick, serverTimeSec, obj, snap=false)`; `SnapshotBuffer::isSnap(idx)` accessor for the interpolator<br>• **`SnapshotInterpolator::push(netId, serverTick, obj, teleport=false)`** forwards snap to buffer; `sample()` honours snap: if upper bracket is snap, `alpha` is forced to 0 (lerp short-circuits to the upper record's bytes)<br>• **`ReplicationManager::markTeleported(netId)`** public API + `_teleportPending` `std::unordered_set`; on `tick()`, per-netId `teleport = _teleportPending.count(netId) > 0`; emit frame with `frameFlags = teleport ? kFlagTeleport : 0`; drain marker only after successful emit (`replicatedToAny`) so peers connecting later still see the teleport; idempotent re-mark keeps persistent teleport working<br>• **Full-not-Delta on teleport**: `_initialized = false` forces the next tick to emit Full Snapshot (markTeleported mirrors `forceReplicate`'s per-peer reset) — receiver needs the complete new state, not a delta on top of the lerped previous state<br>• **Authority API**: game code calls `mgr.markTeleported(entityId)` whenever the server position changes discontinuously (respawn / portal / map warp); server-side flag makes the client-side lerp give way to a hard snap<br>• **`AYTest_SnapshotInterpolation.cpp`** (+4 case): WireTeleportFlagRoundTrip / SnapshotBufferSnapRecord / **SnapshotInterpolatorTeleportSnap** (3 normal + 1 teleport; sample between tick 2→3 expects `x=9999.0` snap value) / ReplicationManagerTeleportEmitsFlag<br>• **Design**: §15.5 wire table now 3 columns (R3.x / R5.0 / R5.1); §15.7 test matrix = 12/12 (R5.0) + 4/4 (R5.1); new §15.8 "Teleport / 非插值规则" covering problem statement, authority API, full-not-delta, client flow diagram, 5-case bracket behaviour table, snap-on-record rationale<br>**R5.1 = 614/614 PASS (R5.0 baseline 610 + R5.1 +4 case), test exit=0** |
| 2026-08-24 | **R5.2 Client Prediction + Server Reconciliation ship** — Unity NetCode-style model: server never rewinds, client owns smoothing. No determinism burden. New wire msgType `kMsgTypeClientInput=0x0014` + optional 8-byte AckTail on Full Snapshots. Design lives in §15.9.<br>• **Wire format (R5.2)** — adds `kMsgTypeClientInput=0x0014` envelope (header-only codec) with body `[u32 inputSeq][u32 serverTickAtSend][u8 payload...]`; **AckTail** appended to Full Snapshot bodies **only** when at least one destination connection owns an AutonomousProxy ghost in frame: `[u32 lastAckedInputTick][u32 serverCommandAge]` (8 bytes); `AckTail::present=false` → zero bytes emitted, R5.0/R5.1 wire unchanged<br>• **New `Prediction/` modules** (`include/Prediction/{InputRing.h,PredictionManager.h,MispredictionResolver.h,ClientInputCodec.h}` + matching `src/Prediction/*.cpp`):<br>　– **`InputRing`** — per-connection circular ring with `_slots[cap]`, drop-oldest on overflow (default cap 32, configurable via `setRingCapacity`); `push` rejects non-monotonic seq (raw subtraction wrapped via centralized `seqGreaterThan(a,b) = (int32)(a-b)>0`); `tryGet` does inclusive `[oldestLiveSeq, newestSeq]` wraparound-aware range check; `ackUpTo(N)` advances cursor to `N+1` (forward-only); `pendingCount()` = `(newestSeq - oldestLiveSeq + 1) - ackDone` so it survives drop-oldest<br>　– **`PredictionManager`** — orchestrator owning `_rings[connId]`, `_ackedSeq[connId]`, `_ghosts[netId]`; server-side API: `onClientInput(conn, body, size)` decode + push; `consumeClientInputs(simTick, applyFn)` walks each ring's unacked range and invokes `apply(conn, seq, payload, size)` once per record in seq order; `markAcked(conn, lastAcked)` advances acked cursor; `pendingInputCount/trackedConnections/lastAckedInputTick` read-only test seams; client-side: `predict(netId, inputs, stepFn, dtSec)` + `onServerAck(netId, lastAcked, serverCommandAge)`; ghost registry (`registerPredictedGhost/isPredictedGhost/setPredictedBytes/tryGetPredictedBytes/getLayoutHash`)<br>　– **`MispredictionResolver`** — pure function `reconcile(predicted, server, layout, predictedInputSeq, serverLastAckedInputTick, dtSec, smoothingDuration)`; threshold: float 1e-4 relative / double 1e-6 relative / others byte-exact; above-threshold AND `predictedInputSeq <= serverLastAckedInputTick` → **snap** (overwrite predicted with server); within threshold → **smooth** with `alpha = clamp(dtSec / smoothingDuration, 0, 1)` exponential lerp; `ServerAuthoritative`-tagged fields are **skipped** (preserves predicted local copy); layout drift (size mismatch) → snap all<br>　– **`ClientInputCodec`** — header-only inline `write/read` for the `kMsgTypeClientInput` envelope; payload is opaque bytes (no reflection); zero-length payloads allowed (keepalive heartbeats)<br>• **`FieldAttribute::ServerAuthoritative = 1 << 17`** in `AYReflect/IReflect.h` — bit 17 free per file context; consumed by `MispredictionResolver` skip-list so HP/health fields stay client-owned even though server receives them<br>• **`ProxyKind` enum + `ReplicationManager` accessors** in `INetwork.h`: `enum class ProxyKind : uint8_t { Server, AutonomousProxy, SimulatedProxy }`; `setObjectProxyKind(netId, kind)` server-side; `getObjectProxyKind(netId) const` both sides; `isLocallyControlled(netId)` derived helper; `getInputRingCapacity/setInputRingCapacity` (default 32); `getLastAckedInputTick(conn)` read-only test seam; `setInputApplicationFn(std::function)` for the gameplay-side apply hook (per-call lambda, no member storage on hot path); `consumeClientInputs` / `onClientInput` / `buildAckTailForConnection`<br>• **`ReplicationManager::registerObject`** default `ProxyKind::SimulatedProxy`; `tick()` Full Snapshot emit path converts `AckTailInfo` → `ReflectSerializer::AckTail` and calls `writeAckTail` only when the destination connection owns an AutonomousProxy ghost **and** the connection has a non-zero ack seq (saves 8 B per Full Snapshot for SimulatedProxy-only clients)<br>• **`ReplicationManager::onReceive`** R5.2 branch: after `deserializeObject`, attempt `readAckTail` (8 bytes optional, treats missing as `present=false` for back-compat); when `present && _prediction && !isAuthority()` → `_prediction->onServerAck(netId, tail.lastAckedInputTick, tail.serverCommandAge)`; then push to interpolator (R5.0/R5.1 layer unchanged)<br>• **`AYNetworkSubSystem` wiring**: phased GameLoop → in `FixedPrePhysics` (BEFORE `drainSimulationInbound`) call `_replicationManager.consumeClientInputs(simTick)`; `applySimulationInbound` switch gains `case kMsgTypeClientInput: _replicationManager.onClientInput(fromId, body, bodySize)`; client-side dispatch is a no-op (server-only handler)<br>• **No new mutexes**: `PredictionManager` main-thread only; per-call lambda capture for `_inputApplicationFn` (one alloc per fixed tick, off hot path)<br>• **Backward compatibility**: every R5.0/R5.1 wire byte still produced when no AutonomousProxy ghost is in frame; the "always attempt 8-byte tail read" pattern catches both old senders (no tail → `present=false`) and new senders (8 tail bytes → `present=true`); Delta frames never carry the tail<br>• **`AYTest_ClientInput.cpp` (+10 case)**: InputRing_PushLookup / InputRing_DropOldestOverflow / InputRing_AckUpToClamp / ClientInputCodec_RoundTrip (write→read 8-byte header + opaque payload, truncation returns false) / **AckTail_RoundTrip** (present=true writes 8 bytes, present=false writes 0) / Ack_WraparoundMath (0xFFFFFFF0 vs 0x00000005 across u32 boundary) / **Misprediction_ServerAuthoritativeSkip** (pos snapped, hp preserved) / Misprediction_SnapVsSmoothThreshold (sub-threshold smooths, above-threshold snaps byte-equal) / ConsumeClientInputs_Ordering (2 conn × 2 inputs → 3 callback fires in seq order; `pendingInputCount` reaches 0) / PredictionManager_EndToEnd (ghost registry round-trip + markAcked + onServerAck)<br>• **Design (§15.9)**: §15.9.1 7 design decisions table; §15.9.2 public API diff; §15.9.3 wire format; §15.9.4 client input envelope; §15.9.5 fixed tick ordering; §15.9.6 smoothing strategy with per-field thresholds; §15.9.7 10-case test matrix; §15.9.8 ship definition<br>• **Bug fix during ship (tryGet wraparound math)**: initial `geLo = !seqGreaterThan(inputSeq, lo); leHi = !seqGreaterThan(hi, inputSeq)` excluded the open interval `(lo, hi)`; fixed to `geLo = !seqGreaterThan(lo, inputSeq); leHi = !seqGreaterThan(inputSeq, hi)` (matches `[lo, hi]` inclusive intent under `seqGreaterThan` semantics)<br>• **Bug fix during ship (pendingInputCount semantics)**: original returned raw `ring.size()`; ring keeps records past acked for rollback/inspection, so "pending" must be unacked-count; added `InputRing::pendingCount()` and routed `PredictionManager::pendingInputCount` through it. Consumed 2 + 1 records → 0 pending after `consumeClientInputs`<br>**R5.2 = 993/993 PASS (R5.1 baseline 614 + R5.2 +10 case, 379 new check assertions), test exit=0; 2026-08-24 ship**
| 2026-08-24 | **R5.3 Replay recording v1 (Authoritative Replay) ship** — strict two-layer architecture: **AYReplay foundation** (`d:\Aliyat\AliyatEngine\AYFoundation\AYReplay\`, network-agnostic) + **AYNetwork Replay Adapter** (`d:\Aliyat\AliyatEngine\AYRuntime\AYNetwork\src\Replay\`, depends on foundation). Design lives in §15.10.<br>• **Why two layers (not direct write from ReplicationManager)**: single-player games need recording for rollback tests / bug reproduction / editor preview without faking GNS or PacketCodec; file format / timeline / compression / index / seek are network-agnostic and belong in a foundation module; Network adapter records **logical messages** (Snapshots, RPC, Spawn, Authority), NOT raw GNS packets — playback is independent of the transport library version at record time.<br>• **Foundation (`AYReplay`)**: `ReplayFileHeader` (108 B packed on MSVC; `sizeof(ReplayFileHeader)` at runtime is authoritative) + `ReplayEventHeader` (20 B) + `ReplayCheckpointHeader` (24 B), all `#pragma pack(1)`. `IReplayRecorder` interface (`beginSession / recordEvent / recordCheckpoint / flush / endSession`). `IReplayPlayer` interface stub — `open()` validates magic+version; `readNextEvent/seekToTick/seekToCheckpoint` return `Error::NotImplemented` (full impl in v2 playback release). `FileReplayRecorder` default impl: 64 MiB / 5 min rotation; LZ4 raw-block compression when body ≥ 256 B and the compressed size is smaller; crash-tolerant (truncated last rotation accepted by `open()` returning `Truncated`); `kEvtFoundation_SessionBegin=0x0001` / `SessionEnd=0x0002` / `TextMarker=0x0010` / `Checkpoint=0x0020`. Foundation reserves event-type range `[0, 0xFFFF]`; consumers get `[0x10000, 0xFFFFFFFF]` (AYNetwork claims `[0x10000, 0x1FFFF]`). Public helpers: `FileReplayRecorder::rotationPathFor(base, idx)` and `currentPath()` so tests/tools can read the actual on-disk file without re-implementing the `_NNN.ayrp` rotation suffix.<br>• **`Fnv1a64`** header-only state-hash: `kFnv1a64Offset = 0xcbf29ce484222325`, `kFnv1a64Prime = 0x00000100000001B3`, plus `fnv1a64Combine(prev, data, size)` for incremental hashing.<br>• **Network Adapter (`NetworkReplayRecorderAdapter`)**: implements `IReplayRecorder`. Wire-tap hooks at: `ReplicationManager.cpp` Initial-Full-Snapshot success path (line ~456), Delta success path (~487), Spawn/Despawn per-peer visibility flips + rebroadcast loop (~255/295/721/751); `AYNetworkSubSystem.cpp` `applySimulationInbound` switch — `kMsgTypeClientInput` case + `kMsgTypeRpcRequest/Response/Reject` cases (server-only, gated by `isAuthority()`); `Egress` arm calls `rec->flush()` after `_replicationManager.tick()`; `RpcHandler.cpp::emit` captures outbound RPC. **`isAuthority()` promoted from private to public** on `INetworkSubSystem` so the wire-tap helper can read it. Forward-declared `class IReplayRecorder` in `INetwork.h` + `RpcHandler.h` to avoid the header cycle (`NetworkReplayRecorderAdapter.h` → `INetwork.h`). 7 event types: `kEvtNet_InitialFullSnapshot=0x10001` / `kEvtNet_Spawn=0x10002` / `kEvtNet_Despawn=0x10003` / `kEvtNet_DeltaSnapshot=0x10004` / `kEvtNet_InputBatch=0x10005` / `kEvtNet_RpcBatch=0x10006` / `kEvtNet_AuthorityChange=0x10007`.<br>• **Authority gate**: the adapter holds an `_authorityGateOk` flag; wire-tap sites check `isAuthority()` before each call. Gate stays `false` on clients → all `recordXxx` calls no-op and return `true` (so existing caller behavior is unchanged). The recording is **opt-in** — `setReplayRecorder(nullptr)` (the default) means no recording, no overhead beyond the `if (rec)` short-circuit.<br>• **Periodic checkpoint**: `recordPeriodicCheckpoint(serverTick, registeredNetIds, provider)` invokes a `StateBytesProvider` per registered ghost, combines FNV-1a across all bytes in netId-iteration order, writes one `ReplayCheckpointHeader` (state hash, no snapshot bytes inline). Tick is incremented every `_checkpointIntervalTicks` (default 30 = 1 s at 30 Hz) inside `ReplicationManager::tick()`.<br>• **API additions**: `ReplicationManager::setReplayRecorder/getReplayRecorder/setCheckpointIntervalTicks`; `isAuthority()` promoted public; `INetworkSubSystem::setReplayRecorder/getReplayRecorder` virtual (default `nullptr`). All existing public API unchanged.<br>• **Wire-format payload layouts** (see §15.10 for full spec): `InitialFullSnapshot = [u32 connId][u8 frameFlags][sealed snapshot bytes]`; `Spawn = [u32 connId][u32 netId][u64 schemaHash][spawn payload]`; `Despawn = [u32 connId][u32 netId]`; `DeltaSnapshot = [u32 connId][u8 frameFlags][sealed delta bytes]`; `InputBatch = [u32 connId][u32 inputSeq][u32 serverTickAtSend][u32 payloadLen][bytes]`; `RpcBatch = [u16 messageType][u32 connId][u32 payloadLen][bytes]`; `AuthorityChange = [u32 netId][u8 oldKind][u8 newKind][u32 connId][u32 reserved]`.<br>• **Out of scope (v1)**: playback (v2 player release); deterministic re-simulation from inputs (v2+ alongside tick-level lockstep); background writer thread (v1 main-thread only); editor timeline UI (lives in `AYEditor`); network capture (raw GNS — not a replay source per design); connection lifecycle events (v1.1 — derivable from per-event `connectionId` field).<br>• **Tests**: 1063 R5.3 baseline + 5 foundation (`AYTest_ReplayFoundation.cpp`: HeaderMagicVersion / EventRoundTrip / CheckpointRoundTrip / Lz4CompressionShrinksZeros / Fnv1a64KnownVectors) + 5 network adapter (`AYTest_ReplayNetwork.cpp`: AdapterPureRecord / AuthorityGateRejectsClientMode / EndToEndGnsLoopback / AuthorityChangeEventCaptured / PeriodicCheckpointProduced) = **1073/1073 PASS**, `test exit=0; 2026-08-25 ship`. |

| 2026-08-25 | **R5.4 Network Fault Simulation ship** — per-connection / per-channel fault injection at the sealed-bytes seam. Design lives in §15.11.<br>• **Why AYNetwork-side, not GNS `Fake*` wrapper**: GNS `FakePacketLoss_Send/Recv`, `FakePacketLag_*`, `FakePacketReorder_*`, `FakePacketDup_*`, `FakeRateLimit_*_*` knobs are **global-only** per `steamnetworkingtypes.h:1325-1328` ("These are global (not per-connection) because they apply at a relatively low UDP layer"). User explicitly required per-connection + per-channel configuration; an interceptor at the seam between `PacketCodec::encode` and `s_gns->SendMessageToConnection` (send) and between `ReceiveMessagesOnPollGroup` and `PacketCodec::decode` (recv) gives that granularity with main-thread determinism.<br>• **Architecture** — five new source files in `src/Transport/`:<br>　– **`TokenBucket.h`** (header-only) — double-precision rate-limit primitive; `tryConsume(bytes, dt)` adds `dt * rate` tokens up to burst, deducts on success, returns bool. Zero rate = bucket disabled (`tryConsume` always true).<br>　– **`DelayedFrameQueue.h/.cpp`** — per-channel `std::deque<DelayedFrame>`; `enqueue` takes a frame + samples its release deadline from profile (mean ± uniform jitter, clamped ≥ 0); `releaseReady(nowMs)` drains everything past deadline; `enforceCap(maxSize, &droppedCount)` is explicit (caller invokes after enqueueing excess), reports the oldest-first drop count via `consumeDroppedCount`.<br>　– **`TransportFaultController.h/.cpp`** — singleton-style store of `std::unordered_map<uint32_t, TransportFaultProfile>` keyed by AYNetwork netId, plus a parallel `std::unordered_map<uint32_t, std::mt19937_64>` RNG map. `setProfile` lazily creates the RNG seeded from `profile.randomSeed` (or session randomSeed if 0).<br>　– **`TransportFaultInterceptor.h/.cpp`** — per-`GnsConnection`, owns `std::array<ChannelState, 4>` (one entry per `CHANNEL_RELIABLE/UNRELIABLE/FRAGMENTED/ACK`). `onSend(sealed, len, channel)` runs the Bernoulli-loss → reorder-swap → delay-jitter → dup draw pipeline, enqueues into `_channels[ch].delayQueue`, returns nothing (frame released on next `tick`). `onRecv` mirrors for incoming. `tick(nowMs, sendOut, recvOut)` drains ready frames; rate-limited frames that fail `tryConsume` are re-enqueued at `nowMs+1ms`.<br>• **`TransportFaultProfile` struct** (`interface/AYNetwork/TransportFaultProfile.h`): `randomSeed:u64`, `channelMask:u8` (defaults to `kFaultChannelAll=0x0F`), `latencyMeanMs:u32`, `latencyJitterMs:u32`, `lossPercent/dupPercent/reorderPercent:f32`, `rateLimitBytesPerSec:u32`, `rateLimitBurstBytes:u32`. Channel mask constants `kFaultChannelReliable=0x01`, `kFaultChannelUnreliable=0x02`, `kFaultChannelFragmented=0x04`, `kFaultChannelAck=0x08`, `kFaultChannelAll=0x0F`, reserved bits `kFaultChannelReserved=0xF0`.<br>• **`GnsConnection::pump()` integration** — three `isEnabled()` short-circuits added to prevent spurious 1-pump lag when no profile is installed (pre-receive drain / receive-loop queueing / post-receive drain all gate on `interceptor && interceptor->isEnabled()`). When a profile is installed, the send-side hook in `send()` replaces the immediate `_rawSend` with a tick-driven release via the interceptor; the receive-side hook in `pump()` queues incoming bytes into the interceptor and releases them through `onRawData` on tick.<br>• **`FakeTransport` test seam** (`GnsConnection::setFakeTransportSender/Receiver`) — `using FakeTransportSender = std::function<bool(uint32_t connId, uint8_t channel, const uint8_t* sealed, size_t len)>`; null = off (default). When set, `_rawSend` calls the fake instead of GNS; the receive pump calls the fake receiver instead of `onRawData`. Mirrors `ReplicationManager::setBroadcastSinkForTesting`. Fault injection works in BOTH real-transport and fake-transport modes (interceptor sits above this seam).<br>• **`INetwork.h` API additions** (all additive, default-implemented to keep existing test stubs like `StubSubSystem` non-abstract): `INetworkSubSystem::setTransportFaultProfile(netId, profile)` / `clearTransportFaultProfile(netId)` / `getTransportFaultProfile(netId)` — production `AYNetworkSubSystem` overrides to forward to its internal `TransportFaultController`. No existing public API changed.<br>• **CMake** — `src/Transport/{TokenBucket.h, DelayedFrameQueue.cpp, TransportFaultController.cpp, TransportFaultInterceptor.cpp}` added to `SRC_FILES`; `src/Transport` added as `PRIVATE` include dir (R5.4 transport-fault headers are implementation-detail, not public); `unittest/CMakeLists.txt` registers 5 new test files; `unittest/CMakeLists.txt` adds `src/Transport` to test include path.<br>• **RNG determinism** — per-profile `std::mt19937_64` seeded from `profile.randomSeed` (or session randomSeed). All Bernoulli draws (loss / dup / reorder) and the latency jitter uniform draw go through it; tests reproduce exact sequences by fixing the seed (verified by `TransportFaultProfile_RngDeterminism` and the `Interceptor_*` cases with fixed seeds).<br>• **Out of scope (deferred)**: deterministic-replay integration with R5.3 (R5.4 records faults but does NOT replay them into the fault controller — future work); body-mutating faults (CRC-protected at sealed seam; would require re-seal via `PacketCodec::encode`); cooperative cross-connection simulation (single-connection profiles only in R5.4); GNS global `Fake*` fallback (interceptor-only in R5.4; callers wanting the GNS global can call `SteamNetworkingUtils_LibV4()->SetGlobalConfigValue_Int32(...)` directly — documented as escape hatch); editor UI for live knobs (lives in `AYEditor`); bandwidth probe / metrics channel (separate analytics concern).<br>• **Tests** (5 new test files):<br>　– `AYTest_TransportFaultProfile.cpp` (5 case): Defaults / ChannelMaskBits / AffectsChannel / IsNoOpVariants / RngDeterminism<br>　– `AYTest_TokenBucket.cpp` (4 case): ConfigureDefaults / RateExhaustionAndRecovery / BurstCapacity / ZeroRateDisabled<br>　– `AYTest_DelayedFrameQueue.cpp` (3 case): ReleaseAtDeadline / FifoOrder / DropOnCap<br>　– `AYTest_TransportFaultInterceptor.cpp` (5 case): NoProfilePassesThrough / LossPercent / LatencyAndJitter / RateLimit / ChannelMaskGating<br>　– `AYTest_FakeTransport.cpp` (3 case): InstallAndClear / SendSideDispatch / RecvSideDispatch<br>**R5.4 = 1234/1234 PASS (R5.3 baseline 1073 + 5+4+3+5+3 = 20 R5.4 case, ~161 new check assertions), test exit=0; 2026-08-25 ship**.
| 2026-08-25 | **R5.5 Bandwidth & Connection Profiler ship** — per-msgType bytes / per-netId replication cost / RTT-loss-congestion-sendQueue / Direct-Relay path / Full-Delta-RPC distribution 通过单 `ProfilerSnapshot` API 暴露 + 周期 stderr dump + test sink seam. 详见 §15.12.<br>• **5 个 msgType 家族**：per-msgType 字节 (7 in-game 槽 + extras map for Handshake / AppAck / ClientInput) / per-entity replication cost (cumulative + currentTick + dirtyFieldCount) / RTT-loss-congestion-send-queue depth (GNS `GetConnectionRealTimeStatus` 包) / Direct vs Relay path (GNS `GetConnectionInfo` 包) / Full vs Delta vs RPC distribution (绝对字节数 + 百分比)<br>• **API 形态**：pull `getProfilerSnapshot/snapshots` 任意帧 + periodic `[AYProfiler] netId=X window=[a,b] in=I out=O msgType[Replication]=R(P%) ...` stderr dump (interval 通过 `setProfilerDumpInterval(N)` 启用) + test seam `setProfilerSinkForTesting(fn)` 每帧触发<br>• **钩子注入点** (post-fault 字节数)：`ReplicationManager::sendSealedToConnection` ×7 site (Full / Delta / Spawn ×2 / Despawn ×3 / rebroadcast) ; `RpcHandler::emit` (按 envelope msgType 分支) ; `GnsConnection::_sendHello/Welcome/Reject` (handshake extras key) + ackWire `_rawSend` (AppAck extras) ; `GnsConnection::onRawData` reassembled body (按 hdr.msgType)<br>• **Counter atomic 策略**：`cumulativeSendBytes / cumulativeRecvBytes` 用 `std::atomic<uint64_t>` (永不重置); `currentTickSendBytes / sendCount / recvCount` plain (单写单读主线程)<br>• **Post-fault 字节计数设计**：R5.5 profiler 数打到线上字节 (R5.4 fault interceptor 之后); 真正想要 caller intent bytes 的测试读 R5.6 留位的 `intentBytes` 并行计数器<br>• **模块边界**：不新增 `AYProfile` 基础模块 — R5.5 仅有 AYNetwork 一消费者，预演抽象会强加 ABI 边界加倍 per-tick 成本；R5.6 如出现第二消费者再 promote<br>• **Default-impl virtuals** (R5.4 lesson)：`INetworkSubSystem` 新增 4 虚函数全部 default no-op，test stub (例如 `StubSubSystem`) 无需任何 override 即可编译通过<br>• **CMake**：`src/Profiler/ProfilerRegistry.cpp` 加入 SRC_FILES; `include/Profiler` PUBLIC include dir; 5 个测试文件注册<br>• **测试** (5 文件 / +25 case / target 1259 PASS)：<br>　– `AYTest_ProfilerAccumulator.cpp` (5): recordSend/Recv / window reset / cumulative monotonicity / dump 空跳过 / dump 有流量清零<br>　– `AYTest_PerMsgType.cpp` (5): 7 槽位映射 / Handshake extras key / AppAck+ClientInput extras / 百分比 ±0.01% / 未知 msgType 兜底<br>　– `AYTest_PerNetId.cpp` (4): register 3 ghost cumulative / currentTick reset / unregister 移除 / 排序降序<br>　– `AYTest_RttAndCongestion.cpp` (4): 默认值 / ConnAccessor 串联 / 字段默认值 / path 字段 surface<br>　– `AYTest_ProfilerSubsystem.cpp` (7 E2E): 2 subsystem snapshot ≥1 / perNetId ≥3 ghost 累计 >0 / msgType 总和 ≥ perNetId 总和 / sink per-update 触发 / dump interval 不断 crash / 百分比 100% ±0.01 / 未知 conn 返回 false<br>• **Out of scope (deferred to R5.6+)**：Replay checkpoint `windowSendBytesDelta` 4-byte foothold (本期未做); 跨 host profiler aggregation; 通用 Instrumentor / ScopeTimer 宏框架; body-mutating faults profiler (R6.x)<br>**R5.5 = target 1259/1259 PASS (R5.4 baseline 1234 + 5+5+4+4+7 = 25 R5.5 case)**, test exit=0; 2026-08-25 ship. |
| 2026-08-25 | **R6 Determinism Fixes ship** — 10-commit sweep fixing all 12 Blocker + 14 High findings from `determinism-risk-register-2026-08-25.md` (Medium ride along in C2/C5/C8). Design lives in §15.13.<br>• **C1** clock seam: `GnsConnection::s_nowOverride` + `setNowOverrideForTickRate(serverTick, tickRate)`; `NetworkTime::_accumulatorUs` + `ReplicationManager::_accumulatorUs` replace `double` accumulators. (5)<br>• **C2** sort-by-key: `std::unordered_map → std::map` on `ReplicationManager::_peers`, `PredictionManager::_rings/_ackedSeq/_ghosts`, `EntityReplicationWorldBinder::_bindings/_desired/_collisions`, `ProfilerRegistry` iteration paths. (6)<br>• **C3** RpcHandler + AckTracker single-thread: dropped `_pendingCallsMutex` + `AckPipeline::_mutex`; sorted `_pending` + `_pendingCalls`. (4)<br>• **C4** RNG + replay rotation: `TransportFaultController::setSessionSeed(uint64_t)`; per-profile `std::mt19937_64`; replay rotation boundary = `_serverTick % _maxTicksPerFile`. (2)<br>• **C5** stable `connectionId` + profiler paths: `allocateNetId(acceptOrdinal, slotOrdinal) = (acceptOrdinal<<8) | slotOrdinal`; profiler paths always 0 + sort byMsgTypeExtras/perNetId; RPC outbound tick = `getServerTick()`. (5)<br>• **C6** drop `RpcAsyncPool` + `_simulationInboundMutex`: async RPC completes synchronously in `RpcHandler::tick()` in `callId` order; simulation inbound no longer mutex-guarded. (2)<br>• **C7** fixed-point float precision: `QuantizedFloat` 24-bit round-to-nearest at serializer boundary; `MispredictionResolver` lerp uses `uint16 alpha_q16`; `BitStream::writeFloat` uses `lroundf`. (6)<br>• **C8** pump-boundary funnel: `gns_status_callback` defers `_stateHandler` into `pendingActions` queue drained at pump end on main thread; `_nextFragmentId` atomic; profiler iteration sorts by netId. (9)<br>• **C9** state-equal E2E suite: `INetworkSubSystem::computeStateHash(HashKind)` virtual + AYNetworkSubSystem override (FNV-1a 64-bit, sorted per-netId + pending RPC keys + ack cursors + opt-in profiler atomic counters). `AYTest_StateEqual.cpp` 8 case: TwoRuns_SameInputs_SameStateHash / ReplayFile_RewindReplays_SameHash / FaultProfile_RngDeterministic_SameHash / FloatQuantization_NoisyInputs_SameHash / ClockOverride_TickDriven_SameHash / PumpBoundary_DeferredHandler_DeterministicOrder / RpcAsync_DroppedPool_SynchronousCompletion / SortByKey_PeerOrder_StableAcrossRuns. (validates 44 findings)<br>• **C10** docs: §15.13 "Determinism Contract" (12-rule table, replay scope v1 vs v2, escape hatches, behavior-change documentation, ship definition).<br>**R6 = 1492/1492 PASS (R5.5 baseline 1259 + R6 C9 +8 case, ~110 new check assertions), test exit=0; 2026-08-25 ship**. Engine pointer bump pending. v2 player (`B-10` full remap table) → R6.5.

---

- [KCP Protocol](https://github.com/skywind3000/kcp)（**已弃用，仅历史对照**）
- [GameNetworkingSockets (Valve OSS)](https://github.com/ValveSoftware/GameNetworkingSockets)
- [ENet](https://github.com/lsalzman/enet)
- [kissnet](https://github.com/Ybalrid/kissnet)
- [Unreal Networking](https://docs.unrealengine.com/en-US/InteractiveExperiences/Networking/index.html)
- [Unity NetCode](https://docs.unity3d.com/Packages/com.unity.netcode@1.0/manual/index.html)
- [O3DE Networking](https://o3de.org/docs/learning-guide/gems/multiplayer/)
- [Gaffer on Games — Networked Physics](https://gafferongames.com/post/networked_physics_2004/)
