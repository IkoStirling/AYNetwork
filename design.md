# AYNetwork Design

## 1. 概述

AYNetwork 是 AY Engine 的**网络同步模块**，负责游戏对象的网络复制和远程通信。

### 1.1 设计目标

- **可靠性**：KCP 协议，支持可靠/不可靠/有序模式
- **分层设计**：Transport → Protocol → Replication
- **元数据驱动**：基于 AYReflect 自动序列化，零手动实现
- **ECS 友好**：数据组件与功能组件分离，支持未来迁移 Pure ECS

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

### 2.1 目标特性矩阵

| 特性 | AYNetwork | Unreal | Unity | O3DE | 优先级 |
|------|:---------:|:------:|:------:|:----:|:------:|
| UDP 传输 | ✅ | ✅ | ✅ | ✅ | P0 |
| KCP 可靠层 | ✅ | ❌ | ❌ | ❌ | P0 |
| 状态同步 | ✅ | ✅ | ✅ | ✅ | P0 |
| RPC 调用 | 规划 | ✅ | ✅ | ✅ | P1 |
| 增量同步 | 规划 | ✅ | ⚠️ | ✅ | P2 |
| Interest Management | 规划 | ✅ | ⚠️ | ✅ | P3 |

### 2.2 核心差异

| 引擎 | 同步粒度 | 标记方式 |
|------|----------|----------|
| Unreal | Actor 级别 | UPROPERTY 宏 |
| Unity | GameObject/Component | NetworkVariable |
| O3DE | NetworkHierarchyComponent | Az::Reflect |
| **AYNetwork** | **DataComponent 级别** | **AYTYPE_FIELDS** |

---

## 3. 分层架构

### 3.1 五层架构

```
┌─────────────────────────────────────────────────────────────────┐
│                      Game Code / Gameplay                        │
│         (Entity, Component, DataComponent, System)              │
├─────────────────────────────────────────────────────────────────┤
│                    Replication / RPC Layer                        │
│    - NetComponent (功能组件)                                     │
│    - NetDataComponent (数据组件，标记 NetReplicate)              │
│    - ReplicationSystem (基于元数据自动同步)                      │
├─────────────────────────────────────────────────────────────────┤
│                       Protocol Layer                            │
│    - PacketHeader (序列号, 通道, 分片)                          │
│    - PacketAssembler (组包/拆包)                                │
│    - SequenceNumber (序号管理)                                 │
├─────────────────────────────────────────────────────────────────┤
│                      Transport Layer                             │
│    - KCP (可靠/有序/快速)                                      │
│    - UDP Socket (平台抽象)                                     │
│    - Connection (连接状态机)                                   │
├─────────────────────────────────────────────────────────────────┤
│                       Platform Layer                            │
│    - ThreadPool (异步 I/O)                                     │
│    - Mutex / Atomic (线程安全)                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 各层职责

| 层级 | 职责 | 关键类 |
|------|------|--------|
| **Transport** | 字节收发、KCP 封装 | `KcpConnection`, `UdpSocket` |
| **Protocol** | 组包/拆包、序列号 | `PacketHeader`, `PacketAssembler` |
| **Replication** | 对象同步、变更检测 | `ReplicationManager`, `ReplicationSystem` |
| **RPC** | 远程函数调用 | `RpcHandler` (规划) |
| **Interest** | 距离裁剪、相关性管理 | `RelevanceManager` (规划) |

---

## 4. 传输层设计 (Transport)

### 4.1 KCP 集成

KCP 是一个快速可靠协议，比 TCP 快 30%。

```cpp
namespace ayt::net
{

// KCP 模式
enum class KcpMode : uint8_t {
    ReliableOrdered     = 0,  // 可靠 + 有序 (像 TCP)
    Unreliable        = 1,  // 不可靠 (像 UDP)
    Ordered           = 2,  // 有序但不可靠
};

// KCP 配置
struct KcpConfig {
    uint32_t conv = 0;           // 连接 ID
    uint32_t mtu = 1400;        // 最大传输单元
    uint32_t wndSize = 64;       // 窗口大小
    uint32_t noDelay = 1;       // 0/1: 正常/快速模式
    uint32_t interval = 10;      // 更新间隔 (ms)
    uint32_t resend = 2;         // 快速重传
    uint32_t nc = 1;             // 拥塞控制开关
};

class KcpConnection {
public:
    void init(const KcpConfig& config);
    int send(const uint8_t* data, size_t len);
    int receive(uint8_t* buf, size_t len);
    void update(uint32_t currentTime);
    bool isConnected() const;
    int getPing() const;

private:
    ikcpcb* _kcp = nullptr;
    int _sockfd = -1;
    KcpConfig _config;
};

} // namespace ayt::net
```

### 4.2 连接状态机

```
                    ┌─────────────┐
                    │ Disconnected│
                    └──────┬──────┘
                           │ connect()
                           ▼
                    ┌─────────────┐
         ┌────────►│  Connecting │
         │         └──────┬──────┘
         │                │ onConnectAck()
         │                ▼
         │         ┌─────────────┐
         │         │ Connected  │
         │         └──────┬──────┘
disconnect()             │ disconnect()
         │         ┌──────▼──────┐
         │         │ Disconnecting│
         │         └──────┬──────┘
         │                │
         └────────────────┘
```

### 4.3 通道设计

| 通道 | 用途 | 可靠性 | 示例 |
|------|------|--------|------|
| CHANNEL_RELIABLE | 重要数据 | KCP 可靠有序 | 技能释放、背包操作 |
| CHANNEL_UNRELIABLE | 实时数据 | UDP 不可靠 | 移动同步、动画 |
| CHANNEL_FRAGMENTED | 大数据包 | KCP + 分片 | 资源加载 |

---

## 5. 协议层设计 (Protocol)

### 5.1 包头结构

```cpp
#pragma pack(push, 1)
// PacketHeader: 12 bytes
struct PacketHeader {
    uint32_t packetId;     // 包序号 (递增)
    uint16_t length;       // 包长度 (不含 header)
    uint8_t  channel;     // 通道 0-3
    uint8_t  flags;        // 标志位 (分片、压缩等)
    uint32_t timestamp;    // 时间戳 (ms)
    uint32_t checksum;     // 校验和 (可选)
};
#pragma pack(pop)
```

### 5.2 分片机制

对于大于 MTU 的数据，自动分片：

```cpp
struct FragmentHeader {
    uint32_t fragmentId;    // 分片组 ID
    uint16_t fragmentIndex;// 当前分片索引
    uint16_t fragmentCount;// 总分片数
};
```

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
│   │   ├── KcpConnection.h  # KCP 连接
│   │   ├── UdpSocket.h      # UDP Socket 封装
│   │   └── Connection.h      # 连接状态机
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

## 9. 依赖关系

```
AYNetwork
├── AYReflect    (元数据)
├── AYSerializer (序列化工具)
├── AYEntity     (Component 引用)
├── AYPlatform   (线程、Socket 抽象)
└── KCP         (第三方库)
```

---

## 10. Phase 定义

### Phase 1：传输层

**目标**：KCP + UDP 基础通信

**交付物**：
- [x] KcpConnection 实现
- [x] UdpSocket 封装
- [x] Connection 状态机
- [x] 单元测试

### Phase 2：协议层

**目标**：包头、分片、序列号

**交付物**：
- [x] PacketHeader
- [x] PacketAssembler
- [x] SequenceNumber 管理

### Phase 3：复制层

**目标**：基于 AYReflect 的自动同步

**交付物**：
- [x] ReplicationManager
- [x] ReplicationSystem
- [x] NetDataComponent 标记宏
- [x] Entity 注册/注销

### Phase 4：RPC

**目标**：远程函数调用

### Phase 5：Interest Management

**目标**：距离裁剪、相关性管理

---

## 11. 与 AYEntity 集成

### 11.1 组件注册

```cpp
// Entity 持有 Component，Component 持有 DataComponent
Entity* entity = World::instance().createEntity();
auto* health = entity->addComponent<HealthComponent>();
health->setData(new HealthData());

// ReplicationManager 注册 Entity
network->getReplicationManager()->registerEntity(entity, netId);
```

### 11.2 同步流程

```
ReplicationManager.replicate()
    ↓
    for each registered Entity:
        ↓
        for each Component:
            ↓
            if has DataComponent:
                ↓
                serializeData(DataComponent) → BitStream
                ↓
                send(CHANNEL, BitStream)
```

---

## 12. 参考

- [KCP Protocol](https://github.com/skywind3000/kcp)
- [Unreal Networking](https://docs.unrealengine.com/en-US/InteractiveExperiences/Networking/index.html)
- [Unity NetCode](https://docs.unity3d.com/Packages/com.unity.netcode@1.0/manual/index.html)
- [O3DE Networking](https://o3de.org/docs/learning-guide/gems/multiplayer/)