# GoTable Distributed V2 技术架构

## 1. 目标与边界

本架构将现有单节点 GoTable 演进为多机、分片、多副本数据库。业务模型仍保留 DB、Table、RowKey、普通列空间和 Z 列空间；数据以 RowKey 计算 Slot，同一 RowKey 的列和 Z 双索引必须落在同一 Slot。

V2 不兼容 V1 协议。首期范围是单 IDC/同 Region、多故障域、RF=3、单 Shard 强一致、Replica 在线迁移、故障恢复、全量备份和可恢复的 Shard Split。

## 2. 组件与数据流

```text
Client SDK
  │  V2 长连接、多路复用、RequestID、Deadline
  ▼
Proxy 集群
  │  本地 Route Cache；按 Slot/Shard fan-out
  ├───────────────┐
  ▼               ▼
Data Server     Control Plane
  │               │
  ├─ Shard Raft   ├─ Metadata Raft
  ├─ LogDB        ├─ RouteVersion
  └─ StateDB      └─ Scheduler
```

正常数据请求不访问 Control Plane。Proxy 从 Control Plane Watch 路由；Data Server 通过本地状态和 Raft 判断请求是否合法。

## 3. Shard 与路由

### 3.1 Slot

固定保留 8192 个 Slot。当前 Key 编码已经把 Slot 放在物理 Key 前缀，V2 保留这一排序特性。权威映射必须是控制面维护的 `Slot → Shard`，不能长期依赖 `shardID = slotID / 32`，因为 Split 后边界可以不均匀。

### 3.2 初始与动态规模

初始 Shard 数默认为 128，可选择 64/128/256/512。推荐依据是：

```text
Shard 数 ≥ 规划总 QPS / 单 Shard 目标 QPS
Shard 数 ≥ 规划数据量 / 单 Shard 目标容量
Shard 数 ≥ 4～8 × 规划 Data Server 数
```

运行时先做 Replica 搬迁；当数据量、QPS、Snapshot 恢复时间或 Leader 均衡粒度超过阈值，再执行 Split。Shard 数通过逐个 Split 从 128 增长到 256/512，不修改配置瞬时重分片。

## 4. Proxy

Proxy 维护：

```text
RouteVersion
Slot → Shard
Shard → Replica/Leader Hint
```

Proxy 通过 Watch 接收增量，断线后按版本断点续传，断点无效时重新拉取完整路由。Leader Hint 仅用于优化；真正的 Leader 身份由 Raft Term 决定。

请求流程：

```text
接收请求
→ 校验 Deadline/认证
→ 用本地路由按 Slot 分组
→ 并行发送内部 RPC
→ 处理 MOVED/NOT_LEADER/ROUTE_STALE
→ 仅对明确允许的 RequestID 重试
→ 按原始顺序聚合响应
```

Proxy 无状态，不保存不能重建的业务状态。控制面不可用时，已缓存路由继续服务；路由变更和调度暂停。

## 5. V2 请求与一致性

V2 写请求包含：

```text
ClientID
RequestID
RequestHash
Deadline
ExpectedRevision
Consistency
```

内部 Proxy→Data Server RPC 另外携带 `RouteVersion` 和 `ShardEpoch`。客户端不直接理解 Shard、Raft Term 或 InstallID。

### 5.1 Revision/CAS

每个 Shard 维护单调递增的 64 位 Revision Clock。成功改变记录的操作生成新 Revision；CAS 失败和无效请求不推进版本。`ExpectedRevision=0` 表示 Create-if-absent；非零值要求当前记录 Revision 相等。

Revision 不直接使用 Raft Index，因为 Split 后 Child 会拥有独立 Raft Index。

### 5.2 幂等

Dedup 键为 `(ClientID, RequestID)`，值包含 RequestHash、原始结果、Revision、AppliedIndex 和过期时间。重复且 Hash 相同返回首次结果；Hash 不同返回冲突。默认 Deadline 30 秒，最大 3 分钟，Dedup 保留到 Deadline 后加安全余量。

Dedup 与业务数据、Revision、`lastApplied` 在同一个 StateDB WriteBatch 中写入，并进入 Snapshot 和 Split 数据。

### 5.3 批量与读取

- 同一 Slot 的原子批量写：一个 Raft 命令、一次状态机 Apply、全有全无。
- 跨 Slot 批量写：Proxy 拆分，返回 `success/failed/unknown`；不做补偿事务。
- 同 Shard 读取：一次 ReadIndex 加 RocksDB Snapshot。
- 跨 Shard MGET：每个 Key 线性一致，但不保证全局同一时间点。
- SCAN/ZSCAN：普通分页允许页间漂移；Snapshot Scan/Export 使用固定 Snapshot 边界。

## 6. Raft 与持久化

每个 Shard 是独立 Raft Group，默认三副本，副本分布在不同故障域。Data Server 节点可承载多个 Group。

```text
LogDB
  Raft Entry / Term / Index / HardState / Snapshot Metadata

StateDB
  业务 KV / Z 双索引 / Revision / Dedup / lastApplied
```

建议每个 Data Server 一个 LogDB RocksDB 和一个 StateDB RocksDB。StateDB 使用每 Shard Replica 一个 CF；LogDB 可按 Group 使用 CF 或 Prefix。

写入确认顺序：

```text
Raft Log 多数副本 durable
→ Raft Commit
→ Leader StateDB Apply 成功
→ 业务数据、Revision、Dedup、lastApplied 同批落盘
→ 返回成功
```

StateDB Apply 失败不得跳过 Entry。LogDB 与 StateDB 没有跨库事务，依赖上述顺序、持久化 `lastApplied` 和恢复对账。

## 7. Snapshot 与副本恢复

### 7.1 Snapshot Manifest

Manifest 至少包含：

```text
ClusterID / ShardID / ShardGeneration / SnapshotID
lastIncludedIndex / lastIncludedTerm / ConfState
state_schema_version / key_encoding_version / comparator
files: name / size / checksum / key range
manifest checksum
```

Snapshot 业务状态必须等于 Apply 完 Raft Index N 的状态，且状态内 `lastApplied=N`。

### 7.2 B+ 快照路径

发送端对 Shard 获取 ApplyGate，确定 N 后执行 CF Export；导出 CF 必须包含业务数据、Revision、Dedup 和 `lastApplied=N`。接收端把文件 Import 到唯一新 CF，校验后作为 hidden state，持久化 Raft Snapshot Metadata，再切换 Active CF Mapping，最后从 N+1 追日志。

Install Journal 处理 LogDB 与 StateDB 之间的崩溃窗口。Active 切换前失败只清理 staging；切换后只允许继续追日志和清理旧 CF。

### 7.3 C 备选路径

共享 CF + Prefix/Generation 可以降低 CF 数量，但没有任意 Prefix 零扫描导出。流程是：

```text
确定 Index N
→ 创建 RocksDB logical Snapshot
→ 顺序扫描 Shard 范围
→ SstFileWriter 重建目标 Generation SST
→ Ingest 到隐藏 Generation
→ 原子切 ActiveGeneration
→ 追 N+1 日志
```

C 用迁移时扫描换取稳态资源密度，只在 B 的 CF 密度或尾延迟压测不达标时采用。

## 8. Replica 搬迁

```text
Add Learner
→ 生成 Snapshot(N)
→ 保留 N+1 之后日志
→ Bulk 通道传输并校验
→ 安装 Snapshot
→ 追日志到 Leader Commit
→ Promote Voter
→ Remove 旧 Voter
```

Control Plane 的 Desired Membership 不是最终事实；实际成员配置以 Shard Raft 已提交的 ConfState 为准。一次只提交一个成员变更。Snapshot 传输过慢时，超过日志保留预算则取消旧任务并从更新 Index 重新制作。

## 9. Shard Split

Replica 搬迁不改变 Shard 边界；Split 创建 Child Shard 和独立 Raft Group。

```text
PLANNED
→ CHILD_BOOTSTRAPPING
→ SNAPSHOT_COPYING
→ CATCHING_UP
→ FENCED
→ CUTOVER
→ CLEANING
→ SUCCEEDED
```

Parent 在 Fence Index B 后停止迁移范围的新写，Child 追平到 B，控制面再原子发布新的 Slot 路由。Cutover 前允许回滚；Cutover 后不回切路由，只继续修复和清理。Parent 的旧范围通过 Raft 驱动 `FinalizeSplit`，再执行 DeleteRange、Flush 和限速 Compaction。

## 10. 故障处理

- LogDB Append/fsync 失败：停止该节点的 Raft 确认能力。
- StateDB Apply 失败：Replica 进入隔离，不能跳过日志。
- RocksDB DB 级错误：按整个数据目录隔离，避免共享 StateDB 继续提供不可信状态。
- 磁盘水位过高：停止迁入、转移 Leader、暂停非必要 Bulk 任务。
- 多数派丢失：停止写入；Unsafe Recover 只允许人工执行并永久 fencing 旧副本。
- 节点下线：进入 DRAINING，转移 Leader，等待 in-flight Apply 后关闭。

## 11. 调度、备份与升级

调度器按“故障修复 > Replica 搬迁 > Split > 备份”排序，限制 Snapshot 并发、读写带宽、Bulk 网络、临时空间和 Apply 暂停时间。任务以 OperationID 持久化，支持暂停、恢复、取消和重试。

V2 首期集群备份由各 Shard 独立一致 Snapshot 组成，记录每个 Shard 的 Index/Term/ConfState；不承诺跨 Shard 全局同一时间点，PITR 后置。

滚动升级顺序为：Control Plane → 非 Leader Data Server → Leader 转移 → 原 Leader → Proxy。新 StateDB/Snapshot 格式使用 Feature Gate；启用新格式后采用前滚优先。

## 12. 验收指标

硬指标：

```text
已确认写单节点故障 RPO=0
Leader 故障恢复 P99 ≤ 5 秒
Proxy 额外 P99 延迟 ≤ 1ms
无多数派不产生新的 Commit
Snapshot/Apply 不重复、不跳过
Learner 未追平不得 Promote
```

容量和性能指标通过 256/512/1024 Shard 密度、Snapshot、恢复和故障注入压测冻结。

