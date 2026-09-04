# GoTable Distributed V2 产品设计

## Problem Statement

当前 GoTable 是单节点 RocksDB 数据库，已有宽表、Z 列空间、CAS、批量操作、SCAN/DUMP 和异步主从复制能力，但不具备多机分片、自动故障转移、在线扩缩容和副本级恢复能力。

业务希望客户端只连接 Proxy，不感知 Shard、Replica、Raft Leader 和节点变更，同时获得低延迟、高可用和在线扩展能力。

## Solution

GoTable V2 采用三层架构：

```text
Client SDK → Proxy → Data Server
                         ↘ Shard Raft + RocksDB
Control Plane ───────────→ 拓扑、调度和路由
```

- Proxy 无状态，维护本地路由缓存并通过长连接多路复用请求。
- Control Plane 管理节点、路由和调度，不进入正常数据请求链路。
- Data Server 承载多个 Shard Replica，每个 Shard 使用独立 Raft Group 和三副本。
- 默认初始 128 Shard，可配置 64/128/256/512；机器扩容先迁移 Replica，容量或热点达到阈值后在线 Split。
- V2 不兼容 V1 协议，使用新的 ClientID、RequestID、64 位 Revision 和明确错误语义。

## User Stories

1. 作为业务开发者，我希望只配置多个 Proxy 地址，就能访问整个集群，而不需要感知 Shard 和节点地址。
2. 作为业务开发者，我希望 Proxy 故障或 Leader 切换时，带稳定 RequestID 的写请求可以安全重试。
3. 作为业务开发者，我希望同一 Slot 内的批量写具备全有全无语义。
4. 作为业务开发者，我希望跨 Slot 批量请求能返回每个子请求的成功、失败或未知状态。
5. 作为业务开发者，我希望 CAS 使用持久化 64 位 Revision，避免 Leader 切换后失效。
6. 作为业务开发者，我希望 INCR 在网络超时、Proxy 重试和 Leader 切换下不会重复执行。
7. 作为业务开发者，我希望强一致读写在单节点故障下继续工作，并保证已确认写入不丢失。
8. 作为业务开发者，我希望明确区分线性一致读、Stale Read 和结果未知，而不是得到模糊响应。
9. 作为运维人员，我希望增加机器时可以在线迁移 Replica，不中断业务。
10. 作为运维人员，我希望 Shard 过大或过热时可以在线拆分，不要求全局停写。
11. 作为运维人员，我希望扩缩容、Snapshot 和备份任务可限速、暂停、恢复、取消和重试。
12. 作为运维人员，我希望节点下线前能够自动摘流、转移 Leader 并安全关闭。
13. 作为运维人员，我希望副本损坏时可以从健康副本重建，而不使用不确定的本地修复。
14. 作为运维人员，我希望控制面短暂不可用时，已有数据路由和健康 Shard 仍能继续服务。
15. 作为运维人员，我希望能够执行集群级全量备份，并验证恢复结果。
16. 作为运维人员，我希望备份和恢复失败不会影响当前 Active Replica。
17. 作为平台管理员，我希望控制面能够感知故障域，并避免同一 Shard 的副本落在同一故障域。
18. 作为平台管理员，我希望滚动升级期间新旧相邻 V2 小版本能够安全协作。
19. 作为平台管理员，我希望失去多数派时系统默认停写，避免脑裂和隐式数据丢失。
20. 作为审计人员，我希望危险恢复、路由切换和扩缩容操作都有持久化 OperationID 和审计记录。

## Implementation Decisions

- **分片模型**：固定 8192 个 Slot，权威路由为 `Slot → Shard`；初始 Shard 数默认 128，可配置为 64/128/256/512。
- **扩展模型**：Replica 搬迁和 Shard Split 是两类独立操作。Replica 搬迁不改变数据边界；Split 创建新的 Shard ID 和 Raft Group。
- **一致性**：每个 Shard 默认 RF=3，采用 CP 模型。失去多数派时停止写入；线性一致读也必须具备多数派确认。
- **批量语义**：原子边界固定为同一 Slot；跨 Slot 请求返回逐项结果，不提供跨 Shard 事务。
- **协议**：V2 使用稳定 ClientID、RequestID、RequestHash、Deadline 和 64 位 Revision；不承诺 V1 兼容。
- **幂等**：默认逻辑 Deadline 30 秒，最大 3 分钟；Dedup 保留到 Deadline 后加安全余量，重复请求返回首次结果，RequestID 冲突返回错误。
- **存储**：LogDB 与 StateDB 分离。StateDB 采用每节点一个 RocksDB、每个 Shard Replica 一个带 Incarnation 的 Column Family；LogDB 保存 Raft Log 和 HardState。
- **Snapshot**：Replica 补齐优先使用 CF Export/Import 文件级快照；共享 Prefix 扫描仅作为高密度备选。Snapshot 必须绑定 Raft Index、Term、ConfState 和校验信息。
- **调度**：故障修复优先于搬迁、Split 和备份；所有 Bulk 任务受节点、磁盘、网络、并发和临时空间预算约束。
- **控制面故障**：控制面不可用时，已有路由和健康 Shard 继续服务；拓扑变更和自动调度暂停。
- **恢复**：Apply 失败不跳过日志；损坏副本隔离并从健康副本以新 InstallID 重建。Unsafe Recover 仅允许人工显式执行。
- **升级**：只保证相邻 V2 小版本滚动升级；新格式通过 Feature Gate 开启，启用后采用前滚优先。

## Testing Decisions

- 测试以外部行为和故障结果为主，不依赖具体 RocksDB 文件编号或线程实现。
- 验证单 Slot 原子批量、跨 Slot 部分结果、CAS/Revision 和 RequestID 去重。
- 验证节点宕机、Leader 切换、Proxy 断连、控制面不可用和多数派丢失。
- 验证 Replica 搬迁、Snapshot 安装、Split Fence、路由切换和旧数据清理。
- 对 B+ 与共享 Prefix 方案进行 256/512/1024 Shard 密度压测，比较 RSS、打开时间、前台 P99/P999、Snapshot 吞吐和恢复时间。
- 对 Snapshot/Import/Apply/清理任意阶段执行 kill -9、磁盘满、校验失败和日志缺口测试。
- 使用线性一致性历史测试、故障注入和恢复演练验证 RPO=0、无重复 Apply 和无脑裂。

## Out of Scope

- V1 协议兼容和旧 SDK 透明接入。
- 跨 Shard ACID 事务和全局一致性快照。
- 跨 Region 强一致复制。
- 无限期 exactly-once；幂等保证受 3 分钟逻辑 Deadline 和 Dedup 窗口约束。
- 单个超热 RowKey 的自动拆分。
- V2 首期 PITR。
- 自动 Unsafe Recover。

## Further Notes

V2 首期建议优先实现可靠的 Replica 搬迁、B+ 文件级 Snapshot、故障恢复和全量备份；在线 Shard Split 需要在元数据和任务模型中预留，并通过独立原型验证后逐步启用。

