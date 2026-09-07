# KCC Ray 训练编排、故障恢复与实机排障技术总结

> 本文只总结本轮对话中实际设计、修改、检查和实机验证过的工作。
>
> 重点范围是 `kcc_ray`、RayCluster、Ray Jobs、Kubernetes Supervisor Job、checkpoint 恢复、备用机替换，以及 `qw3-*` 和 A3 实机测试。本文不扩展讨论仓库中其他后续或并行架构。

## 1. 工作背景与目标

最初的问题是：执行 `kcc_ray start` 后，脚本到底会经历什么；`start` 和 `recover` 是否有必要分成两个命令；训练中断后 checkpoint 会怎样；节点故障后能否自动切换到备用机。

随着实机测试推进，目标逐步收敛为：

1. 用一个统一的 `start` 命令启动新训练或从已提交 checkpoint 恢复；
2. 支持显式重新训练，也支持选择全部节点参与训练；
3. 将正式训练从脆弱的 `kubectl exec` 迁移到 Ray Jobs；
4. 将恢复 Supervisor 放进 Kubernetes Job；在 Job 重试预算内且固定管理节点及其 hostPath 可用时，终端断开或 Supervisor 进程退出后仍能续接；
5. 支持立即停止和“下一个 checkpoint 提交后停止”；
6. 节点故障时先安全诊断，再决定原拓扑重试或替换备用机；
7. 让控制状态有界并以原子方式落盘，同时冻结训练模板；checkpoint 与日志的容量和介质可靠性仍由训练与存储侧治理；
8. 保持实现轻量，复用现有训练脚本、KubeRay、Ray Jobs、MindSpeed/Megatron 和 ClusterD，不重新实现训练框架。

最终形成的核心原则是：

> KCC 不负责重新实现训练或 checkpoint，而是负责组织、校验、停止和重建一次完整的分布式训练世界。

## 2. 整体架构的演进

### 2.1 最初的执行方式

早期流程主要由本地 CLI 和 `kubectl exec` 串联：

```text
用户终端
  -> kcc_ray
  -> 创建 RayCluster
  -> kubectl exec 进入 Ray Head
  -> 启动训练 Driver
  -> Driver 在各 Worker 上拉起训练进程
```

这种方式能够跑起来，但控制链依赖发起命令的进程和 `exec` 连接。终端、网络或 Head 上的会话发生问题时，很难区分“训练失败”和“观察连接失败”。

### 2.2 正式训练改为 Ray Jobs

后续将正式训练提交改为 Ray Jobs：

```text
kcc_ray
  -> Ray Jobs API
  -> Ray Job Driver
  -> 每个 Worker 上的 Ray Actor
  -> torchrun / MindSpeed / Megatron
```

Ray Jobs 的直接收益是：

- Driver 生命周期不再绑定本地终端；
- 外部 `kubectl exec` 断开不会直接杀死正式训练；
- 可以使用确定性的 submission ID 查询同一训练；
- Supervisor 重启后可以重新连接已经提交的 Ray Job，而不是盲目重复提交。

Ray Jobs 不能独立解决以下问题：

- Ray Head 或整个 RayCluster 消失；
- Driver 本身崩溃；
- checkpoint 是否一致、是否可恢复；
- 旧 Ray world 的安全清理；
- 节点故障诊断和备用机替换；
- 多次 attempt 的持久化管理。

因此，Ray Jobs 解决的是 Driver 与客户端解耦，而不是完整的训练容灾。

### 2.3 Supervisor 放进 Kubernetes Job

为了让恢复控制逻辑也不依赖 SSH 会话，外层 Supervisor 被放进 Kubernetes Job：

```text
kcc_ray start（默认恢复模式）
  -> 创建 Kubernetes Supervisor Job
  -> recovery_supervisor.py
  -> attempt a00
  -> 六阶段启动流程
  -> Ray Jobs 正式训练
  -> 失败诊断
  -> 清理旧 Ray world
  -> 同拓扑重试或备用机替换
  -> attempt a01 / a02 ...
```

Supervisor Job 的意义不是替代 Ray Job，而是管理 Ray Job 之上的生命周期：

- Supervisor Pod 可在 Kubernetes Job 的 `backoffLimit` 内重建；
- 管理节点及其 hostPath 状态目录仍可用时，重建后读取持久状态并续接，而不是重新创建一个逻辑任务；
- 管理 attempt、清理、诊断、恢复和停止；
- 在用户终端退出后继续运行。

这不是跨管理节点高可用：Supervisor Pod 被固定到配置的管理节点，源码和状态依赖该节点的 hostPath。该节点或磁盘不可用时，Job 不能迁移到其他节点继续读取同一份状态。

## 3. 命令语义的收敛

### 3.1 合并 `start` 和 `recover`

最初存在“重新训练”和“恢复训练”两个概念。讨论后认为，没有必要让用户在正常启动前先判断是否存在 checkpoint，因此统一为：

```text
kcc_ray start
```

默认行为是：

1. 检查配置的 checkpoint 路径；
2. 如果存在所有 Worker 一致认可的已提交 checkpoint，则恢复；
3. 如果不存在合格的可恢复 checkpoint，则安全失败；
4. 如果 checkpoint 存在但证据不一致，则拒绝猜测并进入人工处理。

新训练必须显式使用 `--fresh`。它表达“忽略恢复入口，按新训练语义启动”，而不是隐式删除历史 checkpoint。

需要明确一个当前 legacy 实现边界：无特殊模式的默认 `start` 会创建 Kubernetes Supervisor Job；`--fresh` 和 `--all-nodes` 目前仍进入单次前台六阶段流程，因此不具备 Supervisor Pod 重建和跨 attempt 自动恢复能力。这是当前代码路径的差异，不应把两种模式的容灾能力描述成完全相同。

### 3.2 节点选择

`start` 支持：

- 使用配置文件中的默认 active 节点；
- 显式传入 `--node`；
- 显式传入 `--spare-node`；
- 使用 `--all-nodes` 让所有候选节点参与，而不是保留备用节点。

显式 `--node` 或 `--all-nodes` 时，入口会在内部处理拓扑变更许可，不再要求用户额外理解或手动输入 `--allow-topology-change`。

本轮后期整理出的默认拓扑是：

```text
active workers:
  gpu-server-00
  gpu-server-01
  gpu-server-02
  gpu-server-03
  gpu-server-05
  gpu-server-06

spares:
  gpu-server-07
  gpu-server-08
```

这些默认值集中到 `config/cluster.yaml`，避免继续散落在多个 Python、Shell 和 YAML 文件中。运行时仍可以用命令行覆盖。

### 3.3 运行标识

一个逻辑训练使用稳定的 `run-id`，每次重建使用新的 attempt：

```text
qw3-10
  -> qw3-10-a00
  -> qw3-10-a01
  -> qw3-10-a02
```

这样可以同时满足：

- 用户始终用同一个逻辑 ID 查询和停止任务；
- 每个 RayCluster、Ray Job 和日志目录仍有唯一身份；
- 恢复不是修改旧 world，而是创建一个新的完整 world；
- 历史 attempt 可以追踪，旧结果不能污染新 attempt。

### 3.4 停止命令

最终保留两种停止语义：

```text
kcc_ray stop --run-id RUN_ID
kcc_ray stop-after-checkpoint --run-id RUN_ID
```

二者不能合并，因为语义本质不同：

- `stop`：尽快停止当前训练；不主动删除已经提交的 checkpoint；
- `stop-after-checkpoint`：记录当前 checkpoint 基线，等待一个更大的、所有 Worker 一致认可的新 checkpoint 提交后再停止。

终端中的 `Ctrl-C` 只应该停止前台观察器，不能被当作可靠的分布式训练停止协议。

### 3.5 去除人工 checkpoint 独占确认

早期命令要求：

```text
--confirm-checkpoint-exclusive
```

这个参数只是人工确认，并不是真正的分布式锁，无法从技术上阻止两个训练同时写同一路径。最终将其从正常使用流程中去掉。

仍然存在的安全前提是：同一个 checkpoint 根目录不能被多个逻辑训练并发写入。这个约束应该由配置、目录规划或后续真正的所有权机制保证，而不是依赖一个形式化确认参数。

## 4. 单次 attempt 的六阶段流程

每个 attempt 都重新执行完整的启动链路，而不是只补一个 Worker。

### 阶段 1：环境与节点检查

检查内容包括：

- 目标节点是否 Ready；
- NPU 数量和基本健康状态；
- 是否存在明显占用；
- 必要命令、Python 包和配置是否可用；
- active 和 spare 是否重复；
- 请求的节点数和训练拓扑是否一致。

`kcc_ray check` 后来改为默认检查所有配置节点，减少用户遗漏某台备用机的概率。

这里遇到过一个典型的环境差异：同样 SSH 到 `server-00`，同事执行 `kcc_ray check` 报：

```text
ModuleNotFoundError: No module named 'yaml'
```

原因不是机器不同，而是命令解析到的 Python/conda/PATH 环境不同。相同宿主机并不代表相同解释器。这个问题推动了依赖说明和环境加载的整理。

### 阶段 2：按本次运行渲染 RayCluster

脚本根据本次 active 节点生成 attempt 专属 RayCluster：

- 一个 Head；
- 每台 active 节点一个 Worker；
- Worker 带明确的节点约束；
- 资源数、环境变量、挂载和网络参数由配置生成；
- RayCluster 使用配置中的稳定资源名；创建新 attempt 前必须先确认旧 RayCluster 已清理。attempt 通过 run-id 标签、状态和独立产物目录区分，而不是依赖 RayCluster 名称。

### 阶段 3：创建并等待 RayCluster Ready

KubeRay 创建 Head/Worker Pod，脚本持续观察：

- Pod 是否已经生成；
- 调度到了哪台节点；
- `Pending`、`Running` 和 Ready 状态；
- RayCluster 的期望 Worker 数是否满足；
- 是否出现调度、设备或 gang 相关错误。

这里一个重要经验是：

> Head Ready 不代表 RayCluster 可以训练；只要一个 Worker 因设备或节点问题 Pending，整个分布式 world 就不完整。

在 gang 调度语义下，一个 Worker 的问题也可能阻塞整组资源，因此不能只看单个 Pod。

### 阶段 4：拓扑发现、RankTable 清洗与真实 HCCL gate

Ray Worker 全部就绪后，脚本读取本次真实拓扑并生成/清洗 RankTable。

本轮修正了 ClusterD RankTable 可能出现的 rank 空洞问题。清洗逻辑会：

- 按实际存在的唯一 rank 重新压紧编号；
- 保留节点、设备和 endpoint 的真实映射；
- 拒绝重复设备或无法唯一解释的拓扑；
- 校验最终 world size；
- 在正式训练前执行真实 HCCL 通信检查。

真实 HCCL gate 的意义是提前证明：

- 所有训练节点能够建立通信；
- RankTable 与当前 Pod 世界一致；
- 每个 rank 能参与 collective；
- 训练不会在加载大量模型后才发现基础拓扑错误。

### 阶段 5：冻结并注入训练模板

工具不重新实现同事已有的训练脚本，而是介入现有实现：

1. 读取默认或用户选择的模板；
2. 为逻辑 run 创建只读语义的快照；
3. 记录模板来源、大小和 SHA256；
4. 在每个 Worker 上生成本次运行副本；
5. 只注入分布式运行必须变化的字段。

典型注入内容包括：

- `MASTER_ADDR`、`MASTER_PORT`；
- `NODE_RANK`；
- `WORLD_SIZE`；
- `RANK_TABLE_FILE`；
- 本次 checkpoint、数据和输出路径；
- 是否恢复及恢复位置。

源训练代码和源模板不会因为启动一次训练而被直接修改。

默认模板整理为：

```text
ray_startup_bundle/training_templates/pretrain_150M.sh
```

同时保留命令行选择其他模板的能力。

### 阶段 6：通过 Ray Jobs 启动正式训练

Supervisor 使用确定性的 submission ID 提交 Ray Job。Driver 再在每个 Worker 上拉起 actor，最终由训练框架执行 `torchrun`/MindSpeed/Megatron。

为了避免 Supervisor 重启后重复训练，设计增加了：

- 确定性的 submission ID；
- create-only 的 Ray Job 提交记录；
- run-id、attempt 和集群归属校验；
- 重启后的 reconnect 路径；
- Ray Jobs 状态查询的退避重试。

状态查询遇到短时网络错误时不会立即把训练判死；重试窗口按本轮要求扩展到约 300 秒。

## 5. Checkpoint 的恢复语义

### 5.1 什么才算“已提交 checkpoint”

训练中可能先创建一个新的 `iter_*` 目录，再逐步写入模型和优化器文件。如果进程中途被杀，最大的目录可能只是半成品。

因此 KCC 不按目录名选择最大的 `iter_*`，而以 Megatron 的 tracker 为提交点：

```text
latest_checkpointed_iteration.txt
  -> iter_%07d
```

只有 tracker 指向的 iteration 才有资格成为恢复候选。

进一步还要验证：

- tracker 内容可解析；
- 对应 iteration 目录存在；
- 必需文件存在且非空；
- 模型和优化器分片可见；
- 所有 active Worker 看到相同 tracker；
- 所有 Worker 得到相同 tracker 内容及 SHA、iteration、目录，以及相同的必需文件路径和大小清单。

当前 legacy 校验不会对每个大型 checkpoint 分片做全文件内容哈希；分片侧主要检查存在、非空、首字节可读，并跨 Worker 比较相对路径和大小。因此它能排除常见的缺失、空文件和可见性分歧，但不能证明文件任意字节都未发生静默损坏。

任何 Worker 不可达或观察结果不一致时，都不能声称该 checkpoint 已经安全提交。

### 5.2 能恢复哪些训练状态

真正的训练状态由 MindSpeed/Megatron 从 checkpoint 加载。一般包括：

- 模型参数；
- 优化器状态；
- 学习率调度器状态；
- 随机数生成器状态；
- 已完成 iteration；
- 框架写入 checkpoint 的其他训练元数据。

KCC 恢复的是外围计算环境：

- 重新创建 RayCluster；
- 重新发现节点和 rank；
- 重新生成 RankTable；
- 重新执行 HCCL gate；
- 重新创建 Driver 和 actors；
- 将已经验证的 checkpoint 位置传给训练脚本。

以下内容不会被原地恢复：

- Python 调用栈；
- 原 Ray Driver 内存；
- 原 Ray actor 状态；
- 原 HCCL communicator；
- 尚未提交的梯度；
- 当前正在处理但未形成 checkpoint 的 batch；
- W&B 的网络会话。

所以恢复点一定是最后一个已提交 checkpoint，而不是故障发生的那一条指令。

### 5.3 中断对 checkpoint 的影响

立即中断时：

- 当前尚未提交的计算进度会丢失；
- 已提交 checkpoint 保持不动；
- 训练框架来不及完成的新 checkpoint 目录可能成为半成品；
- 下次启动仍以 tracker 指向的已提交 checkpoint 为准，因此不会误选半成品目录。

工具不会因为一次普通中断自动删除历史已提交 checkpoint，也不会擅自把 tracker 回退到更旧版本。

## 6. 故障恢复策略

### 6.1 为什么必须重建完整 Ray world

多机 HCCL 训练中，rank、world size、通信 endpoint 和进程组是一个整体。一个 Worker 消失后，不能简单补一个新的 actor 并让它加入旧 communicator。

正确的恢复路径是：

```text
任一 Worker/Driver/Head 失败
  -> 当前 attempt 失败
  -> 停止剩余训练进程
  -> 删除旧 RayCluster 及相关资源
  -> 等待旧 world 完全消失
  -> 诊断节点
  -> 选择原拓扑或替换拓扑
  -> 创建新 attempt
  -> 重新执行六阶段
  -> 从最后已提交 checkpoint 恢复
```

这会牺牲少量重建时间，但能避免新旧 rank、旧 socket 和残留 communicator 混在一起。

### 6.2 同拓扑重试与备用机替换

不是所有错误都应该换机。

以下错误优先同拓扑有限重试：

- Ray Jobs API 暂时不可达；
- Head/Driver 网络抖动；
- W&B 等外部服务异常；
- 训练依赖或环境变量错误；
- checkpoint 查询时 Worker 暂时不可达；
- 无法证明来自具体节点的全局失败。

只有存在稳定的节点本地故障证据时，才进入备用机替换：

- 故障节点身份能够唯一确定；
- 多次采样的证据一致；
- surviving active 节点健康；
- 备用节点健康且空闲；
- 备用机数量足够；
- 替换预算未耗尽。

如果同时坏了 N 台，就必须完整找到 N 台备用机；不能只替换其中一部分后拼凑一个语义不清的拓扑。

### 6.3 为什么采用 fail-closed

恢复逻辑宁可在证据不足时进入 `MANUAL_REQUIRED`，也不能：

- 猜测哪台节点坏了；
- 把 W&B、Python 依赖或数据路径错误当成硬件故障；
- 在旧 Ray Job 状态不确定时提交第二份训练；
- 在 checkpoint 不一致时选择“看起来最大”的目录；
- 删除无法证明归属于当前 run 的资源。

这也是出现下面提示时的真实含义：

```text
STOP: automatic recovery refused: latest committed checkpoint is unavailable
or differs between active workers
```

它不表示 checkpoint 被占用。它表示在当前故障窗口内，Supervisor 无法从所有 active Worker 得到一致的已提交 checkpoint 证据。Worker 已经掉线时，RPC 不可达也可能触发这一结果。

## 7. 停止控制的实现

### 7.1 立即停止

立即停止的轻量安全实现包含：

1. 按 run-id 找到持久状态；
2. 写入 create-only 停止标记；
3. 校验目标 RayCluster/Ray Job 的归属；
4. 停止当前 attempt；
5. 等待或触发受控清理；
6. 保留已提交 checkpoint。

这样可以避免误停同 namespace 中其他人的训练。

### 7.2 checkpoint 后停止

`stop-after-checkpoint` 不按固定秒数猜测保存时间，而是：

1. 读取并记录当前已提交 iteration，作为 baseline；
2. 向当前训练写入“下一个 checkpoint 后停止”的控制请求；
3. 等待 tracker 增长；
4. 在每个 Worker 上读取 `latest_checkpointed_iteration.txt`，确认均可读、iteration 相同且严格大于 baseline；
5. 依据 Megatron tracker 的提交语义停止训练。

当前 legacy `stop-after-checkpoint` 不会进一步扫描对应 `iter_*` 目录，也不会校验 rank shard、非空文件或内容摘要。因此这里证明的是“跨 Worker tracker iteration 一致并前进”，不能表述为完整的 checkpoint 文件校验。

“等待 tracker 从 547000 增长一次”的意思，就是 baseline 为 547000，必须等到 tracker 指向一个严格大于 547000 的已提交 iteration，不能把原有 checkpoint 当作这次停止请求的完成证据。

### 7.3 边界

如果训练之后再也无法产生 checkpoint，`stop-after-checkpoint` 会继续等待。此时应由操作员改用立即停止。自动超时虽然容易实现，但可能在 checkpoint 正写到一半时破坏用户期望，因此本轮没有用粗暴 sleep 代替提交判断。

## 8. 长时间运行加固

### 8.1 `state.json` 的用途

`state.json` 不是模型 checkpoint。它只保存逻辑训练的编排状态，例如：

- run-id；
- 当前 attempt；
- active/spare 节点；
- 当前阶段和状态；
- 已使用的重试/替换次数；
- 最近诊断摘要；
- RayCluster/Ray Job 身份；
- 模板快照信息；
- result 文件路径；
- 停止请求。

Supervisor Pod 重建后依靠这个文件判断应该续接、等待、清理还是恢复。

### 8.2 `qw3-10` 暴露的状态文件问题

执行：

```text
kcc_ray stop --run-id qw3-10
```

曾出现：

```text
recovery state is unexpectedly large
```

原因是早期实现把不断增长的 stdout/stderr tail 或较大的执行结果直接复制进 `state.json`，旧的 64 KiB 上限最终被突破。训练本身未必异常，但控制命令因此无法读取状态。

### 8.3 轻量修复

加固后的设计是：

- `state.json` 只保存摘要和独立 result 文件路径；
- 完整 stdout/stderr 留在 attempt 日志或结果文件中；
- 状态文件上限提高到 1 MiB，但不以无限放大上限掩盖增长；
- attempt 和诊断历史有界；
- 使用同目录临时文件写入；
- 对文件执行 `fsync`；
- 使用原子 replace；
- 再对父目录执行 `fsync`；
- 新状态超限或写入失败时保留旧状态，不把有效状态覆盖成半个 JSON。

因此，`state.json` 不会再因为持续日志追加而线性增长；但它仍受 1 MiB 上限、最多 32 个 attempt 和底层磁盘可用性的约束。达到边界时会安全失败，不等于可以无条件持续任意时长。

### 8.4 模板漂移保护

逻辑 run 第一次启动时会保存：

```text
training-template.sh
training-template.json
```

元数据记录来源路径、大小和 SHA256。后续 attempt 和 Supervisor 续接始终使用该快照。

这样可以防止训练运行数周后，有人修改默认模板，导致恢复后的学习率、模型参数或数据路径悄悄变化。

### 8.5 仍然存在的长期风险

本轮加固能让控制状态长期有界，但不能自动解决所有数据面问题：

- checkpoint 数量和占用空间仍取决于训练脚本的保留策略；
- stdout/stderr 日志仍需要系统级 rotation 或容量监控；
- 共享存储满、损坏或不可达时无法从同一存储恢复；
- Supervisor Job 当前仍依赖配置的管理节点和其持久目录；
- `server-00` 整机或磁盘故障仍可能成为 legacy 控制面的物理单点；
- KubeRay、Kubernetes、ClusterD、device plugin 和 CANN/HCCL 属于外部依赖；
- “运行一个月”需要容量和告警验证，不能仅靠短时功能测试直接证明。

所以当前设计具备月级运行所需的状态边界和续接机制，但还不能把“代码已加固”等同于“已经完成一个月无故障证明”。

## 9. `qw3-*` 实机测试过程

> 说明：`qw3-1` 至 `qw3-5` 的原始测试产物后来按要求清理，下面对它们的描述来自本轮会话中当时的现场输出和诊断结论。`qw3-8`、`qw3-10`、`qw3-jobs-1` 等仍有归档状态或结果文件可供复核。

### 9.1 `qw3-1`

观察到 Ray Pod 长时间 Pending，现场同时关注到 `gpu-server-02` 的设备健康提示，例如 `02000027`、`0200002B`。

能够确认的是：

- 问题发生在正式训练前的集群拉起阶段；
- 没有证据证明已经进入正式 MindSpeed 训练；
- 该 run 后来被立即停止；
- 旧版状态仍可能残留为 `RUNNING`。

因此不能把 `qw3-1` 简化成某个确定的训练代码错误。它主要暴露了 Pending 阶段需要更好的清理、节点诊断和状态收敛。

### 9.2 `qw3-2`

失败发生在阶段 4。ClusterD RankTable 中出现 rank 空洞，典型表现是缺少 16–23。

处理方式：

- 清洗器按真实唯一设备重新压紧 rank；
- 重复设备仍然拒绝；
- 清洗后重新验证 world size；
- 继续保留真实 HCCL gate，防止清洗器把错误拓扑“修成能解析但不能通信”。

### 9.3 `qw3-3`

48/48 rank 的 HCCL 检查已经通过，说明节点和通信世界成立。阶段 6 的 actor preflight 却拒绝了 kubelet 投影 ConfigMap 使用的合法符号链接。

这不是训练源文件篡改，而是 Kubernetes projected volume 的正常实现。

修复方式：

- 允许已知的 kubelet ConfigMap 投影链接结构；
- 不取消内容校验；
- 继续验证挂载后的 RankTable 内容 SHA 与 HCCL 阶段证据一致。

### 9.4 `qw3-4`

checkpoint 路径配置错误：

```text
错误：/mnt/models/0717
正确：/mnt/models/00_TRAIN_RES/0717
```

正确位置当时的 tracker 为 547000，对应：

```text
iter_0547000
```

这次问题推动了 checkpoint 路径与模板/默认配置的统一。

### 9.5 `qw3-5`

所有六个 Worker 已经看到 checkpoint，`torchrun` 也已启动，随后 rank 47 在 W&B 初始化阶段失败。

同时还发现数据路径从旧位置切换到了实际位置：

```text
旧：/mnt/models/DATA_BIN/CPM
新：/mnt/models/DATA_BIN/GEN
```

该次没有可靠证据证明已经完成 Megatron checkpoint 加载并进入 iteration，因此不能把它算作恢复训练成功。

### 9.6 `qw3-6`

active 节点为 `gpu-server-00/01/02`，备用节点为 `gpu-server-03`。由于 `gpu-server-02` 健康问题，gang 一直 Pending。

随后人工停止。旧状态文件仍可能显示 `RUNNING`，进一步暴露了停止和状态持久化需要收敛。

### 9.7 `qw3-7`

将 `gpu-server-02` 排除，使用 `gpu-server-00/01`，`gpu-server-03` 备用。

Ray Ready、HCCL PASS，正式进程进入分布式初始化。为了应用后续配置，任务被人工停止。因此它证明了两节点启动链路，但不是完整训练恢复证据。

### 9.8 `qw3-8`

关闭 W&B 后，训练成功完成了以下步骤：

- 识别并加载 checkpoint 547000；
- 构建模型；
- 构建数据；
- 完成第一次 forward。

随后 backward 报错：

```text
please install cann-nnal package first
```

现场检查发现 NNAL/ATB 实际已安装，但训练进程没有加载相应环境，`ATB_HOME_PATH` 未设置。

修复方式不是重复安装，而是在 Worker 正式启动训练前：

- source CANN 环境；
- source NNAL/ATB 环境；
- 增加轻量 precheck；
- 确保 Ray actor 继承这些变量。

### 9.9 `qw3-9`

再次出现相同的 NNAL/ATB 环境问题。它支持“正式 Ray actor/训练子进程没有获得所需环境”这一判断；由于没有保留下完整的启动环境证据，这里应视为基于重复现象的推断，而不是对变量传播路径的直接证明。

### 9.10 早期节点故障注入

曾通过以下方式模拟 `gpu-server-01` 故障：

- 临时把节点的 `huawei.com/Ascend910` allocatable 改成 0；
- 删除该节点上的唯一 Worker Pod；
- 使用 trap 在测试结束后恢复 allocatable。

训练当前 attempt 会因 Worker 丢失而退出。一次早期实现随后拒绝自动恢复，提示 checkpoint 在 active Worker 之间不可用或不一致。

这次测试说明：在 Worker 已经消失后，仅靠在线 RPC 询问所有 Worker 会把“无法访问”与“内容不一致”混在一起，恢复逻辑需要结合故障前持久证据和明确的 fail-closed 行为。

### 9.11 `qw3-10`

这是本轮最关键的自动换机实证。

初始拓扑：

```text
active: gpu-server-00, gpu-server-01
spare:  gpu-server-03
attempt: qw3-10-a00
```

`a00` 已完成：

- RayCluster Ready；
- HCCL gate 通过；
- checkpoint 547000 可见。

随后注入 `gpu-server-01` 故障并删除 Worker。运行侧观察到 ActorUnavailable/Socket closed。

Supervisor 诊断结果为：

- `gpu-server-00` 为健康 surviving active；
- `gpu-server-01` allocatable 为 0，是明确的故障目标；
- `gpu-server-03` 健康且可作为备用机。

之后执行：

```text
gpu-server-01 -> gpu-server-03
```

并创建：

```text
qw3-10-a01
```

新 attempt 的 active 变为 `gpu-server-00/03`，再次完成 HCCL gate。状态中记录：

```text
replacementCount = 1
quarantined = gpu-server-01
```

随后由用户手动停止。

`qw3-10` 证明了：

- 逻辑 run-id 在换机后保持不变；
- attempt 会递增；
- 明确节点故障可以触发备用机替换；
- 替换后会重建完整 Ray world；
- 新拓扑能够重新通过 HCCL。

它没有证明一个月运行，也没有覆盖所有软件型故障。

### 9.12 `qw3-jobs-1`

该任务用于验证正式训练切换到 Ray Jobs 后的行为。

已知现象：

- 训练 iteration 从约 547000 增长到约 547010；
- `gpu-server-00` 上出现 Actor Socket closed；
- peer 随后退出；
- 人工 stop 在 Socket closed 约 13 秒之后到达。

因此，Socket closed 不是人工 stop 导致的；但现有日志不足以确定更底层的根因。

由于人工停止抢在自动恢复流程完成前发生：

- `replacementCount = 0`；
- 没有创建 `a01`；
- 备用 `gpu-server-03` 没有实际使用。

所以该案例不能用来证明自动换机失败。它证明的是 Ray Jobs 已经承载真实训练，同时留下了一个未完全定位的 Ray actor/socket 故障样本。

## 10. W&B 与网络问题

本轮遇到过三类不同问题，不能混为一个“网络错误”：

1. `server-00` 上 DNS 解析超时；
2. Pod 能访问 W&B endpoint，但认证超时或返回 401；
3. 训练脚本没有得到有效 API key。

宿主机能联网不代表 Ray Pod 一定能联网。Pod 的网络路径还经过：

- CNI；
- Pod DNS；
- Service/路由；
- NAT 或代理配置；
- NetworkPolicy；
- 容器内代理环境变量。

Ray Pod 与宿主机之间通过 Kubernetes CNI、Pod IP 和节点路由通信；它不自动继承宿主机 shell 中的 VPN/代理状态。宿主机端口 `1314` 上的反向代理，也只有在 Pod 路由和地址配置正确时才能被使用。

为了避免短暂 W&B 抖动过早终止长训练，本轮将 `WANDB_INIT_TIMEOUT` 设置为约 300 秒。Ray Job 状态查询另有有限退避重试，但本轮没有为 W&B 自行实现三次重连。为了先验证核心训练和恢复链路，后续测试又临时关闭了 W&B。

这里形成的原则是：

- W&B 是可选观测能力，不应被误判为 NPU 硬件故障；
- 外部服务故障可以有限重试；
- 如果业务允许，应让训练在 W&B/实验追踪服务不可用时继续，而不是耗尽备用机；
- API key 应通过明确配置注入，不应依赖某个开发者 shell 的隐式环境。

## 11. Ascend 环境与 device plugin 问题

### 11.1 CANN/NNAL/ATB

`qw3-8/9` 说明“软件已安装”和“训练进程环境正确”是两回事。

Ray actor 是新的进程环境。即使宿主或镜像中存在 NNAL/ATB，如果 actor 没有 source 对应环境脚本，训练仍会报依赖不存在。

因此环境加载必须发生在真正执行训练的进程链上，而不是只在发起 `kcc_ray` 的登录 shell 中设置。

### 11.2 同事遇到的 kubelet socket 错误

同事提供的 device plugin 日志包括：

```text
dial unix /var/lib/kubelet/pod-resources/kubelet.sock:
connect: connection refused

no pod passed the filter
not get valid pod
```

对现有归档的检查没有找到完全相同的历史错误字符串。因此不能声称我们曾经复现过同一故障。

本轮 A3 成功测试只能证明：在当时的测试窗口、所用设备和拓扑下，Kubernetes/device plugin 最终能够完成分配并启动训练。它不能证明：

- device plugin 从未发生过瞬态 socket 问题；
- 后续时间点的 kubelet socket 一直健康；
- 未测试的设备号也没有问题；
- 同事当时遇到的错误与我们的旧故障完全相同。

`kubelet.sock connection refused` 更接近 kubelet/device-plugin 基础设施故障，而不是 MindSpeed 训练脚本问题。KCC 可以观察失败并决定重试，但不能在训练工具内部修复 kubelet 本身。

## 12. A3 单机实机验证

本轮还检查了 A3 机器上两卡和八卡的历史实证。

> 这些 A3 结果来自另一套历史 `trainctl`/RayJob 测试，是 Ascend、Ray、HCCL、真实训练和 checkpoint 恢复的兼容性旁证，不是本轮 legacy `kcc_ray + Supervisor` 实现的直接验收证据。

### 12.1 两卡测试

在 `a3-server-00` 上使用两张物理 NPU 进行了真实测试，覆盖：

- Ray Job；
- 两 rank HCCL；
- Qwen3-143M 真实训练；
- 每 iteration checkpoint；
- 在已提交 iteration 1 后注入失败；
- 新 attempt 从 checkpoint 恢复并继续完成 iteration 2–5；
- 作为恢复源的 iteration-1 checkpoint/manifest 保持不变。

这证明两张卡能够为常见训练加速和推理加速实验提供一套可运行基础，例如：

- 数据并行与通信开销观察；
- 混合精度；
- activation checkpoint；
- 算子融合；
- KV cache、batching 和推理吞吐测试；
- checkpoint 恢复链路。

是否能放下更大模型仍取决于参数规模、优化器、序列长度、batch size 和并行策略，不能只由“有两张卡”直接判断。

### 12.2 八卡测试

A3 物理 0–7 的八卡测试完成：

- 8-rank HCCL；
- Qwen3-143M 5 iterations；
- loss 从约 11.24887 降到约 10.55710；
- checkpoint 1–5；
- Ray Job 最终 `SUCCEEDED`。

### 12.3 证据边界

A3 历史成功证明了：

- 单机部分卡可以进行真实 NPU 计算；
- 单机两卡 Ray/HCCL/训练/checkpoint 恢复走通；
- 单机八卡 Ray/HCCL/训练走通。

它没有证明：

- A3 多机训练已经走通；
- A3 多节点自动备用机替换已经走通；
- 设备 15 或所有物理卡在所有时间都健康；
- device plugin 永远不会出现 kubelet socket 瞬态错误；
- 月级训练已经完成。

## 13. 本轮最重要的技术难点

### 13.1 分布式故障不是单进程故障

单个 Worker 消失后，剩余进程不等于“还能继续”。HCCL world 已经破坏，恢复必须围绕整个拓扑重建，而不是只补一个 actor。

### 13.2 checkpoint 的存在性与提交性

最大的 `iter_*` 目录不一定完整。必须使用 tracker 作为提交点，再通过所有 Worker 一致性和文件检查建立可恢复证据。

### 13.3 失败归因

以下现象表面上都可能表现为“训练挂了”：

- NPU 硬件故障；
- kubelet/device plugin 问题；
- Ray Head/Driver/actor 故障；
- HCCL 拓扑错误；
- W&B 网络或认证失败；
- checkpoint 路径错误；
- 数据路径错误；
- CANN/NNAL/ATB 环境没有加载；
- 合法 ConfigMap 投影被安全检查误拒绝。

如果分类错误，系统可能把软件错误当坏机，快速耗尽备用池。因此恢复策略采用稳定证据、有限重试和 fail-closed。

### 13.4 分布式控制的幂等性

Ray Job 提交成功但响应丢失、Supervisor Pod 重启、Ray Dashboard 暂时不可达，都可能制造重复提交窗口。

解决思路是：

- 稳定的逻辑 run-id；
- 单调递增 attempt；
- 确定性 submission ID；
- create-only 提交记录；
- 资源归属校验；
- 重启后 reconnect，不盲目 resubmit。

### 13.5 安全停止

立即停止和 checkpoint 后停止分别代表“尽快结束”和“先完成一次新的持久提交”。后者本质上是一个跨 `training_control.py` CLI/观察进程、Supervisor、多个 Worker、训练 tracker 和 Kubernetes 资源的协调协议，不能用固定 sleep 替代。

### 13.6 长时间运行中的有界状态

长期运行的关键不是让一个 Python 进程永远不死，而是：

- Supervisor 可以重建；
- 状态原子落盘；
- 日志不复制进状态；
- attempt/诊断历史有界；
- 模板和提交身份稳定；
- checkpoint 是外部持久恢复点。

### 13.7 安全与自动化之间的平衡

自动换机越激进，越容易误杀、误换或并发启动两份训练；越保守，越容易进入人工处理。

本轮的取舍是：

- 已知的短时软件/网络问题先重试；
- 只有稳定节点级证据才换机；
- checkpoint、归属或 Ray Job 状态不确定时停止自动决策；
- 所有删除限定在当前 run 的资源范围内。

## 14. 已完成能力与尚未证明的能力

### legacy `kcc_ray` 已完成或有实机证据

- `start` 默认恢复、`--fresh` 显式新训练；
- active/spare 节点可配置；
- 每个 attempt 完整执行六阶段；
- RankTable 空洞清洗和 HCCL gate；
- 模板快照和分布式参数注入；
- 正式训练通过 Ray Jobs 提交；
- Supervisor 可在 Kubernetes Job 的 `backoffLimit` 内重建并续接，但前提是固定的管理节点及其 hostPath 状态目录仍可用；
- 立即停止和 checkpoint 后停止；
- checkpoint 以 tracker 和跨 Worker 一致性判断；
- `state.json` 有界、原子写入和结果外置；
- `qw3-10` 完成一次明确故障节点到备用节点的自动替换，并在新拓扑上重新通过 HCCL；尚未证明新拓扑继续加载 checkpoint 并推进 iteration；

### 独立的历史兼容性旁证

- A3 单机两卡 Ray/HCCL/短训练/checkpoint 恢复和八卡训练有真实历史证据；
- 这些证据不归属于 legacy `kcc_ray + Supervisor` 的直接验收。

### 尚未由本轮实机证明

- 连续运行一个月且经历多次真实故障；
- `server-00` 整机故障后的 legacy Supervisor 跨节点接管；
- 共享 checkpoint 存储故障后的恢复；
- 所有 Ray Socket closed 的底层原因都能自动定位；
- 所有节点局部软件故障都能安全触发换机；
- A3 多机训练和多机备用节点恢复；
- W&B 在所有 Worker 网络环境下长期稳定；
- 日志和 checkpoint 的月级容量治理。

## 15. 关键实现文件

- [`bin/kcc_ray`](../bin/kcc_ray)：统一 CLI 入口；
- [`config/cluster.yaml`](../config/cluster.yaml)：默认节点、路径、超时和其他可分发配置；
- [`ray_startup_bundle/start_ray.py`](../ray_startup_bundle/start_ray.py)：六阶段启动编排；
- [`ray_startup_bundle/recovery_supervisor.py`](../ray_startup_bundle/recovery_supervisor.py)：逻辑 run、attempt、诊断和恢复状态机；
- [`ray_startup_bundle/supervisor_job.py`](../ray_startup_bundle/supervisor_job.py)：Kubernetes Supervisor Job；
- [`ray_startup_bundle/ray_training_submit.py`](../ray_startup_bundle/ray_training_submit.py)：Ray Jobs 提交、查询和重连；
- [`ray_startup_bundle/training_control.py`](../ray_startup_bundle/training_control.py)：立即停止和 checkpoint 后停止；
- [`ray_startup_bundle/hccl_runtime/hccl_check/ranktable/cleaner.py`](../ray_startup_bundle/hccl_runtime/hccl_check/ranktable/cleaner.py)：RankTable 清洗；
- [`ray_startup_bundle/training_templates/pretrain_150M.sh`](../ray_startup_bundle/training_templates/pretrain_150M.sh)：默认训练模板；
- [`ray_startup_bundle/README.md`](../ray_startup_bundle/README.md)：工具内部使用与设计说明。

## 16. 可复核的历史证据

当前 legacy 日志已从工具目录移到工作区归档，以保持分发目录干净：

```text
/home/ywj/kcc-workspace-extras-20260825/pretrain-ray-main/log
```

关键文件包括：

```text
# qw3-10 自动替换状态
/home/ywj/kcc-workspace-extras-20260825/pretrain-ray-main/log/training-jobs/qw3-10/state.json

# qw3-8 正式训练执行结果
/home/ywj/kcc-workspace-extras-20260825/pretrain-ray-main/log/training-runs/qw3-8-a00/execution-result.json

# A3 两卡故障恢复
/home/ywj/pretrain-ray-platform-workspace-archive/test-results/a3-ray-qwen-recovery-20260724.json

# A3 八卡真实训练
/home/ywj/pretrain-ray-platform-workspace-archive/test-results/a3-ray-qwen-fresh-8npu-20260727.json
```

## 17. 最终结论

本轮工作的主要成果不是简单增加了几个命令，而是把原本一次性的 Ray 启动脚本，整理成了一个具有明确安全边界的训练生命周期工具：

```text
统一启动入口
  + 完整六阶段前置验证
  + Ray Jobs Driver
  + Kubernetes Job Supervisor（管理节点和 hostPath 可用时）
  + tracker 驱动的 checkpoint 共识
  + attempt 化重建
  + 节点诊断与备用机替换
  + 两种停止协议
  + 有界、原子、可续接的控制状态
```

最关键的实机闭环是 `qw3-10`：在训练 world 已经建立后，人工注入一个明确的节点故障，Supervisor 识别故障节点，选择健康备用机，保持逻辑 run-id 不变、递增 attempt，并在新拓扑上重新通过 HCCL。这是“故障诊断、换机和 world 重建”的闭环；由于随后人工停止，它不是“新拓扑从 checkpoint 恢复并继续迭代”的完整训练恢复闭环。

与此同时，文档没有把短期成功夸大为完全生产化。当前实现已经具备继续做长时间训练验证的基础，但月级无人值守还需要存储容量、日志轮转、外部组件高可用、管理节点故障和更多故障类型的持续验证。
