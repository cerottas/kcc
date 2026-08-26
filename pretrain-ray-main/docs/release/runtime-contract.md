# 训练子进程环境契约

stable runtime 通过环境变量向 Recipe 的 `command` 传递路径和分布式上下文。框架启动
脚本必须显式读取这些变量；如果仍把输出或 checkpoint 写到硬编码路径，控制面无法可靠
发现、发布或恢复结果。

## 输入与工作目录

- `KCC_SOURCE_DIR`：已校验并物化的 source artifact 根目录；训练代码应只读使用。
- `KCC_MODEL_DIR`：已校验并物化的 model artifact 根目录；训练代码应只读使用。
- `KCC_DATA_DIR`：已校验并物化的 data artifact 根目录；训练代码应只读使用。
- Recipe `workingDirectory` 相对 `KCC_SOURCE_DIR` 解析，运行时将该目录作为子进程 cwd。

输入目录是内容寻址缓存。训练脚本不得原地修改、删除或把运行状态写入这些目录。

## 可写输出与恢复

- `KCC_OUTPUT_ROOT`：本 TrainingRun 的稳定可写输出根；最终要发布的输出必须写在这里。
- `KCC_CHECKPOINT_ROOT`：checkpoint 根目录。框架应在此维护 tracker（例如
  `latest_checkpointed_iteration.txt`）和对应 iteration 目录，且只在 checkpoint 完整落盘后
  原子更新 tracker。
- `KCC_RESUME_FROM`：仅在存在已验证恢复点时设置，值为恢复 checkpoint 目录。脚本必须
  在非空时将它转换成框架的 resume/load 参数；首次 attempt 不保证存在该变量。
- `KCC_ATTEMPT_ROOT`：当前 attempt 的隔离工作根，用于日志和 attempt 临时状态。
- `KCC_ATTEMPT`：从 0 开始的当前 attempt 编号。

不要把最终输出只写入 `KCC_ATTEMPT_ROOT`，也不要把未完成的临时目录登记为 checkpoint。

## Checkpoint 后暂停合同

`suspendMode: AfterCheckpoint` 不要求训练脚本实现额外控制 API，但要求严格遵守 tracker
提交语义：checkpoint 的所有 shard 完整落盘并持久化后，最后一步原子更新
`latest_checkpointed_iteration.txt`。runtime 接受请求时记录当前 iteration；只有所有
Worker 一致看到更大的 iteration，并且 sampled snapshot digest 一致，才会向训练进程组
发送 SIGTERM。结果以 `STOPPED`、请求 generation、baseline iteration 和 checkpoint
摘要写入 Attempt Result ConfigMap。

等待期间不会删除 RayCluster。取消请求会恢复 `Continue`；如果取消与停止发生竞态，
Controller 会从已确认 checkpoint 创建新 attempt，且不消耗故障重试预算。

## 分布式上下文

- `RANK_TABLE_FILE`：runtime 校验并快照后的 ClusterD/HCCL RankTable 文件。
- `NODE_RANK`：当前 Ray worker 的节点序号。
- `WORLD_SIZE`：全局训练进程数（worker 数乘每节点设备数）。
- `KCC_NODE_RANK`、`KCC_WORLD_SIZE`：兼容别名；新脚本优先消费标准名称。

runtime 使用结构化 argv 启动 `torch.distributed.run`，同时传入 node rank、world topology、
master address/port。Recipe 不应覆盖上述控制面变量，也不应自行发现宿主机节点或重写
RankTable。

## 最小接入检查

1. 用短任务打印变量是否存在及路径类型，但不要输出 Secret。
2. 从 `KCC_SOURCE_DIR` 读取代码，从 `KCC_MODEL_DIR`/`KCC_DATA_DIR` 读取输入。
3. 在 `KCC_CHECKPOINT_ROOT` 产生一个可恢复 checkpoint，并以新的 attempt 验证
   `KCC_RESUME_FROM`。
4. 将可发布文件写入 `KCC_OUTPUT_ROOT`，确认 TrainingRun 最终生成 `status.outputArtifact`。
5. 多节点任务确认 `NODE_RANK`、`WORLD_SIZE` 与 `RANK_TABLE_FILE` 拓扑一致。
