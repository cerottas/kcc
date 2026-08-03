# Ray 正式预训练启动链路

这个目录是当前可直接使用的正式入口。它把原来的 Shell 多机启动方式串成：

```text
环境与占用检查
→ 根据本次 --node 列表渲染 RayCluster
→ 启动 RayCluster
→ 自动发现 worker/NPU 拓扑
→ 生成并验证 RankTable
→ 原生 HCCL AllReduce
→ 为每个 worker 复制并注入正式训练脚本
→ Ray 整机 actor 并发启动各节点 torchrun
```

`raycluster.yaml` 是基础模板，每个 worker 当前申请整机 8 张 NPU。主入口会
根据本次重复传入的 `--node` 自动生成一份 create-only YAML，设置 worker
副本数和节点 affinity；参数注入器与 Ray driver 同样不固定为 2×8。

## 一行启动

```bash
cd /home/ywj/pretrain-ray-platform/ray_startup_bundle
PYTHONUNBUFFERED=1 ./start_ray.py \
  --allow-topology-change \
  --confirm-checkpoint-exclusive
```

当前默认节点是 `110.129.0.20/22`（2 台），而正式模板声明 `NNODES=6`，
所以示例必须显式确认拓扑变化。若提交的节点数与源脚本一致，不需要该参数。

每次机器数不同时重复传 `--node` 即可，worker 数由节点列表自动得出：

```bash
./start_ray.py \
  --node gpu-server-02 \
  --node gpu-server-07 \
  --node gpu-server-08 \
  --allow-topology-change \
  --confirm-checkpoint-exclusive
```

`--node` 同时接受 Kubernetes 节点名和 InternalIP。基础 YAML 不需要为每次
训练手工修改。

## 两台备用机的故障恢复入口

需要自动换机时改用 `recovery_supervisor.py`。它复用 `start_ray.py` 的全部
启动参数；前 6 个 `--node` 是本次 active，两个 `--spare-node` 是备用池：

```bash
PYTHONUNBUFFERED=1 ./recovery_supervisor.py \
  --run-id pretrain-150m-20260803 \
  --node gpu-server-00 \
  --node gpu-server-01 \
  --node gpu-server-02 \
  --node gpu-server-03 \
  --node gpu-server-04 \
  --node gpu-server-05 \
  --spare-node gpu-server-06 \
  --spare-node gpu-server-07 \
  --confirm-checkpoint-exclusive
```

`--run-id` 在这里是逻辑任务 ID；实际每轮证据使用
`<run-id>-a00`、`<run-id>-a01`。正式训练明确导出 `FAIL` 后，恢复入口会：

1. 重发当前 RayCluster 的幂等删除，等待旧 RayCluster、Pod、Service 和
   RankTable ConfigMap 全部消失；删除 Pod 会结束该 RayCluster 内的旧训练进程。
2. 只做一次 active + 当前剩余备用机快照。Node 消失或 NotReady、Kubernetes
   NPU 数量不足、exporter 少卡或报卡异常均记为坏机；证据读不到记为 unknown。
3. 只有所有存活 active 已空闲，且坏机数不大于健康、空闲备用机数时，才整批
   替换。备用机按声明顺序放入原 active 列表位置；新一轮实际 `node_rank` 仍由
   新发现的 Pod/RankTable 拓扑重新冻结，不假设物理机 rank 不变。
4. 用替换后的完整 active 列表从六阶段起点创建新 Ray world；不会复用上一轮
   actor、RankTable 或 checkpoint 检测结果。
5. 新 world 的所有 worker 重新读取 `/mnt/models` 上最新已提交 checkpoint；
   一致性检查通过后，训练脚本才按原来的 `--load`/`--save` 拉起 `torchrun`。

### 最新可恢复 checkpoint 的检测

恢复模式不会按目录名扫描编号最大的 `iter_*`。每一轮正式 `torchrun` 拉起前，
6 个 Ray worker 都会从共享的 `CKPT_LOAD_DIR` 读取 Megatron 自己的提交指针：

```text
/mnt/models/0717/latest_checkpointed_iteration.txt
```

指针中的正整数 `N` 唯一选择 `iter_%07d`（例如 `1000` 对应
`iter_0001000`）。以当前模板的
`TP=1`、`PP=1` 和 distributed optimizer 为例，所有 worker 必须同时看到并能
读取以下两个非空文件：

```text
/mnt/models/0717/iter_XXXXXXX/mp_rank_00/model_optim_rng.pt
/mnt/models/0717/iter_XXXXXXX/mp_rank_00/distrib_optim.pt
```

各 worker 的 tracker 内容、tracker 摘要、所选目录、文件相对路径和文件大小还
必须完全一致。这样，故障保存遗留的更大 `iter_*` 半成品不会被误选。tracker
缺失/损坏、指向目录或文件不完整、值为 `release`，或者各 worker 视图不一致时，
本轮导出 `CHECKPOINT_UNAVAILABLE`；恢复入口清理本轮 RayCluster 后写入
`MANUAL_REQUIRED`，不会进入坏机诊断，也不会消耗备用机。平台不修改 tracker，
也不会自动回退到更旧目录。文件内容以及模型/优化器参数是否真正兼容，仍由
MindSpeed/Megatron 的正式加载做最终确认。

checkpoint 内容错误和机器掉线分开处理：tracker/分片确实有问题时按上述规则
人工处理；如果 worker 在 checkpoint 检查期间掉线、actor 丢失或 Ray RPC 失败，
该轮记为 worker runtime failure，仍会在清理旧 world 后进入坏机诊断。actor
身份检查、worker 预检和 checkpoint RPC 的等待上限均为 300 秒，避免共享存储
或失联 actor 令 supervisor 无限等待。换机后的下一轮再从头检查 checkpoint，
避免把机器故障误报成 checkpoint 损坏。driver 内部错误、协议证据异常和人为
中断则清理后直接 `MANUAL_REQUIRED`，不会因软件错误消耗备用机。

因此两个备用机既可以处理两轮各坏一台，也可以处理同一轮同时确认坏两台；
若坏机多于剩余健康备用机、任一 active 状态不确定，或清理后 exporter 仍看到
NPU 进程，则停止并写入 `MANUAL_REQUIRED`，不会部分换机或扫描宿主机 PID。

恢复状态原子写入：

```text
log/training-jobs/<run-id>/state.json
```

其中记录初始/剩余备用机数、累计替换数、隔离节点和每次 attempt 的诊断结果。
失败 attempt 保存坏机到备用机的 `replacements`；下一 attempt 保存替换后的
`activeNodes`，并在结束时保存正式 `trainingResult`。只有该结果属于当前 run ID、
状态为 `PASS`，且含全部 worker 一致的 checkpoint iteration/目录证据，整个恢复
任务才会写成 `PASS`。
恢复模式会把已确认失败的 RayCluster 保留时间强制为 0；checkpoint 和
`/mnt/models` 中的训练日志不会被删除。

默认正式模板是：

```text
training_templates/pretrain_150M-22.sh
```

它基于 `/home/ywj/qwen3/pretrain_150M-22.sh`，原脚本不会被修改；当前模板
采用 6 台、每台 8 张 NPU、`GBS=96` 的正式基线。也可以显式提交另一份同结构脚本：

```bash
./start_ray.py --train-script /path/to/train.sh
```

默认训练工作目录是 worker 内已经验证过的：

```text
/mnt/models/CODE/MindSpeed-LLM-v2.3.0
```

如镜像内源码位置变化，使用 `--training-cwd /new/path`。

## 自动注入与保持不变的内容

HCCL 成功后，`inject_training_params.py` 根据本次证据为每个 worker 生成
一份脚本，只修改：

- `RANK_TABLE_FILE`
- `NPUS_PER_NODE`
- `MASTER_ADDR`、`MASTER_PORT`
- `NNODES`、`NODE_RANK`
- 每个 node rank 独立的 `LOG_FILE`
- 未注释的 `torchrun`，改为
  `/root/miniconda3/envs/ms/bin/torchrun`

`MASTER_PORT` 默认保留源脚本值，可用 `--master-port` 覆盖。

如果 HCCL 发现的 `NNODES × NPUS_PER_NODE` 与源脚本不同，注入阶段默认停止，
不会启动训练。缩容或扩容必须由用户显式传入
`--allow-topology-change`；该确认会写入 `injection.json`，供前端审计。
确认前需要同时核对 checkpoint 是否支持新 world size，并确保
`CKPT_SAVE_DIR` 没有被另一训练任务写入。现有 legacy Shell 任务没有统一锁，
因此这一项只能由提交者通过 `--confirm-checkpoint-exclusive` 确认；确认值和
目录会写入 `injection.json`。平台不会擅自改 checkpoint 目录。

以下正式训练语义不会被改写：

- `TRAIN_ITERS` 及模型、并行、batch、学习率参数
- 数据路径与数据缓存参数
- checkpoint 的 `--load`、`--save`、`--save-interval`
- `--exit-on-missing-checkpoint`
- W&B 和 TensorBoard 参数

流程不删除任何 checkpoint。训练通过 `/bin/bash -o pipefail` 执行，避免
尾部 `tee` 掩盖 `torchrun` 的失败退出码。

## 文件职责

- `start_ray.py`：唯一正式主入口；顺序执行六个阶段，任一阶段失败即停止后续阶段。
- `environment_check.py`：只读检查 Kubernetes NPU 占用、硬件健康和 NPU
  进程；发现占用时打印原因并退出，不停止别人的进程。
- `ray_cluster_start.py`：打包 HCCL runtime、应用 YAML、等待 Ray 就绪。
- `render_raycluster.py`：把节点名或 InternalIP 解析为本次 worker 列表，
  生成对应副本数和 affinity 的运行 YAML。
- `hccl_gate.py`、`hccl_runtime/`：拓扑发现、RankTable 和真实 HCCL gate。
- `inject_training_params.py`：读取 PASS 证据，创建每节点正式脚本和冻结清单。
- `ray_training_submit.py`：把清单、脚本和 driver 复制到 Ray head 并提交。
- `ray_training_driver.py`：每个 worker 申请其全部 NPU，核对 Pod/RankTable/
  `ms` 环境后并发执行正式脚本。
- `recovery_supervisor.py`：失败清理、单次诊断、备用机计数、整批替换和重启。
- `recovery_diagnostics.py`：一次性读取 Kubernetes/exporter 证据并给出
  fail-closed 的 N 坏机换 N 备用机决策。
- `training_templates/`：正式脚本副本；不修改 `/home/ywj/qwen3` 原文件。
- `raycluster.yaml`：当前两节点 Ray 部署及镜像、挂载、资源声明。

仓库旧的 `scripts/start_ray.py` 两阶段入口已经删除，避免误用旧 smoke
manifest。正式训练统一从本目录的 `start_ray.py` 启动。

旧训练 smoke、测试、历史产物和报告已迁移到同级工作区
`/home/ywj/pretrain-ray-platform-workspace-archive/`，不会进入 launcher
镜像。HCCL 代码中的 `ranktable_smoke` 是通信验证证据类型，不是旧训练
smoke。

## 证据、日志与资源生命周期

每次运行自动生成唯一 `run-id`。主要本地结果为：

```text
log/hccl-startup/<run-id>/
log/training-runs/<run-id>/injection/
log/training-runs/<run-id>/execution-result.json
```

这些控制端目录应挂载持久卷。worker 上的正式训练日志统一写入共享
`/mnt/models`：

```text
/mnt/models/pretrain-ray-platform/log/<run-id>/logs/node-rank-<N>.log
/mnt/models/pretrain-ray-platform/log/<run-id>/ray-driver/node-rank-<N>/
```

成功时默认删除 RayCluster 以释放 NPU，但保留 checkpoint、训练日志和本地
证据。使用 `--keep-success-resources` 可保留 RayCluster。

Ray 启动、HCCL 或训练失败时，已确认的失败资源默认保留 1800 秒后删除
RayCluster；Namespace、checkpoint 和日志不会删除。使用
`--failure-retention-seconds 0` 可立即清理，使用 `-1` 可永久保留。如果
训练状态不确定或结果未能导出，RayCluster 会保留，避免误删仍在运行的任务。

需要提前删除时，可中断等待进程后执行：

```bash
/usr/local/bin/k3s kubectl \
  --kubeconfig /home/ywj/.kube/k3s-learning.yaml \
  delete raycluster pretrain-gpu00-gpu01 \
  -n pretrain-ray --wait=false
```

当前 `ray_training_submit.py` 是前台等待式 CLI：训练期间启动进程需要持续运行。
Ctrl-C 或控制链路断开时不会猜测训练状态，也不会删除 checkpoint，RayCluster
会留在现场供检查。接入前端时应由持久化的后端任务进程托管该 CLI；将提交
进一步改成可重连的 Ray Job 是后续 TODO，不影响当前 actor 拉起训练的路径。

## 为前端预留

前端只需调用 `start_ray.py` 对应的后端 API，不需要了解 Kubernetes、
RankTable 或 HCCL。已经预留的提交字段包括：

- 训练脚本、训练工作目录
- 节点列表（数量可变）、基础 YAML、namespace、RayCluster 名称
- 期望 worker/world size
- master port
- 是否明确同意改变源脚本拓扑（缩容/扩容）
- 是否确认 checkpoint 保存目录没有其他 writer
- 各阶段 timeout、失败资源保留时间
- caller 提供的 run ID
- 成功后是否保留 RayCluster

`run-id` 可作为任务 ID；上述 JSON 目录可直接映射为阶段状态、注入参数、
节点日志和最终结果接口。
