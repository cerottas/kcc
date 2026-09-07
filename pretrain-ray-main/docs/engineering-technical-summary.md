# KCC 工程化与集群排障工作总结

> 范围：只总结本轮上下文中实际讨论、检查、修改和验证过的工作，不扩展到仓库中未参与讨论的其他功能。
>
> 代码基线：2026-09-07，commit b41a23b。

## 1. 本轮工作的目标

本轮工作从两个问题开始：

1. 解释 /home/ywj/kcc 中原有自动化训练流程；
2. 判断它能否被标准化、工程化，并接入现有平台后部署到其他集群。

后续目标逐步明确为：

- 保留已经跑通过的训练、HCCL、checkpoint 和恢复能力；
- 去除固定用户目录、固定节点、固定卡数、固定镜像和本地状态依赖；
- 将训练流程变成声明式 Kubernetes 工作负载；
- 为 Backstage/Crossplane 提供稳定接口；
- 让 KCC 只承担训练生命周期，不重复实现平台已有能力；
- 支持打包、版本化、上传 GitHub/Gitea 和跨集群部署；
- 避免“过度防御”把临时错误全部变成人工阻塞；
- 通过真实集群排障，明确 KCC 与 K3s、KubeRay、Volcano、MindCluster、Ascend 环境的责任边界。

## 2. 最初的自动训练流程

工程化前的主要入口是 bin/kcc_ray 和 ray_startup_bundle。单次运行由六个阶段组成：

~~~text
1. 环境和 NPU 占用检查
2. 按本次节点列表渲染 RayCluster
3. 创建并等待 RayCluster
4. 发现拓扑、生成 RankTable、执行 HCCL gate
5. 复制训练脚本并注入分布式参数
6. 通过 Ray Jobs 启动正式训练
~~~

恢复模式外面还有一层 Supervisor：

~~~text
kcc_ray start
  -> Kubernetes Supervisor Job
  -> attempt-0 六阶段
  -> 失败后清理旧 RayCluster
  -> 检查 active/spare 节点
  -> 同拓扑重试或替换故障节点
  -> attempt-1 重新执行六阶段
  -> 从已提交 checkpoint 恢复
~~~

旧实现已经具备一些实际价值：

- Ray Job 与终端会话解耦；
- RankTable 和 HCCL 在训练前验证；
- 训练脚本按 Worker 注入 node rank；
- checkpoint 通过 tracker 判断；
- 故障后能够使用备用节点；
- 支持立即停止和 checkpoint 后停止。

但它不适合作为可迁移的平台实现：

- kubeconfig、kubectl 路径和管理节点目录写进配置；
- /home/ywj、/mnt/models 等现场路径成为隐式合同；
- A2/A3、8 卡/16 卡、镜像、节点和挂载容易硬编码；
- 状态依赖本地目录和 Supervisor JSON；
- 上层只能调用 CLI，缺少稳定 Kubernetes API；
- 集群参数、训练参数和运行状态混在一起；
- CLI、Python 脚本、YAML 模板与恢复脚本耦合较深。

这也是我们判断“工程化前能使用”和“已经适合跨集群交付”不是一回事的原因。

## 3. 集成方案如何收敛

### 3.1 从完整平台链路到明确边界

最初讨论过：

~~~text
GitHub/Gitea
  -> Tekton
  -> Argo CD
  -> Crossplane
  -> TrainingRun
  -> KCC
  -> KubeRay/RayCluster
~~~

上下文中随后进行了两个重要澄清：

- Material 是当时用于参考的集成工程，不是训练请求必须经过的一层；
- 实际用户入口由 Backstage 负责，所谓臃肿主要指 KCC 自身代码和部署方式。

最终形成的边界是：

~~~text
静态交付：
Gitea/GitHub -> CI -> Argo CD
                        -> KCC CRD/Controller
                        -> Crossplane XRD/Composition
                        -> Profile/Recipe catalog

运行请求：
Backstage -> TrainingRequest（Crossplane XR）
          -> Crossplane Composition
          -> TrainingRun
          -> KCC Controller
          -> 独立 RayCluster
          -> KubeRay
          -> Head/Worker
~~~

### 3.2 各组件职责

| 组件 | 本轮确定的职责 |
|---|---|
| Backstage | 用户表单、项目/租户、创建、停止、恢复和状态展示 |
| Crossplane | 将 TrainingRequest 翻译为一个 TrainingRun，并回写状态 |
| Argo CD | 部署静态 CRD、Controller、XRD、Composition 和 catalog |
| Tekton/CI | 测试、镜像、Schema、bundle 和 artifact 校验 |
| KCC | attempt、停止、恢复、HCCL、checkpoint 和结果 |
| KubeRay | 根据 RayCluster 创建 Head/Worker Pod |
| Volcano | 可选的 gang/topology 调度能力 |

关键结论：

- Crossplane 不直接创建 RayCluster；
- Argo CD 不管理每次 TrainingRun；
- KCC 不安装或接管 KubeRay；
- KubeRay Operator 可以全局复用；
- RayCluster 不与其他训练共享，每个 attempt 单独创建；
- Tekton 不进入训练运行状态机。

### 3.3 TrainingRequest 为什么是可选层

KCC 自身只需要 TrainingRun。

如果 Backstage 能够直接、安全地创建 KCC 对象，可以直接提交 TrainingRun。

如果平台需要隐藏镜像、节点和物理卡号，并承担审批、配额、租户和状态映射，则使用 Crossplane TrainingRequest XR。

TrainingRequest 不是一个新的独立服务，而是 Crossplane 暴露的 Kubernetes API。当前方案倾向保留这一层，但它只转换意图，不实现 HCCL、checkpoint 或恢复逻辑。

## 4. 本轮完成的工程化改造

### 4.1 收敛公共 API

公共模型收敛为三个 training.kcc.io/v1beta1 对象：

| 对象 | 内容 |
|---|---|
| TrainingRuntimeProfile | 镜像、卡型、卡数、RuntimeClass、PVC、节点池、selector 和外部集成 |
| TrainingRecipe | 训练框架、结构化命令、工作目录、环境变量和制品 |
| TrainingRun | Worker 数、恢复预算、节点选择、参数覆盖和停止意图 |

RayCluster、attempt ConfigMap、Ray submission ID、Lease 和 progress/result ConfigMap 都变成内部对象，不再要求 Backstage/Crossplane 理解。

### 4.2 将本地状态机迁入 Controller

原来由本地 Supervisor 和 JSON 状态文件承担的职责，被迁入 in-cluster Controller：

- 从 TrainingRun 推导期望状态；
- 创建确定性 attempt；
- 创建、等待和删除 RayCluster；
- 幂等提交 Ray Job；
- 读取 progress/result；
- 处理 suspend、retry、replace 和 terminal status；
- 将稳定状态写入 TrainingRun.status；
- 使用 Lease 支持 Controller 双副本。

并发和所有权保护包括：

- resourceVersion 乐观并发；
- TrainingRun UID；
- attempt 编号；
- ownerReference；
- 删除 RayCluster 时使用 UID precondition；
- 拒绝旧 attempt 的 result 污染新 attempt。

### 4.3 去除主要硬编码

本轮处理的硬编码包括：

- run-id 不再固定为 test1；
- 节点通过 TrainingRun/Profile 或 CLI 参数传入；
- Worker 数和节点列表一致；
- 每节点卡数可配置；
- A2 的 8 卡和 A3 的 16 卡不再共用隐式假设；
- 支持可选物理设备 ID；
- 镜像、workspace、RuntimeClass、selector 和 pull secret 外置；
- command、arguments、environment 和 artifact 支持 per-run 覆盖；
- 公共 API 不再依赖本地 kubeconfig 和 SSH。

legacy kcc_ray 也补充了 A3、卡数、workspace 和镜像等可配置项，以便迁移期继续实测。

### 4.4 训练脚本和参数注入

长期方案被确定为：

~~~text
训练人员提交 Gitea 源码
  -> CI 固定 commit/tag，构建 source artifact
  -> Recipe 引用 source artifact
  -> KCC 下载到共享 workspace
  -> 从相对 workingDirectory 执行结构化 command
~~~

参数合并链路：

~~~text
Recipe.command
  <- TrainingRun.training.command 覆盖
  + TrainingRun.training.arguments 追加

Recipe.environment
  <- TrainingRun.training.environment 覆盖

Recipe.artifacts
  <- TrainingRun.training.artifacts 覆盖
~~~

KCC 再注入：

- RANK_TABLE_FILE；
- MASTER_ADDR、MASTER_PORT；
- NODE_RANK、WORLD_SIZE；
- KCC_SOURCE_DIR、KCC_MODEL_DIR、KCC_DATA_DIR；
- KCC_OUTPUT_ROOT、KCC_CHECKPOINT_ROOT；
- KCC_ATTEMPT、KCC_RESUME_FROM。

最终使用结构化 argv 调用 torch distributed，不拼接一整条 shell 命令。

审核中还发现过一次参数链断裂：TrainingRun 已保存追加参数，但 Controller 提交 Ray Job 时仍使用 Recipe 原命令。后来修正为提交最终 effective command，并通过实际训练验证。

### 4.5 Artifact 和共享存储

为了去除管理节点和固定目录依赖，我们定义：

~~~text
artifact://namespace/name/version
~~~

用于 source/model/data/output。

正式链路是：

- Head 通过 Artifact Gateway 下载；
- 校验 size、SHA256 和 manifest；
- 安全解包到 RWX PVC；
- Worker 读取同一份内容；
- checkpoint/output 写共享 PVC；
- output 作为不可变 artifact 发布。

当前还保留 workspace provider 作为没有 Gateway 时的兼容方式。我们明确：

- workspace 不是完整跨集群 artifact 闭环；
- /mnt/models 数据 hostPath 仍是可迁移性债务；
- Ascend driver hostPath 与数据 hostPath 是不同问题；
- 正式跨集群交付最终仍应使用 artifact + RWX PVC。

### 4.6 停止和 checkpoint

停止被收敛为 TrainingRun 声明：

~~~yaml
spec:
  suspend: true
  suspendMode: Immediate | AfterCheckpoint
~~~

Immediate：

- 先停止全部训练进程；
- 清理当前 attempt 未批准的 checkpoint；
- 保留已有可信恢复点；
- 再进入 Suspended 并删除 RayCluster。

AfterCheckpoint：

1. Controller 先写 Stopping 和请求 generation；
2. control ConfigMap 写 StopAfterCheckpoint；
3. runtime 记录当前一致 checkpoint 作为 baseline；
4. 等待所有 Worker 看到更大的 iteration；
5. 验证相同 checkpoint snapshot；
6. 停止所有 Worker；
7. Result 绑定 run UID、attempt、generation；
8. Controller 先持久化 checkpoint/Suspended；
9. 最后删除 RayCluster。

这也对应“第一个前向结束后、保存第一个 checkpoint 前停止”的测试要求：

- Immediate 应立即结束，不能留下可误恢复的半成品；
- AfterCheckpoint 必须等到请求之后的完整 checkpoint。

### 4.7 自动恢复

恢复策略同时做了两个方向的调整。

避免过度防御：

- 短暂 Kubernetes/Ray 网络错误不直接进入 ManualRequired；
- Profile/Recipe 暂未创建时等待；
- Ray 503、API 5xx 和连接中断按瞬态错误重试；
- 合同错误、权限、UID 冲突和 checkpoint 冲突才转人工。

避免过度自动化：

- 普通进程报错不能直接认定坏卡；
- 只有稳定、连续且完整的设备证据才允许换机；
- survivor 和 spare 都必须健康、空闲；
- 严格 N-for-N 替换；
- 新 attempt 重新生成 RankTable、重新 HCCL、重新检查 checkpoint。

恢复顺序也修正为：

~~~text
先持久化 Recovering/cleanup 意图
 -> 精确校验并删除旧 RayCluster
 -> 等待旧对象确实不存在
 -> 再创建新 attempt
~~~

这样避免两个训练 world 同时写 checkpoint。

### 4.8 发布和打包

本轮形成了 canonical stable 发布面：

- Python package；
- stable Helm Chart；
- 三个 CRD；
- Controller/Head/Worker 镜像定义；
- RBAC 和 PDB；
- examples/contracts；
- stable preflight；
- offline bundle；
- wheelhouse；
- image lock 和 SHA256SUMS；
- 安装脚本和回滚说明。

历史 Chart 和 legacy bundle 暂时保留用于迁移/回滚，但已经从 stable bundle 和主审计入口排除。

## 5. 重新审核发现并修复的问题

### 5.1 RankTable/HCCL 顺序死锁

启动曾一直等待 RankTable 文件。

根因是 HCCL pipeline 本身负责产生该投影，旧逻辑却先等文件、后启动 pipeline，消费者等待生产者。

处理为：

- 先执行 HCCL pipeline；
- 再等待最终投影；
- 比较文件字节 SHA256 与 HCCL 证据；
- 一致后才启动训练。

### 5.2 HCCL 入口和 native probe

遇到过：

- Python module 入口写错，导入但没有真正执行 pipeline；
- /opt/hccl-check/bin 与 /opt/kcc-hccl/bin 不一致；
- ARM64 probe 无法在 AMD64 发布机可靠构建；
- 容器缺少匹配 driver 动态库。

处理包括：

- 统一 module entrypoint；
- 统一 probe 路径；
- 在 ARM64 Worker 节点构建；
- 检查 ELF/ldd；
- 只读挂载 Ascend driver；
- HCCL 失败时停止全部 rank 进程组，避免残留占卡。

### 5.3 node rank 不能按列表猜

Profile 顺序、Pod 创建顺序、Ray node ID 和 ClusterD RankTable 顺序可能不同。

现在从 RankTable 的 rank_start 计算 node rank，并验证：

- 可被 devicesPerNode 整除；
- rank 连续且唯一；
- Worker 节点集合一致；
- world size 一致。

### 5.4 KubeRay 资源被多层转义破坏

资源 JSON 曾经过 YAML、KubeRay、shell 和 Ray 多次解析，空格/引号被破坏；Pod CPU request 与 Ray logical CPU 也曾混淆。

后来改用结构化 KubeRay 字段，并区分：

- Kubernetes CPU/内存；
- NPU resource request；
- Ray logical CPU；
- actor node affinity；
- 物理 NPU 分配。

### 5.5 no-progress 只看 rank 0

MindSpeed 的迭代日志可能由最后 global rank 输出。只看 rank 0 会把仍在训练的任务误判为卡死。

现在同时观察全部 node-rank 日志和 checkpoint tracker，任一 Worker 有新进度即视为全局有进展。

### 5.6 Ray submit 响应丢失

HTTP 超时不代表 Ray 没有收到任务。直接重提会造成重复训练。

现在：

- submission ID 与 attempt 确定性绑定；
- 超时后查询同一 ID；
- 验证 owner metadata；
- Dashboard 历史缺失但可信 Result 存在时，以 Result 为准。

### 5.7 checkpoint 误选半成品

扫描最大 iter 目录可能选中正在写的 checkpoint。

现在：

- 只跟随 latest_checkpointed_iteration.txt；
- 要求完整 shard 落盘后最后原子更新 tracker；
- 检查普通文件、symlink、文件稳定性和大小；
- 对各 Worker 的 sampled snapshot 做一致性比较。

### 5.8 Artifact 上传失败导致重训

训练 PASS 但 output 上传临时失败，不应该重新训练。

现在会：

- 单独记录 training success；
- 持久化 completion receipt；
- 后续验证 receipt 后跳过训练；
- 只重试确定性打包和发布。

### 5.9 Kubernetes 1.34 CRD 和 PDB

审核中修复：

- CRD 的 properties 与 additionalProperties 冲突；
- 大数组唯一性/CEL 表达不适合 K8s 1.34；
- PDB minAvailable 整数被 Helm quote 成字符串。

修复后的 CRD 已做 K3s 1.34.6 API server dry-run，PDB 保留真实类型。

### 5.10 镜像运行了旧包

基础镜像已有旧 KCC 包时，普通 pip install 可能认为依赖已满足。

发布镜像改为强制重装当前 wheel，避免镜像构建成功但实际运行旧 Controller。

## 6. legacy CLI 测试中处理的问题

### 6.1 kcc_ray check 报错

legacy check 不只检查节点可达，还会检查：

- NPU resource 数量；
- exporter/device health；
- 设备占用；
- 宿主或容器中的 NPU 进程；
- 节点和配置是否匹配。

因此“110.123.0.3 可用”不等于 check 必然通过。后续现场确实发现外部 Docker 占卡、DeviceInfo 网络账本和 A3 配置不匹配等问题。

### 6.2 指定 a3-server-00

节点选择被改成参数化：

~~~bash
kcc_ray start \
  --fresh \
  --node a3-server-00 \
  --expected-workers 1 \
  --run-id test1
~~~

其中 expected-workers=1 表示一个 Worker Pod/一个节点，不表示只使用一张卡。每节点卡数由运行配置/Profile 决定。

随后按要求把 A3 改为 8 卡测试，而不是在代码中固定 test1、节点或卡数。

### 6.3 为什么不能 stop test2

legacy 中逻辑 run-id、本地 Supervisor 状态和固定 RayCluster 名称曾经不是同一个权威身份。

如果失败发生在状态目录或提交记录建立之前，stop test2 可能找不到能够证明属于 test2 的资源。工具为了避免误删其他任务，会拒绝盲目删除。

工程化后的解决方式是：

- TrainingRun UID；
- attempt；
- ownerReference；
- run-id label；
- 确定性内部资源名；
- UID precondition delete。

### 6.4 fresh 启动早期停止

fresh 在 RayCluster 创建前后的部分等待阶段曾不能及时响应 stop。

我们补充了：

- 各阶段边界轮询 stop marker；
- startup 等待期间也响应；
- 退出码 130；
- 不删除 checkpoint；
- stop 后禁止 Supervisor 再起下一 attempt。

### 6.5 失败保留看起来像“卡住”

旧流程在错误后可能按 failedResourceRetentionSeconds 保留 RayCluster 供诊断，看起来像命令没有退出或资源没有释放。

工程化后将 phase、诊断保留、cleanup intent 和 terminal status 分开表达，不再用长时间等待隐含生命周期。

## 7. A3 与 Volcano 排障

### 7.1 accelerator-type=module-a3-16

本轮确认它是 Ascend Volcano/MindCluster 用来选择 A3 16 卡拓扑 handler 的标签，不是模型类型，也不是只通过 Gitea 更新代码就能自动修复的东西。

缺少该信息时，插件可能退回传统 8 卡 handler。节点实际提供 16 卡 topology，旧 handler 按最大 8 卡校验，于是报：

~~~text
node npu top<[...16-card topology...]> is invalid
~~~

准确含义是“调度器用了错误的拓扑解释器”，不等于 16 张 NPU 都坏了。

标签需要进入插件实际用于 handler 选择的 Pod selector/admission 输入。排障必须看 live Pod，不能只看 Node 标签或 RayCluster 模板。

### 7.2 CPU Head 的 skip 注解

~~~yaml
annotations:
  huawei.com/skip-ascend-plugin: enabled
~~~

它用于不请求 NPU 的 Ray Head：

- 告诉 Ascend admission/Volcano 不要把 Head 当 NPU task 校验；
- 避免 A3 handler 因 Head 申请 0 NPU 拒绝整个 gang。

它不解决 Worker 缺 A3 selector、RuntimeClass 缺失、HCCN 问题或物理卡占用。

### 7.3 同事 Head + Worker 被拦

用户提供的目录是：

~~~text
/home/admin/qwen38-diagnostics/
  volcano-ray-tp2-skip-head-20260825T034331+0000
~~~

核对结果：

- Head 已有 skip；
- 一个 Worker 请求 2 NPU；
- Head/Worker 缺少 accelerator-type=module-a3-16；
- Volcano 实际介入；
- Worker 被错误的 8 卡 handler 校验；
- 完整 16 卡 topology 被判 invalid；
- PodGroup 一个成员可调度、一个不可调度；
- gang minMember 未满足，Head 也一起 Pending。

所以主因不是 Head skip，也不是“Volcano 永远不支持 Ray”，而是 A3 handler 选择错误。

### 7.4 default-scheduler 与 Volcano 的历史

本轮反复澄清了“以前是否绕过 Volcano”。准确结论是两类实验都存在。

明确绕过：

- 2026-07-24 静态 0/1 定卡；
- 使用 default-scheduler；
- 停止宿主外部占卡后，NPU 张量 sanity 通过；
- 只证明静态设备和算子。

实际经过 Volcano：

- 2026-07-24 A3 两卡恢复；
- 2026-07-27 A3 八卡 HCCL + Qwen 训练；
- 2026-08-30 selector 修正后的 TP2。

前两个实验的模板虽请求 default-scheduler，但 admission 将 live Pod 改为 Volcano，证据明确记录 ADMISSION_MUTATED_TO_VOLCANO 和 bypassedVolcano:false。

因此不能说“以前全部绕过 Volcano”，也不能说“Volcano 从来没问题”。准确表述是：

- Volcano 在正确配置下可以工作；
- 它依赖 A3 handler、Head skip、设备账本和运行环境；
- 某些静态验证确实绕过过；
- 是否实际走 Volcano必须看 live Pod。

### 7.5 修正后的 TP2

2026-08-30 同类测试中：

- Head/Worker 都带 A3 selector；
- Head 保留 skip；
- Worker 分到 Ascend910-10/11；
- live Pods 的 schedulerName 为 volcano；
- PodGroup 达到 2/2；
- 两个 Pod Running。

它证明调度和容器创建恢复正常，但不能代替正式 HCCL/训练验收。

## 8. A3 的 HCCN 与 DeviceInfo

用户说明 A3 没接外部交换机，是单机节点。

此时：

- HCCN IP 可能为空；
- 外部链路为 down；
- DeviceInfo 把卡标为 NetworkUnhealthy；
- 反映的是外部 HCCN 数据面不存在。

这不一定妨碍一个 NPU Worker Pod 被 Volcano 分配，也不一定妨碍本机多 rank HCCL，因为单机通信可以使用 HCCS/PCIe。

但它意味着：

- 不能将该 A3 Profile 用于多机 HCCN 训练；
- 不能伪造 IP 让健康账本变绿；
- 单机通过不能证明多机网络。

MindCluster 的某些 NetworkUnhealthy 过滤路径取决于 NPU-requesting task 数，而不是单个 Pod 申请的卡数。一个 Worker 即使申请 2/8/16 卡仍可能只是一个 NPU task，所以单 Worker 场景能够调度并不矛盾。

长期建议是在 Profile 中明确区分：

~~~text
单机：intra-node / no external HCCN fabric
多机：inter-node / HCCN configured and verified
~~~

## 9. A2 多机验证

用户说明 A2 是 gpu-server-00/01 等节点后，我们重新核对了证据。

2026-07-29 两机链路：

- gpu-server-00/01；
- 每节点 8 张 910B3；
- 跨节点同设备 HCCN ping 0% loss；
- ClusterD 生成 server_count=2、world_size=16 RankTable；
- 16 rank 调用正式 HCCL API；
- HcclCommInitClusterInfo 和 AllReduce 全部 PASS；
- Qwen3-143M 两机 16 NPU 运行 2 iteration；
- 两个 Worker return code 均为 0。

这证明 A2 有真实跨机通信和训练 smoke，不只是手工注入 RankTable。

但它不等于长稳、性能基准、模型收敛或完整故障 SLA。

我们还发现旧 hccl_test.sh 实际使用：

~~~text
--nnodes=1
--master_addr=127.0.0.1
~~~

但 Python 输出硬编码了“2-node HCCL test passed”。所以不能以成功文案作为多机证据。真正证据必须包含实际 world size、跨节点 IP、RankTable 和逐 rank 结果。

## 10. Ascend RuntimeClass 与 K3s containerd

本轮遇到过：

~~~text
no runtime for "ascend" is configured
~~~

当时：

- Kubernetes 中有 RuntimeClass/ascend；
- 宿主安装了 Ascend Docker Runtime；
- K3s 实际使用的 containerd 配置只有 runc/crun；
- 设备分配成功；
- Pod sandbox 创建阶段失败。

这说明 RuntimeClass 对象存在，不等于 K3s containerd 已注册 handler。

长期路径二选一：

1. 在 K3s 实际 containerd template 注册 ascend，重启并逐节点实测；
2. 使用默认 runc + device plugin + driver/device mount，Profile 不设置 runtimeClassName。

如果 Profile 指定 ascend，handler 就是必要依赖；采用第二条路径时则不应硬编码 ascend RuntimeClass。

这是集群运维问题，不应继续在 KCC 中加隐式绕过。

## 11. PodResources socket

本轮还确认过 K3s kubelet 的 PodResources socket 与 Ascend 组件预期路径错配。

它可能影响：

- Pod 与物理设备 ID 的映射；
- device plugin/健康账本；
- admission 读取分配信息。

正确方案是：

- 找到 K3s kubelet root-dir 下的真实 socket；
- 对齐 DaemonSet hostPath/mount；
- 验证 gRPC ListPodResources；
- 写入集群 bootstrap。

临时 symlink 可用于验证，但不应成为 KCC 永久合同。它也不是 2026-08-25 topology invalid 的直接根因。

## 12. NPU 0/1 占用和双账本

A3 排障中发现：

- Kubernetes 认为设备可分配；
- 宿主独立 Docker /vllm 占用了多张 NPU；
- Pod 分配成功后，torch.npu.set_device 报 507033 Insufficient_Stream_Resources。

集群实际存在两套资源账本：

1. Kubernetes/device plugin；
2. K3s 之外的宿主 Docker/进程。

Kubernetes 无法自动知道第二套占用。运维上应禁止受管 NPU 节点运行绕过 Kubernetes 的长期容器，或显式隔离这些节点；preflight 也要同时检查 Kubernetes allocation 和宿主 NPU 进程。

## 13. Ascend Deployer 的自动化边界

本轮确认：

- MindCluster/Ascend Deployer 可以部署组件和下发已经确定的配置；
- 它不能凭空生成正确的 HCCN 网络规划；
- 运维仍需提供物理连接、IP、子网、VLAN、路由和交换机信息；
- A3 没接交换机时，Deployer 不能修复不存在的外部网络；
- A2 多机可以在网络规划明确后自动化配置和验证。

合理链路是：

~~~text
运维提供网络意图和地址规划
  -> Deployer/自动化下发
  -> hccn ping
  -> ClusterD RankTable
  -> KCC HCCL gate
  -> 训练
~~~

KCC 不应自行推测 HCCN IP。

## 14. 本轮梳理出的依赖

### 14.1 核心运行依赖

| 依赖 | 用途 |
|---|---|
| Kubernetes/K3s | CRD、Controller、Pod、ConfigMap、Lease |
| KubeRay Operator | RayCluster 到 Head/Worker |
| OCI Registry | Controller/Head/Worker 镜像 |
| Ascend driver/CANN/HCCL | NPU 运行和通信 |
| Ascend device plugin | NPU 资源注册和分配 |
| ClusterD/MindCluster | RankTable |
| RWX CSI/PVC | 共享代码、模型、checkpoint 和 output |

### 14.2 按功能启用

| 依赖 | 何时需要 |
|---|---|
| Artifact Gateway | 正式 artifact 输入输出和跨集群交付 |
| npu-exporter | 自动 N-for-N 硬件替换 |
| Volcano | 使用 gang/topology 调度时 |
| RuntimeClass/ascend | Profile 选择 ascend handler 时 |

### 14.3 平台集成依赖

| 依赖 | 用途 |
|---|---|
| Backstage | 用户入口 |
| Crossplane | TrainingRequest 到 TrainingRun |
| Argo CD | 静态控制面 |
| Tekton/CI | 测试、镜像和制品 |
| Gitea/GitHub | 源码和配置版本 |

平台组件不应成为训练 Worker 的直接运行时依赖。

## 15. 当前代码复核结果

本次总结前执行了正确测试入口：

~~~bash
PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_*.py'
make check
~~~

结果：

- 208 tests PASS；
- examples/contracts PASS；
- stable Helm Chart lint/template PASS；
- wheel 构建 PASS；
- stable static audit PASS。

之前直接运行 unittest 出现 ModuleNotFoundError:kcc_training，是测试命令遗漏 PYTHONPATH=src，不是代码回归。

make check 不连接集群、不构建真实 CANN 镜像，也不能替代 NPU/HCCL/存储验收。

## 16. 当前尚未收口的问题

### 16.1 版本漂移

当前：

- Python package：1.1.8；
- stable Chart：1.1.8；
- Python module version：1.1.8；
- README 仍写 1.1.3；
- STABLE 仍写 1.1.5；
- 分支名仍是 v1.1.4；
- Git tag 只有 v1.1.0～v1.1.3；
- 当前分支尚未完全同步到内部 Gitea/GitHub。

这是上传 GitHub/正式分发前必须先修复的发布问题。

### 16.2 旧发布面仍在

仓库仍有多个历史 Chart 和 legacy wrapper。虽然 stable bundle 只认一个 canonical Chart，但源码树仍显臃肿。

后续需要：

- 把仍被 stable 镜像使用的 HCCL 代码迁出 legacy bundle；
- 用 tag 保留回滚，不在主分支永久保留全部旧入口；
- 删除 deprecated Charts/wrapper；
- 保留一个明确安装入口。

### 16.3 数据路径兼容债务

MindSpeed 兼容仍可能挂载 /mnt/models。目标集群没有相同路径和内容时不能直接迁移。

最终需要用 Gitea source artifact、model/data artifact、RWX checkpoint/output 和 Artifact Gateway 完全替代数据 hostPath。

### 16.4 RuntimeProfile 更新语义

原设计希望 Profile 版本化不可变，当前代码允许原地更新，因此下一 attempt 可能看到不同镜像或节点池。

后续需要二选一：

- 恢复不可变 Profile，新配置用新名称；
- 或在 attempt spec 中冻结完整 Profile snapshot/digest。

### 16.5 目标集群验收

每个新集群仍要验证：

- 镜像架构和 digest；
- CANN/driver/torch_npu ABI；
- KubeRay；
- Volcano selector/Head skip；
- RuntimeClass/containerd handler；
- PodResources socket；
- device plugin；
- ClusterD RankTable；
- HCCN 或单机 HCCS 模式；
- RWX PVC；
- Artifact Gateway；
- HCCL；
- 短训练；
- checkpoint stop/resume；
- 故障恢复。

## 17. 本轮工作中最难的技术点

### 17.1 区分 KCC 和集群问题

同一个“Pod Pending/训练卡住”可能来自：

- KCC 状态机；
- KubeRay 渲染；
- Volcano handler；
- device plugin；
- containerd RuntimeClass；
- 宿主外部占卡；
- HCCN；
- RankTable；
- HCCL；
- 训练脚本。

必须沿 live 对象逐层定位，不能看到 Volcano event 就把所有问题归因于调度器。

### 17.2 建立可信身份链

需要同时绑定：

~~~text
TrainingRun UID
 -> attempt
 -> RayCluster UID
 -> Worker Pod
 -> Kubernetes Node
 -> 物理 NPU
 -> ClusterD server/rank
 -> Ray actor
 -> checkpoint/result
~~~

任何一段使用名字猜测或固定值，都会在恢复、停止或多任务环境中出错。

### 17.3 checkpoint 停止竞态

AfterCheckpoint 涉及 Controller、ConfigMap、runtime、多个 Worker 和训练 tracker。难点不是发送 SIGTERM，而是证明：

- 请求发生在 checkpoint 之前；
- 所有 Worker 看到同一个新 iteration；
- checkpoint 完整；
- Result 属于当前 generation/attempt；
- 状态落盘后才释放旧 world。

### 17.4 自动恢复的证据门槛

既不能把瞬态错误全部转人工，也不能看到普通训练报错就换卡。

需要同时证明：

- 错误类别允许重试；
- 旧 world 已完全退出；
- checkpoint 可信；
- 硬件故障证据稳定；
- spare 真实健康空闲。

### 17.5 Volcano 的隐式变换

RayCluster template 不一定等于 live Pod：

- KubeRay 会生成 Pod；
- admission 可能改 schedulerName；
- handler 根据 Pod selector 选型；
- gang 中一个 Worker 失败会让 Head 也 Pending。

所以必须看最终 Pod、PodGroup、events 和 allocation annotation。

### 17.6 兼顾当前可用和最终可迁移

直接删除 hostPath、legacy 和现场兼容会破坏已有能力；永久保留又无法真正工程化。

本轮采用：

1. 先建立 canonical API/Chart/Controller；
2. 保留旧入口作迁移验证；
3. 用单测和实机证据对齐行为；
4. 明确兼容债务；
5. 新路径完成 acceptance 后再删除旧实现。

## 18. 本轮工作的最终判断

通过本轮工作，KCC 已完成从“固定环境自动化脚本”到“声明式训练控制面”的主要结构迁移：

- 平台边界已经清晰；
- 参数和脚本注入链已明确；
- 主要硬编码已外置；
- Controller、attempt 和资源所有权已建立；
- HCCL、checkpoint、停止和恢复的关键语义已标准化；
- stable 打包和静态审计已通过；
- A2/A3、default-scheduler/Volcano、HCCN/HCCS、RuntimeClass 和 socket 问题已经被区分。

当前最需要完成的是：

1. 统一版本、tag、文档和远端发布；
2. 清理 legacy、多 Chart、hostPath 等迁移遗留；
3. 按目标集群完成可复现的 acceptance matrix。

完成这三项后，才能从“可以受控集成”进一步判断为“可以标准化跨集群交付”。

## 19. 关键文件

- [工程约束](../ENGINEERING.md)
- [渐进迁移 ADR](adr/0001-incremental-migration.md)
- [当前迁移状态](migration-status.md)
- [Controller](../src/kcc_training/controller.py)
- [v1beta1 API](../src/kcc_training/api_v1beta1.py)
- [RayCluster 生成](../src/kcc_training/raycluster.py)
- [Runtime coordinator](../src/kcc_training/runtime/coordinator.py)
- [Checkpoint 校验](../src/kcc_training/runtime/checkpoints.py)
- [TrainingRun CRD](../deploy/helm/kcc-training-stable/crds/trainingruns.yaml)
- [Crossplane/Backstage 集成方案](release/material-crossplane-integration-plan.md)
- [稳定发布入口](../deploy/helm/kcc-training-stable)
- [兼容性矩阵](compatibility-matrix.md)
