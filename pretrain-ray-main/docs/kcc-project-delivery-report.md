# KCC 分布式训练系统：完整项目推进与技术复盘报告

整理日期：2026-09-07。范围：两份历史总结、平台工作单及当前会话中可追溯的工作。

## 一、项目总览

KCC 是一整套面向 Ascend NPU 的分布式训练系统，不是某个 Kubernetes 对象，也不只是启动脚本或 Backstage 插件。项目负责人主导完成了训练架构设计及实现：从多节点训练启动、HCCL 通信验证、RankTable 注入，到 checkpoint 一致性、停止与恢复、备用节点接替，再到 Kubernetes 控制器、平台 API、可视化操作台、真实模型训练和训练中自动评测。

项目复用了 Kubernetes、Ray/KubeRay、MindSpeed、Ascend 软件栈，以及已有平台的 Crossplane、Backstage 等基础能力。KCC 自身负责把这些能力组织成可重复运行、可观察、可停止和恢复的训练生命周期。已有推理、数据和制品平台不是本项目重新建设的部分，集成过程中保留了同事的相关更新。

推进过程可以概括为四次转变：

1. 从“命令行能拉起训练”，转向“训练不依赖终端在线，并能识别故障、重建训练世界”。
2. 从“特定机器上验证过的脚本”，转向“由声明式 API、Controller 和明确的资源所有权管理”。
3. 从“后台能够执行”，转向“用户可在 Backstage 自助配置、提交、观察、停止、恢复和整理任务”。
4. 从“控制链路 smoke 通过”，转向“真实 MindSpeed 模型训练、真实 checkpoint、自动评测和训练指标接入”。

截至本文整理时，核心训练与平台链路已有多轮实际运行证据。最新 W&B 训练指标修复已完成代码修改和本地脚本检查，但未完成新的真实训练指标验收；正式镜像归档、GitOps 对齐和 CI/CD 收口仍是独立待办，不能把当前集成状态写成全部正式发布完成。

### 1.1 本报告与历史材料的关系

原文保留，不用新结论覆盖当时的现场记录：

- [旧版训练与恢复总结](kcc-ray-training-recovery-summary.md)：重点是旧版 kcc_ray、Supervisor、训练启动和故障恢复。
- [工程技术总结](engineering-technical-summary.md)：重点是稳定 API、控制器、硬件兼容和工程交付。
- [平台集成工作单](/home/ywj/Material/production/model-platform/training/WORK.md)：记录平台接入、真实训练和后续功能的逐次推进。

本文按项目主线重组这些材料，补入当前会话中的个人 W&B、真实目录、评测点诊断、loss 缺失定位和指标补齐工作。它不是其他不可见聊天的完整导出；没有记录支撑的结果不补写为已完成。

### 1.2 统一证据口径

| 容易混淆的结论 | 本报告采用的口径 |
| --- | --- |
| 60 步 canary 成功 | 证明训练控制、HCCL、checkpoint 协议等链路，不等于真实模型训练了 60 步 |
| RayCluster/Pod Running | 证明基础运行条件，不等于已产生训练 iteration/loss |
| 单卡 HCCL PASS | 单 rank 无跨 rank collective，不能代替多卡/多节点 HCCL 验收 |
| 故障后备用节点加入 | 区分拓扑重建、HCCL 重过、checkpoint 加载和恢复后迭代推进 |
| W&B 文件同步成功 | 不足以证明云端 history 已包含 loss |
| API/构建测试通过 | 不等于用户浏览器全流程或目标硬件长期稳定已验收 |
| 已直接部署 | 不等于源码、配置、Argo 和正式镜像发布状态已全部对齐 |

历史记录中的“当前版本”“已通过”和 TODO 均有时间范围；下文遇到被新证据修正的结论时会明确说明。

## 二、项目是怎样推进的

### 2.1 分阶段路线

| 阶段 | 当时要解决的问题 | 主要成果 |
| --- | --- | --- |
| 旧版训练内核建设 | 多节点训练怎样可靠启动、退出和恢复 | 六阶段启动、Ray Jobs、Supervisor、checkpoint 证据、N-for-N 恢复 |
| 7 月硬件与真实训练验证 | A2/A3 的设备、通信、环境是否真正可用 | 单卡/多卡及 A2 双节点实测，梳理 Volcano、HCCN、RuntimeClass 边界 |
| 稳定工程接口建设 | 怎样脱离特定主机与手工脚本交付 | v1beta1 API、Controller、状态机、Lease、stable Chart、合同与离线包 |
| 8 月平台控制链路接入 | 怎样由现有平台管理 KCC | TrainingRequest → Crossplane → TrainingRun，静态/动态所有权划分 |
| 8 月下旬标准链路直测 | 平台请求能否跑 HCCL、停止恢复和换机 | 3×8 卡 smoke、AfterCheckpoint/resume、两活一备、可重复 Runbook |
| 8 月 28—31 日操作台建设 | 用户能否自助使用，并看到真实过程 | Backstage 表单、模板、按钮、终端、阶段历史、归档与协作合并 |
| 9 月 1—2 日真实模型迁移 | 从 canary 走向已验证的 MindSpeed 训练 | 旧环境复用、真实 loss/iteration、参数透传、日志修复、GPU 节点存储 |
| 9 月 3 日功能扩展 | 训练时评测，以及批量串行复用资源 | LightEval opt-in、checkpoint 适配、详情曲线、dependsOn 串行任务 |
| 9 月 7 日个人化与指标完善 | 各用户能否看自己的 W&B，为什么没有 loss | 每任务 Secret、真实目录、entity 排障、指标写入修复、本地回归 |

阶段之间有交叉，不是写完全部代码再统一测试。实际方法是先明确架构边界，再通过用户正常入口运行，根据现场错误形成工程补丁、模板和工作记录。

### 2.2 贯穿全过程的取舍

- 保留已经验证过的训练主体。后期真实训练与 LightEval 接入尽量复用旧环境和命令，不为平台化重写训练算法。
- 先跑通标准业务链路，再补正式发布。按用户要求，不把权限反复核验、镜像正式归档或 CI/CD 建设持续前置。
- 修复进入工程。可重复 canary、镜像构建、HTTP registry 配置、恢复演练都有脚本或配置入口，不只留临时命令。
- 不扩张基础设施职责。暂不引入 Artifact Gateway，复用现有 KubeRay、Crossplane Function 等共享能力。
- 验收后释放资源，按要求清理测试 checkpoint，并保留任务与证据，使后续使用不依赖 AI 会话。

## 三、最终形成的架构与职责

### 3.1 控制链路与执行链路

用户在 Backstage 选择模板、节点、卡数和参数，提交 TrainingRequest；Crossplane 将平台意图映射为 TrainingRun；KCC Controller 生成本次 attempt 的执行配置和 RayCluster；KubeRay 拉起 Head/Worker；KCC 运行时组织 HCCL、RankTable、训练、checkpoint 和恢复；进度与结果再返回运行详情。

| 层次 | 对象/组件 | 职责 |
| --- | --- | --- |
| 用户入口 | Backstage 页面与 training-platform backend | 创建/操作请求，配置模板，展示过程、日志、目录和指标 |
| 平台 API | TrainingRequest、Crossplane XRD/Composition | 表达平台训练意图，传递参数，回传执行结果 |
| 训练原生 API | TrainingRuntimeProfile、TrainingRecipe、TrainingRun | 描述运行条件、训练配方及一次逻辑训练的期望与状态 |
| KCC 控制面 | Controller、TrainingRun.status、Lease | 协调状态机、attempt、停止恢复、清理、故障分类和备用替换 |
| Ray 基础设施 | RayCluster、KubeRay Operator | 维护 Head/Worker Pod、Service 等 Ray 集群资源 |
| KCC 执行运行时 | Head coordinator、Ray Job Driver、Worker actor | 预检、HCCL、rank 注入、torchrun、日志收集、checkpoint 协议 |
| 模型训练 | MindSpeed/Megatron、Ascend 软件栈 | 前向/反向、优化器更新、保存和加载训练状态 |
| 附属能力 | LightEval、W&B、共享 workspace | 评测、指标上报、输入输出和运行证据持久化 |

TrainingRun 本身也是 Kubernetes API 资源，不是一个隐藏的本地对象。TrainingRequest 是平台入口，TrainingRun 管理一次逻辑运行及其多个 attempt，KCC 则是包含这些 API、控制器和运行时的整个训练项目。

### 3.2 为什么保留 Request 和 Run 两层

平台需要统一项目、参数表单、操作动作与展示；训练内核需要管理 attempt、checkpoint、失败证据和恢复。两层分离，让平台不必直接理解训练内部状态机。

这不是 KCC 核心必须依赖 Crossplane：独立使用可以直接提交 TrainingRun，当前平台集成选择 TrainingRequest 为用户入口。推理侧的 ModelDeployment XRD 可以作为接入参考，但不能推导训练必须具有相同对象层数或生命周期。

当前字段也不同：TrainingRequest 使用 desiredState、stopMode；Composition 映射为 TrainingRun 的 suspend、suspendMode。操作说明不能混写两套字段。

### 3.3 为什么没有改成 RayService 或重写为 Ray Train

讨论过推理侧 RayService 与训练侧模型。推理偏向持续服务，而本项目核心是有限训练、checkpoint 提交、停止、恢复与故障后整个分布式世界重建，不能直接套用推理服务生命周期。

Ray 有训练相关能力，但改用 Ray Train 并不会自动解决 Ascend HCCL、RankTable、设备健康证据、备用机选择和既有 MindSpeed 恢复问题。因此本阶段保留 KCC 已建立的控制逻辑，通过 Ray Jobs/actors 执行，没有再引入一次训练框架迁移。

### 3.4 所有权划分

| 管理者 | 负责写入 | 不接管的部分 |
| --- | --- | --- |
| 平台 Git + Argo/Helm | 安装、CRD、Controller、RBAC、XRD/Composition、预置模板 | 不为每条用户训练创建 RayCluster，不管理其运行状态 |
| Backstage | TrainingRequest、另存的用户模板、获授权的每任务 Secret | 不直接修改 TrainingRun.status、RayCluster、Pod |
| Crossplane | 派生 TrainingRun metadata/spec、平台状态映射 | 不拉训练镜像，不运行训练，不争写训练状态 |
| KCC Controller/运行时 | TrainingRun.status、attempt 配置、进度/结果、RayCluster 期望 | 不安装或替换共享 KubeRay |
| KubeRay | RayCluster.status、派生 Pod/Service | 不决定 checkpoint 可恢复性或备用机替换 |

当前 Composition 引用已有 function-patch-and-transform 做资源字段映射。Function 不是训练执行器，也不是镜像拉取主体；镜像由节点容器运行时拉取。共享 Function、KubeRay 和相关 provider 在本集成中只引用，不重复安装、升级或删除。

用户新建模板由 Backstage 管理，预置模板由平台 Git 管理，避免同一模板落入两套管理链路。集成期授权的直接 apply/rollout 是阶段性手段，正式发布仍需收回 GitOps，不能把暂时无冲突等同于永远不会漂移。

## 四、旧版训练内核：从能启动到可恢复

### 4.1 整理工作区与厘清旧入口

初期梳理训练代码、脚本、日志、测试材料和环境目录，把非工程材料移出主工作区并保留归档，明确旧 kcc_ray 与新平台入口的关系。

后续 kcc_ray check 报错并不意味着入口被主动关闭。现场 traceback 显示它等待 kubectl 子进程时被 Ctrl-C 中断；另一条命令引用的归档 .venv/bin/activate 已不存在。解释器环境、历史路径和 Kubernetes 访问等待是不同问题，不能仅凭 KeyboardInterrupt 判断训练代码出错。

旧 CLI 保留为整机检查和历史兼容入口；平台不把它作为每次提交 TrainingRequest 前必须手工执行的前置条件。

### 4.2 六阶段启动

1. 检查节点、设备、环境与占用，确定活动/备用节点。
2. 渲染并创建 RayCluster，统一 Head/Worker 参数。
3. 等待实际 Pod、Ray 节点和 Worker 就绪。
4. 取得 ClusterD RankTable，整理/校验拓扑，执行真实 HCCL gate。
5. 固化训练模板，注入 master、node rank、world size、RankTable 与 checkpoint 配置。
6. 提交 Ray Job，由 Driver/actors 启动各 Worker 的 torchrun 和模型训练。

这样把“Pod 已启动”“通信已通过”“训练已开始”拆开，使失败能定位到具体阶段。

### 4.3 训练脱离终端

改造分两步：先以 Ray Jobs API 和确定性 submission ID 提交训练，再把外层恢复监督从本地进程移动到 Kubernetes Supervisor Job，避免终端断开导致控制链路退出。

Ray Jobs 不自动解决 Head 丢失后的 checkpoint 恢复；旧 Supervisor 仍使用固定管理节点上的 state.json，所以不是跨管理节点高可用。这也是后来迁移到 TrainingRun.status + Lease 的原因。

旧命令并非全部采用同一执行路径：默认恢复进入 Supervisor，历史 --fresh、--all-nodes 等模式仍有前台单 attempt 六阶段路径。不能把默认路径改善扩大成所有旧命令都已脱离终端。

### 4.4 RankTable 与 HCCL

多次实测暴露 rank 缺口、节点顺序和设备映射不一致。例如缺少 rank 16—23 时，不能只说 JSON 可解析；需要基于真实 ClusterD 设备/IP 映射整理连续唯一 rank，再校验 world 和节点对应关系。

随后修复两类工程错误：

- 把 Kubernetes 投影 ConfigMap 的正常符号链接误拒，导致 HCCL 已通过却进不了训练；调整投影读取，保留内容校验。
- 等待 RankTable 的消费端先于生成流程启动，形成等待闭环；调整生成、投影、消费顺序，并用 hash 关联证据。

真实通信 gate 用实际节点/rank 执行 collective。旧测试脚本中硬编码的“多节点成功”文案不能作为证明，必须看实际 world size、节点和返回结果。

### 4.5 checkpoint 与恢复的边界

恢复依据是框架提交标志 latest_checkpointed_iteration.txt 和其指向的 iter_XXXXXXX，而不是扫描最大的目录名。旧版检查 tracker 内容/哈希、iteration、文件存在/可读、相对路径和大小；稳定版进一步增加快照与控制代次绑定。这些机制不等价于所有大文件逐字节完整性校验。

KCC 恢复运行拓扑与提交协议，模型框架加载权重、优化器、学习率、随机数状态和 iteration。未提交 batch、进程调用栈和旧 communicator 不会原地恢复。

立即停止和 checkpoint 后停止是不同操作。后者记录 baseline，等待后续一致 checkpoint 再退出，不能在按钮一按下就杀掉训练。

### 4.6 备用机不是热插入原训练进程

恢复流程是：识别失败范围，确认可恢复 checkpoint，停止并清理旧分布式世界，保留健康节点、用备用节点 N-for-N 替换故障节点，重新构建 RankTable/HCCL，再加载 checkpoint。

软件错误、W&B 连接错误和短暂 API 故障不自动视为硬件损坏；同拓扑重试与硬件替换分别受预算控制。只有身份、归属或 checkpoint 证据无法确认时才需要人工处理，而不是所有异常都变为 ManualRequired。

### 4.7 典型困难与解决

| 问题 | 解决与边界 |
| --- | --- |
| 恢复路径错，找不到 iteration 547000 | 对齐真实 /mnt/models/00_TRAIN_RES/0717，不能把另一个短路径当有效输入 |
| 首次 backward 缺 ATB/CANN 环境 | 在实际训练入口加载已安装的 set_env.sh，不只检查宿主机目录存在 |
| 改变设备 allocatable 后恢复证据不足 | 统一失败身份、存活节点、checkpoint 和替换预算判断 |
| state.json 包含日志而超出 64 KiB | 改为有界摘要，详细结果外置，上限 1 MiB、有限 attempt，原子写入并 fsync |
| Ray submit 超时，不知道是否已创建 | 确定性 ID、查询和提交回执重连，避免盲目重复提交 |
| 用户停止与 actor 异常相近 | 按时间序列区分原因，不能用后发生的 stop 解释前面的异常 |

旧 qw3-10 验证活动 00/01、备用 03 切换为活动 00/03 并再次通过 HCCL，但没有证明恢复后 iteration 推进。qw3-jobs-1 则看到了真实训练由 547000 到约 547010，之后 actor 异常原因未闭环。不能把这两次证据拼成一次不存在的完整恢复成功。

## 五、硬件与运行环境：从配置正确到实际运行正确

### 5.1 A2/A3 兼容不是只换卡数

不同 Ascend 机型涉及设备数、Volcano 拓扑处理、设备插件、驱动挂载、CANN/torch-npu/算子包和网络能力，不能假设同一模板直接覆盖所有节点。

A3 的 accelerator-type=module-a3-16 会影响插件选择 16 卡处理逻辑；缺失时可能进入 8 卡处理分支并拒绝拓扑。CPU-only Ray Head 则需要 huawei.com/skip-ascend-plugin: enabled，避免不申请 NPU 的 Head 被 Ascend 插件阻断 gang。Head 修复不能替代 Worker 型号/拓扑修复。

还发现 admission 会把模板中的 scheduler 改成 Volcano。因此应查看最终 Pod 的 schedulerName 和事件，不能只看模板就断言绕过或使用了某个调度器。

### 5.2 物理卡、逻辑卡与实际分配

项目增加 physicalDeviceIDs 和前端选卡入口，同时梳理设备插件/Volcano 的边界。容器看到逻辑 Ascend910-0,1，不代表底层分配的就是物理 0、1；需要结合 huawei.com/kltDev 等实际分配证据。

已有并发验收明确证明两套独立 RayCluster/Pod 在不同节点集合同时运行，不是同一节点任意物理卡分区已验收。页面能勾选不等于所有机型、插件版本和调度模式都保证相同语义。

为继续集成，后续先使用三台 910B3 节点的完整资源，再做两活一备，没有让精确选卡问题持续阻塞真实训练。

### 5.3 基础设施问题及处理

| 问题 | 原因与处理 |
| --- | --- |
| 有 RuntimeClass，却报 runtime 未配置 | RuntimeClass 不等于 k3s/containerd 已注册 handler；明确注册 Ascend handler 与默认运行时加设备挂载两条路径 |
| device-plugin 读不到 PodResources | k3s kubelet 路径与插件 hostPath 不一致；对齐真实 socket/root，临时软链接不当作最终部署 |
| K8s 显示空闲，训练却报资源不足 | 宿主机 Docker/vLLM 等进程不在 K8s 资源账本里；结合宿主机占用检查 |
| 驱动目录存在，设备工具仍失败 | Worker 挂载匹配宿主机的 driver，按授权采用实际设备访问所需兼容配置 |
| ARM64 HCCL probe 构建困难 | 在目标架构节点 Pod 内编译后组装镜像，形成可重复构建脚本 |
| probe 路径错误或子进程残留 | 对齐模块入口、安装路径，透传 stderr，失败清理全部参与 rank 进程组 |
| 单机能过，HCCN IP 却为空 | 可能使用机内互联；不能证明多机网络可用，也不能伪造 HCCN IP |
| CANN/torch-npu/OPP 不匹配 | 通用镜像兼容单独跟进，真实模型先复用已验证软件组合 |

KCC 可以消费正确的设备/网络环境并报告错误，但不能凭空分配物理 HCCN 地址或修复交换机设计。

### 5.4 实际硬件证据

- A3，2026-07-24：双卡短训练、故障后从 checkpoint 恢复到后续 iteration；属于独立 trainctl/RayJob 路径，不冒充旧 kcc_ray 原路径。
- A3，2026-07-27：八卡 HCCL 与真实 Qwen3 模板 5 iterations，loss 约由 11.25 降到 10.56。
- A2，2026-07-29：gpu-server-00/01 各八卡，HCCN 连通、16-rank AllReduce 与真实 Qwen3 模板 2 iterations，Worker 正常返回。
- A3 后续 TP2/Volcano：正确 selector 下资源分配与 PodGroup 运行通过，范围限于该次调度验收。

这些不是所有卡持续健康、A3 任意多节点拓扑可用或月级无人值守稳定性的证明。

## 六、稳定工程化：从本地监督到 Kubernetes 控制器

### 6.1 三类原生 API

training.kcc.io/v1beta1 将运行条件和任务分离：

- TrainingRuntimeProfile：节点、每节点卡数、Head/Worker 镜像、调度条件、workspace 和外部集成。
- TrainingRecipe：框架、命令、默认环境变量，以及 source/model/data/output。
- TrainingRun：一次逻辑训练，引用模板，保存覆盖项、停止模式、恢复预算、依赖关系和执行状态。

per-run 覆盖逐步扩展到节点、备用机、卡数、镜像、完整 argv、追加参数、环境变量和制品，使用户改一次任务不必修改全局默认模板。

### 6.2 Controller 接管控制状态

稳定版用 TrainingRun.status 与 Lease 替代管理节点本地状态文件，按对象身份、resourceVersion、run UID、attempt 与控制代次协调运行、过滤旧结果，避免重启或重复 reconcile 导致重复执行。

主要阶段包括 Pending、Queued、Starting、Running、Stopping、Suspended、Recovering 和完成/失败态。每个 attempt 关联执行配置、RayCluster、progress/result，停止或完成后释放计算资源，同时保留证据。

双副本 Controller 与 Lease 改善控制进程接替，并不让模型进程变成透明高可用；训练故障仍需 checkpoint 和重建流程。

### 6.3 停止与恢复协议增强

AfterCheckpoint 保存控制代次和 baseline，等待更新且一致的 checkpoint，关联本次 run/attempt，先持久化认可状态，再释放执行资源，不是“看见一个目录就停止”。

Immediate 增加 StopImmediate → CheckpointCleanup → Suspended：首次运行清理尚未被控制器批准保留的 checkpoint；从已批准 baseline 恢复时，保留认可的恢复依据，清理本 attempt 新产生、部分写入或无法识别的内容。“未批准”也可能包含框架已写完但尚未被控制协议接纳的 checkpoint，不仅是半个文件。

AfterCheckpoint 保留恢复点；归档只整理记录；Immediate 清理又是另一项操作，三者不能混淆。

### 6.4 处理分布式系统的模糊结果

- K8s/Ray 短暂 5xx、对象暂未出现、首次环境准备，不全部判成不可恢复业务失败。
- 提交超时先用确定性 ID 查询已有任务，避免一个请求执行两次。
- 旧 RayCluster 清理完成才创建下一 attempt，避免旧/新进程共同写 checkpoint。
- 训练已成功、仅输出 artifact 发布失败时，凭完成回执重试发布，不重新训练。
- 覆盖参数不仅要写进 spec，也必须进入最终 Ray submit。019 暴露“参数已保存但未执行”，修复后用 020 正常请求验证。

### 6.5 制品和可交付性

工程层建立 artifact://namespace/name/version 合同、manifest、大小/hash 校验、解包与输出回执。稳定发布面收敛到 deploy/helm/kcc-training-stable，形成 wheel、依赖、镜像锁、校验和、安装/预检脚本和离线 bundle。

实际集成先采用 workspace artifact 模式，不强制安装 Artifact Gateway。Gateway 是制品访问后端的一种选择，不是必须横在 Backstage 和 KCC 之间的业务入口。

真实 MindSpeed 兼容配置仍使用既有 /mnt/models 代码和数据，这是移植边界；有 artifact URI 不等于训练输入已经完全可移植。

### 6.6 工程交付修复

处理过 K8s 1.34 CRD schema/CEL 兼容、PDB 数值渲染、KubeRay 参数多层转义、镜像内旧 KCC 包未被真正覆盖、目标架构构建和脚本路径等问题。检查扩展到 CRD server dry-run、Helm lint/template、合同、wheel 和实际镜像入口。

旧 Chart 保留在源码中，但不进入 stable bundle。README、STABLE、包、Chart、分支名曾发生版本漂移；例如旧 STABLE 仍写 RuntimeProfile 不可变。当前以实际 API/源码与部署记录判定行为，正式版本口径统一仍需收口。

## 七、平台直连：用正常入口验证核心功能

### 7.1 先验证架构，正式发布随后

按用户要求，先落地 XRD/Composition、KCC 安装和模板，确认 Request 能生成 Run，再逐步接入存储、设备和执行。初始 Argo OutOfSync/Missing 表示资源未同步/创建，不是训练代码已经失败。

权限、NFS export、镜像和设备挂载按实际错误解决，没有把完整发布平台持续前置。运行验收尽量只操作 TrainingRequest，不临时绕过控制链创建训练 Pod。

### 7.2 workspace 与 Ascend Worker

最初缺 RWX PVC 和制品通路，采用 kcc-training-workspace + workspace artifact。NFS export 从初始节点扩展到实际训练节点范围，修复服务端配置目录和导出，不要求用户在每台 Worker 上复制数据。

随后处理驱动访问、逻辑设备映射、资源渲染与 HCCL stderr 隐藏，按授权采用 Ascend 兼容运行配置。测试从初始限定卡位调整为三台 910B3 全卡，随后收敛到两台运行、一台备用。

### 7.3 三项关键验收

| 请求 | 内容 | 结果与边界 |
| --- | --- | --- |
| kcc-910b3-3n8c-016 | 三节点八卡、HCCL、RankTable、控制与输出 | 60 步 smoke 成功、checkpoint 60 一致、三 Worker 返回 0；不是 MindSpeed 模型证明 |
| kcc-910b3-3n8c-017 | AfterCheckpoint、释放、标准 resume | baseline 48 后选 51 停止，attempt 1 从 51 到 60；checkpoint 按要求删除、记录保留 |
| kcc-910b3-2n8c-spare-018 | 两活一备、N-for-N、重建后继续执行 | checkpoint 10 后识别故障、备用接替，attempt 1 到 60，replacement=1，最终成功 |

018 的硬件健康证据通过 exporter 接口边界的验收模拟器注入，没有主动损坏 NPU。RayCluster 重建、HCCL、checkpoint 和接替运行是真实链路；模拟器随后删除，恢复真实健康来源。

成果落为 training-canary.sh、replacement-canary.sh、TrainingRequest 清单和 Runbook，离开 AI 会话也能重复执行。

## 八、Backstage：把训练能力变成可操作的产品

### 8.1 从占位仓库到前后端工程

Material 是仓库名，Backstage 才是前端。最初工作树并非完整应用源码，因此从线上不可变镜像及 source map 恢复可构建工程，加入 training-platform backend、页面和 Scaffolder 模板，再上传内部 platform-backstage 仓库。

同事上线推理生命周期、模型健康等更新后，以实际线上版本为基线合并 KCC 训练部分，保留推理、数据、制品等模块，不用整份旧训练镜像覆盖。个人 W&B 版本也基于同事线上 2a64524，没有顺带带入其未上线的后续迁移。

### 8.2 创建训练与模板能力

表单逐步具备：

- 项目、RuntimeProfile、TrainingRecipe、实际训练脚本查看。
- 八台 gpu-server 活动/备用选择；默认活动 00/01/02/03/05/06，备用 07/08。
- 每节点卡数、可选物理卡、Head/Worker 镜像、完整命令、追加 argv。
- 常用环境变量、source/model/data、输出子目录、恢复预算、无进展超时。
- HCCL-only、立即运行或先停止、串行任务数、自动评测、个人 W&B。

模板不是固定下拉框：用户可以打开默认模板完整 YAML，直接在其内容上修改并另存新名称；新模板可发现和删除，默认模板前后端都禁止删除。

最初 Profile/Recipe 均采用不可变设计。用户要求已有运行模板继续可修改后，移除了 RuntimeProfile.spec 不可变限制，保留稳定 v4 名称；TrainingRecipe.spec 仍不可变，不能把 Profile 的改变推导为所有配方字段也可原地修改。

per-run 覆盖、另存模板、修改可变 RuntimeProfile 是三种操作。用户模板与平台预置模板所有权不同；可变 Profile 对恢复可复现性的影响作为后续改进，不重新增加阻碍使用的限制。

### 8.3 生命周期按钮与收纳箱

主要动作均提供入口：新建 HCCL/训练、运行/恢复、Checkpoint 后停止、立即停止、HCCL/RankTable/日志/checkpoint/输出证据、复制配置和调整下一任务备用机。

归档通过 TrainingRequest annotation 实现，主列表隐藏，收纳箱可展开、查看、移出。不删除请求、运行对象或证据；归档也不等于停止训练。计算资源由停止/完成流程释放，磁盘由保留策略管理。

### 8.4 真实用户操作暴露的问题

“request must be json”来自 Router 没注册 JSON body parser，不是用户需要手工改请求；补解析器后继续验证真实页面创建和停止。

状态展示改为优先读 TrainingRun 最新执行态，同时保留 Request 镜像状态，避免 Crossplane 同步延迟导致页面一直显示旧阶段。

### 8.5 并发能力范围

train-0901015912 与 train-0901015929 通过 Backstage 同时创建，两套 RayCluster 分别在 gpu-server-00/01 与 02/03 运行并进入各自 16-rank HCCL；用户停止后，两条 Suspended，RayCluster/Pod 清零。

证明的是不同节点集合独立并发、控制与清理互不影响，不是同节点物理 0/1 与 2/3 分区实测，也不是后来串行批量功能会同时占卡。

## 九、可观测性：解决“看起来一直卡住”

### 9.1 从状态字到完整过程

Starting、PodInitializing、空 HCCL、Ray 的 block 日志，可能对应镜像拉取、init container、runtime env 准备、HCCL 执行、网络读取失败，也可能只因为没有读取训练进程真正输出的位置。

因此增加结构化 progress、阶段历史、Ray Job/Driver 与训练 stdout/stderr。控制台拆成“过程、训练输出、Ray Driver、Pods、事件、容器日志、原始输出”七个视图，覆盖 HCCL、RankTable、Ray Worker、preflight、Training、checkpoint 清理和终态。

### 9.2 逐次定位与修复

| 用户现象 | 实际问题 | 工程修复 |
| --- | --- | --- |
| logs 媒体类型错误 | 错发 Accept: text/plain | 按 Kubernetes 接口接受类型请求 |
| HCCL 是 {} | 只读完成态 result | 读取 live progress/阶段证据，保留最终结果 |
| Ray 显示 block forever | 守护进程输出，不是训练进度 | 增加 Ray Jobs Driver 和训练 stdout/stderr |
| 刷新强制滚到底 | 自动刷新与跟随耦合 | 上滚停止跟随，独立刷新，提供跳到最新 |
| HCCL 历史被训练心跳覆盖 | progress 只存最新值 | 有界阶段 history，连续心跳合并 |
| 真实训练输出不出现 | Worker 仅写共享文件 | Head 增量读各 node rank 日志，转为 KCC_TRAINING_OUTPUT |
| Driver 有内容，页面仍为空 | 前后端字段兼容 | 标准 trainingOutput 优先，原始 Driver 解析兜底 |
| Ray 日志 HTTP 500 | 默认拒绝策略未放行 Head 8265 | 补必要 egress，Service 优先，detail/log 独立容错，proxy 兜底 |
| 有训练进展却 NO_PROGRESS | watchdog 只看 node-rank-0，MindSpeed 在最后全局 rank 打印 | 观察整组共享日志及 checkpoint tracker |

修复分别落在运行时、Controller、backend、frontend 和平台网络层，没有把所有日志缺失都归咎于训练。

### 9.3 真实训练帮助区分展示故障和执行故障

train-0901085522 已在两台八卡 Worker 上真实训练到 iteration 720，loss 约由 11.34 降至 8.30。首个 checkpoint 计划在 1000，旧 watchdog 却因只看 rank 0，600 秒后误判并终止。它没有生成首个 checkpoint，不能在空目录寻找“已保存的 720”。

train-0902020400 已有共享日志与 29 条结构化输出，检查时到 iteration 320、loss 约 9.276，故障在前端。train-0902023646 已连续保存 33、36、39 等 checkpoint，但 Backstage 因网络读取失败仍显示等待；之后用户 Immediate 停止，清理未批准条目并释放资源。

由此形成的诊断方法是交叉对照期望状态、Pod、Ray Job、阶段进度、iteration 和 checkpoint。初始化/算子编译期间暂时没有 loss 可以解释，但不能用这一句话永久掩盖错误。

## 十、真实 MindSpeed 迁移、参数与存储

### 10.1 从 smoke 进入真实模型

用户询问 train-0901063240 的 checkpoint 60 和空日志，推动项目明确：前期 60 步任务主要验证训练控制协议，不能满足真实模型需求。随后以旧版 ray_startup_bundle/training_templates/pretrain_150M.sh 为模板迁移，不再用 canary 代替模型工作负载。

继续采用 MindSpeed pretrain_gpt.py、原有模型结构、Tokenizer 和数据组合。Head 复用 Python 3.10/Ray 2.49 基底，Worker 复用 CANN 8.3、torch/torch-npu 2.7 环境，只叠加 KCC 运行时与 HCCL probe。

分布式启动由 KCC 管理，模板不重复嵌套 torchrun；外层启动前加载镜像实际 CANN/ATB 环境，解决训练进程库不可见。

kcc-mindspeed-150m-canary-004 经标准 TrainingRequest 完成真实 1 iteration，产生约 2.7 GiB checkpoint。这与几十字节的 smoke checkpoint 是不同层级的结果。

### 10.2 v4 PATH 和默认模板选择

train-0902032242、train-0902062255 的 v4 Worker 在 wait-gcs-ready 报 ray: command not found，不是 HCCL 卡住。构建流程改为从 KCC_WORKER_PYTHON 推导真实 Ray bin，在构建前后验证可执行文件与 PATH。

补回 Backstage catalog 对 v4 的发现，避免运行模板已更新而页面退回旧 v3。kcc-mindspeed-v4-path-smoke-001 后续通过 GCS、HCCL、RankTable、preflight，完成真实训练和单个 checkpoint，并释放资源。

### 10.3 让填写的参数真正生效

分别排查三层：表单是否提交、Request/Run 是否透传、shell/框架最终 argv 是否采用。处理过独立 UI 字段覆盖环境变量、旧页面缓存、脚本硬编码/派生参数覆盖，以及有效命令没有真正提交等问题。

按用户要求删除独立“Checkpoint 保存间隔”输入框，统一在环境变量区修改，不让两个入口争写 SAVE_INTERVAL。

| 参数 | 用途 | 注意事项 |
| --- | --- | --- |
| TRAIN_ITERS | 框架目标总 iteration | 恢复时不是额外训练步数 |
| SAVE_INTERVAL | 每隔多少 iteration 保存 | 不是保留文件个数，也不是评测耗时控制 |
| EVAL_INTERVAL | 框架内部 validation 间隔 | 不是 LightEval checkpoint 触发开关 |
| EVAL_ITERS / DATA_SPLIT | 内部 validation 批次数、数据划分 | 最新脚本可覆盖，默认不增加 validation |
| LOG_INTERVAL | 终端训练日志间隔 | stdout 有 loss 不保证 W&B 有 history |
| METRICS_INTERVAL | 最新补丁的 TensorBoard/W&B 指标间隔 | 与 stdout 间隔分开，不改变训练算法 |

从零开始 TRAIN_ITERS=3、SAVE_INTERVAL=3，正常完成保存点可得到 iteration 3 checkpoint；每两步保存、三个保存点后结束可设目标 6。恢复场景需结合已恢复 iteration 和保存边界计算，不能机械认为每次从零开始。

### 10.4 将输出移到 GPU 服务器

最初 /workspace 对应控制机 /mnt/data/kcc-training-workspace，按用户要求改为 v4 统一挂载 gpu-server-00:/mnt。容器内仍用 /workspace，不改变训练和恢复的存储接口。

outputSubpath 相对 GPU 服务器 /mnt，可选择已挂载且可访问的子目录，例如 sas-server-0/kcc-training/test；KCC 再追加隔离运行目录及 checkpoints。不同 /mnt 子目录可能来自不同盘或挂载，改输出地址不会自动挂载新磁盘。

典型外部输出是 gpu-server-00:/mnt/sas-server-0/kcc-training/test/<任务目录>/<运行隔离目录>/checkpoints。输入仍可来自活动 Worker 的 /mnt/models/DATA_BIN/GEN，数据与输出位置分别显示，不混淆 server-00 和 gpu-server-00。

Backstage 先做终端路径映射，再从当前 run UID 的最新 attempt 配置读取真实数据/checkpoint 根目录。已知 /workspace 给外部映射，已知 hostPath 标注实际 Worker；未知挂载不冒充宿主机目录。

### 10.5 checkpoint 保留和资源占用

测试输出按要求放 test，验收后保留指定 checkpoint 或删除指定产物，没有继续写旧训练保存目录。“只保存一个”是具体短训练/清理策略，不等于所有任务都有全局恒定保留一个的通用机制。

停止后 RayCluster/Pod 释放，不再占用该任务 NPU/显存；checkpoint、日志、TensorBoard/W&B 本地文件和评测输出继续占磁盘。收纳箱不会自动清理持久数据。

## 十一、训练中 LightEval 自动评测

### 11.1 保留训练主体的接入方式

用户已有 110.129.0.20:/home/ywj/lighteval 的单卡 Docker 评测方式。接入以 README local 与实际命令为依据，新增独立 LightEval Profile/Recipe 和 source-v2，初次集成不改稳定默认训练输入，评测默认关闭。

不新增训练控制器，也不为每个评测点创建 RayCluster。现有 Ray Worker 的 rank 0 StructuredWorker 在一致 checkpoint 提交后启动独立 LightEval 子进程，关闭时保持 no-op。

已验证边界是单进程、单 NPU、TP=1、PP=1。独立子进程与设备配置不等于独占的 Kubernetes NPU 配额，与训练的算力/显存竞争仍取决于实际配置；“通常不占网络带宽”也不等于模型加载没有存储 I/O。

### 11.2 checkpoint 适配

保留 Megatron 分片和 tracker 提交语义，补配置与 Tokenizer 等元数据，让评测读取训练 checkpoint，不另造权重保存格式。

只在完整 checkpoint 提交后触发，避免读到写入中目录；按当前配置，评测失败可继续训练。这与 W&B online 初始化失败是否影响训练是不同策略。

### 11.3 真实验证与详情曲线

kcc-mindspeed-v4-evaluation-smoke-001 通过标准链路完成真实 3 iterations，形成 iteration 2/3 checkpoint，两次评测 PASS，耗时约 90.8/90.2 秒。该次参数用于证明训练与评测重叠；保存下来的后续短验收模板改为 SAVE_INTERVAL=3，只生成最终 checkpoint。

日志、原始结果、summary 和趋势保存在本次 evaluation/；status/ConfigMap 保存紧凑指标。iteration 2 checkpoint 后来删除，历史曲线仍保留，模型文件和评测历史分开管理。

右侧运行详情提供任务/指标选择，优先 acc_norm/acc；按 iteration 合并，同一步恢复后由新 attempt 结果覆盖。只读选中任务详情，运行中刷新，防止切换任务后旧响应覆盖；空数据、等待、刷新失败各有提示。

### 11.4 为什么评测点比 checkpoint 少

train-0907042953 设置 SAVE_INTERVAL=3，评测依次完成在 iteration 3、9、15，后来还有 21。不是 UI 只支持两个点，也不是每次保存失败。

协调器采用“一个正在执行 + 一个待执行最新点”。单次评测约 100 秒，其间训练已保存多个 checkpoint，pending 更新为最新值，中间 6、12、18 等可能被合并跳过。EVALUATION_EVERY_CHECKPOINTS=1 不等于保证逐个执行的 FIFO 队列。

若要求每个 checkpoint 都评测，需要单独实现 FIFO、checkpoint 引用保留、积压处理和训练结束时 drain；当前不能声称已具备。

### 11.5 为什么 acc_norm 一直 0.375

该次只评 8 个样本，现场日志确认各 checkpoint iteration、对应权重和 8/8 推理，不是重用同一个缓存结果。原始准确率 0.625、归一化准确率 0.375，分别对应 5/8、3/8。

分数步长为 0.125，邻近 checkpoint 同分不异常，不能据此判断训练没有学习。扩样本/任务集可改善代表性，但会增加耗时和积压；与“每个 checkpoint 必须评测”的队列要求应分别处理。

历史事件有保留上限，不是无限保存全部高频数据。长期分析应以持久评测输出/指标系统为依据。

## 十二、批量串行训练：复用同一组资源

用户明确只要串行：一次提交多个任务，前一个结束才运行后一个。没有新增 Batch CRD 或调度器，而在 TrainingRequest/TrainingRun 增加可选 dependsOn。

Backstage 生成 base-01..NN 线性链，后一个依赖前一个。KCC 只在 Pending/Queued 检查，必须前序 Succeeded 且旧 RayCluster 消失才开始下一条。前序失败/停止，后序保持排队，不自动跳过，不重排已启动任务。

创建中途失败保留已创建请求并报告完成数量，不自动回滚。batch-id/index/size 作为 annotation，单任务不填 dependsOn 时原路径不变。

kcc-serial-0903064242-01/02 在 gpu-server-00 同一两卡配置下，先 Starting/Queued，再前序成功、后序等待清理，旧集群消失后第二条启动；最终两条成功、RayCluster 清零。checkpoint 只有 44 bytes，证明串行和资源交接，不冒充真实大模型批量训练。

该训练依赖链与 LightEval 的 pending 最新点机制不同，不能把评测跳点解释为训练任务重排。

## 十三、W&B：从连通到每个用户真正看见指标

### 13.1 早期接入与出网

旧训练已有 W&B 待办。接入后分别处理 DNS/Pod 出网、代理可达、Key 认证和平台链接，不把所有错误归为账号权限。测试期代理/relay 与正式统一出网是两个阶段，后者仍是 TODO。

W&B 是可选启用能力，但现有 online 脚本初始化失败仍可能导致训练失败；不能声称已经实现任意异常自动降级 offline 的完整机制。

### 13.2 每任务个人账号与凭据

新建表单增加开关、个人 API Key、entity、project，以训练名为稳定 run ID，详情生成对应链接。用户可选自己有权限的 entity，不必固定进入某个团队；但 entity 必须存在且能写入，用户名、组织显示名、团队 slug 不能混用。

Key 只放本次 <训练名>-wandb Secret，不写普通环境覆盖、spec ConfigMap、详情响应和 Git。Secret 关联 TrainingRequest，表单提交/关闭清空，复制配置需重新填 key；共享 Secret 的旧路径保留兼容。

经用户明确授权，Backstage 获得命名空间内 Secret create，不扩到读取/列举/修改/删除。创建顺序为 Stopped Request → Secret → Running，凭据失败时保持停止。没有额外引入网关或复杂审计层。

个人 W&B 与真实目录版本已直接部署，KCC b41a23b、Backstage c937370；Controller 2/2、Backstage 1/1 Ready 是当时记录，不是本文重新巡检结果。

### 13.3 空页面/404 的实际原因

- 某任务 Key 认证成功，但 entity 指向组织标识，服务拒绝直接向 organization 写入，目标 run 未创建。
- 改用另一个用户名样式 entity 后返回 entity not found；Key 有效不代表任意 entity 都可用。
- 使用用户给出的实际 team/entity slug 后，后续任务成功创建 W&B run；前面部分任务在 HCCL/初始化被停止，本就没有训练历史。

项目可在有权限的首次初始化时创建，但 entity 不能任意填写自动生成；详情链接存在也不代表远端已有数据。

### 13.4 连上了却没有 loss：本轮最新定位

读取 train-0907042953 的 W&B run 发现只有 system 类历史、summary 为空，配置 use_wandb=true 但 tensorboard_dir=null。stdout 有 loss，不代表框架执行了 wandb.log。

对照实际 MindSpeed 源码，训练指标写入位于 if writer 条件内：该版本 W&B loss 分支依赖 TensorBoard SummaryWriter。旧训练脚本有 --tensorboard-dir，迁移模板漏掉，导致能建立 run 却没有训练指标。

根因不是 Backstage 链接或浏览器权限，而是模板未满足框架写指标的条件。早期“stdout 有 lm loss，且同步了 5 个文件，所以 loss 已在线同步”的结论证据不足，在此修正。

### 13.5 指标补齐具体修改

修改集中在配置仓库脚本及 Recipe 内嵌副本，不改稳定训练主体：

1. W&B 启用时设置 TENSORBOARD_DIR，默认本次隔离输出目录的 tensorboard 子目录。
2. 增加 METRICS_INTERVAL，默认 10，通过 --tensorboard-log-interval 控制指标间隔。
3. 在实际训练环境检查 torch.utils.tensorboard.SummaryWriter，缺依赖给明确错误，不静默产生“有 run 无指标”。
4. 补 timers、world size、validation ppl 的 TensorBoard 相关开关，保留原训练参数和 LOG_INTERVAL 默认。
5. 暴露 EVAL_ITERS、DATA_SPLIT，默认 0 和 100,0,0，不自动增加 validation。
6. 对齐 source/source-v2 和 Recipe v2/v3 四份脚本，避免展示与执行不是同一份。

Recipe.spec 仍不可变，本轮没有绕过该限制。改用脚本默认值、更新注解脚本，未更换 Controller/Head/Worker 镜像，也未改 HCCL、checkpoint 格式或备用机策略。

### 13.6 训练关注的数据与边界

| 指标 | 获取/展示 | 目前的结论 |
| --- | --- | --- |
| iteration、总步数、进度、ETA | 日志解析进入 Backstage | 有真实输出证据；ETA 为估算 |
| loss、学习率 | 原生日志与 W&B 分支 | 日志已验证；新补丁的云端连续上传待验收 |
| grad norm、loss scale、batch size、consumed samples | 框架输出与原生指标 | 依赖实际输出；BF16 loss scale=1 不自动表示异常 |
| iteration time、timers、吞吐 | 日志及补充的记录开关 | 吞吐为估算 TFLOP/s/device，不是 tokens/s；时间可能是窗口均值 |
| world size | 运行配置、补充开关 | 解释拓扑/性能，不等于节点健康 |
| validation loss / ppl | 需要启用内部 validation | 默认关闭；本次 PPL 开关面向 TensorBoard，不宣称全部已进 W&B |
| PIQA 等评测分数 | LightEval 与右侧曲线 | 与训练 loss 是不同通路 |
| NPU 利用率、显存、温度、设备错误 | Pod/节点/设备监控和 exporter | 本次脚本补丁未自动把它们全部接入 W&B |

没有默认开启可能增加额外归约的参数范数/零梯度数量；stdout 的 NaN/跳步信息也没有被描述成已完成的 W&B 告警体系。

### 13.7 验证状态：按要求不再实测

新增 9 项本地脚本回归，覆盖四份副本一致性、shell 语法、开关、含空格路径、指标间隔覆盖、缺依赖/缺 key 和 validation 默认参数，结果通过；属于 mock/脚本检查，没有模型执行或 NPU 占用。

为指标验收准备的 kcc-wandb-metrics-0907-001 在用户要求停止实测后已请求 Immediate 停止并进行资源清理、归档处理，不作为上传成功证据；之后只做简单检查。

可确认“已定位源码原因并补齐脚本”，不能确认“新真实训练的 loss 曲线已经验收”。四份脚本变更、测试与 smoke 清单仍是配置工作树待提交内容，不因 KCC 已推 GitHub 就自动全部发布。

## 十四、源码协作、镜像与交付链路

### 14.1 三个主要工程面

| 工程 | 作用 | 本轮相关状态 |
| --- | --- | --- |
| /home/ywj/kcc | 控制器、运行时、旧 CLI、API、Chart、测试、文档 | 记录基线 b41a23b；GitHub cerottas/kcc main 已推进至此 |
| /home/ywj/backstage-model-platform | 页面、backend、模板、平台集成 | 个人 W&B 版本 c937370，保留同事线上功能 |
| /home/ywj/model-platform-config | 静态配置、Profile/Recipe、canary、操作脚本 | 最新指标脚本/测试仍有未提交修改，正式分支与 Argo 待对齐 |

Material 工作单是集成记录，不是一个新的运行时组件。

### 14.2 发布身份和代码推送

日常发布优先使用 Gitea release-bot 与 Artifact Keeper 服务账号，凭据不进 Markdown。初期目录访问、ACL 工具缺失和仓库权限问题按授权处理；Backstage 机器人权限与自动发布仍需正式收口，不把一次管理员授权当永久设计。

GitHub 曾受 SSH 22 端口不通、443 可连但 deploy key 无写权限影响。当前会话已使用已有授权账号通过 HTTPS，将 cerottas/kcc main 从 d1be431 推进到 b41a23b，并非必须强制覆盖远端。历史“尚未推送”按此更新，但新指标配置和本报告不会自动随之上传。

### 14.3 HTTP registry 与大镜像

train-0831071232 的 PodInitializing 最终定位到部分 GPU 节点把 HTTP Artifact Keeper 当 HTTPS 请求。合并保留旧配置，八台节点补 30670 HTTP mirror/hosts，逐台处理 agent，实际拉取通过；可重复入口为 configure-worker-registry-http.sh。

真实训练旧 Worker 基底另含约 17.4 GiB 单层，30670 上传出现 HTTP 409/session 问题。为不阻塞实验暂用已有 8889 测试仓库，正式归档留 TODO。Controller/Backstage 已用 30670，不代表所有 Head/Worker 都已迁移。

### 14.4 检查与验证

工程检查覆盖 API/合同、状态机、停止恢复、渲染、序列化、日志、评测历史等。历史工程总结记录过 208 项测试及 stable 检查通过；个人 W&B 阶段有 KCC 41 项定向测试、Backstage 4 项凭据/目录测试与构建通过；最新补丁是 9 项脚本回归。

不同版本和范围的数字不能相加后宣传为当前一次全量验收。本文没有重新跑训练、部署或全量测试。

## 十五、关键验证证据索引

| 场景 | 记录 | 确认结果 | 未覆盖范围 |
| --- | --- | --- | --- |
| 旧版备用替换 | qw3-10 | 拓扑替换、再过 HCCL | 恢复后真实 iteration 推进 |
| 旧版真实训练 | qw3-jobs-1 | 547000 → 约 547010 | actor 异常闭环及完整自动恢复 |
| A3 双/八卡 | 7 月 24、27 日记录 | 短训练、checkpoint/恢复、多卡通信 | 任意多节点、长期稳定 |
| A2 双节点 | 7 月 29 日记录 | 16-rank HCCL、真实短训练 | 长期负载与全部硬件故障路径 |
| 平台 3×8 smoke | kcc-910b3-3n8c-016 | HCCL、checkpoint 60、输出 | 真实模型训练效果 |
| checkpoint 停止恢复 | kcc-910b3-3n8c-017 | 48→51 停止，resume 到 60 | 最新真实模型浏览器全流程 |
| 平台两活一备 | kcc-910b3-2n8c-spare-018 | 模拟故障证据，接替后 10→60 | 自然 NPU 硬件故障验收 |
| 可编辑参数 | 020、kcc-runtime-editable-021 | 有效命令、Request/Run 参数一致 | 每个可能组合 |
| 多任务并发 | train-0901015912、train-0901015929 | 不同节点集合并发、停止清理 | 同节点精确物理卡切片 |
| 真实 MindSpeed | kcc-mindspeed-150m-canary-004 | 真实 1 iteration、约 2.7 GiB checkpoint | 长跑与收敛 |
| 日志/无进度诊断 | train-0901085522、train-0902020400 | 真实 loss，定位 watchdog/页面问题 | 被中止任务不存在的 checkpoint |
| v4 PATH | kcc-mindspeed-v4-path-smoke-001 | GCS 到训练、checkpoint、释放 | 所有镜像变体 |
| GPU 输出目录 | kcc-mindspeed-v4-gpu-mnt-smoke-001 | 输出写入 gpu-server-00 的 /mnt | 自动挂载任意新盘 |
| 自动评测 | kcc-mindspeed-v4-evaluation-smoke-001 | 真实 iteration 2/3 评测及持久曲线 | TP/PP>1、多进程评测 |
| 严格串行 | kcc-serial-0903064242-01/02 | 前序成功且清理后才启动后序 | 大模型串行完整验收 |
| 个人 W&B | train-0907042953 及部署记录 | 正确 entity 创建 run，个人凭据路径可用 | 新补丁的 loss 连续上传 |
| 指标补齐 | test_training_metrics.py | 9 项本地脚本回归 | 真实云端指标，按要求未继续实测 |

## 十六、完成度与剩余工作

### 16.1 已形成的核心能力

1. Ascend 多节点六阶段执行、真实 HCCL gate 和 RankTable 注入。
2. 训练脱离终端、逻辑 run/attempt、checkpoint 停止恢复、N-for-N 备用替换。
3. Kubernetes 原生 API、Controller 持久状态、Lease、单一稳定安装/交付入口。
4. Crossplane 平台 API 接入与所有权边界，保持与既有推理平台共存。
5. Backstage 自助训练、可编辑参数/模板、默认保护、操作按钮和归档。
6. 分阶段过程、Pod/事件/Driver/训练日志、iteration/loss 与真实目录展示。
7. 旧 MindSpeed 环境迁移、真实模型训练及 GPU 节点 checkpoint 输出。
8. checkpoint 驱动 LightEval、详情曲线、严格串行批量任务。
9. 每任务个人 W&B，以及最新训练指标缺失的源码级修复。
10. 正常入口验证、故障模拟、回归测试、可重复脚本与持续工作文档。

### 16.2 已修改或局部验证，尚不能写成全部验收完成

- 最新 W&B 指标补丁已通过本地检查，未确认新训练云端 loss/history 连续曲线。
- 最新真实 MindSpeed 的 AfterCheckpoint → 恢复 → Immediate 保留 baseline，仍需用户允许时补完整回归，不能只引用早期 smoke。
- Backstage 若干操作、收纳箱往返、批量创建和曲线切换已有实现/API 证据，但并非全部完成最终浏览器点选验收。
- 同节点精确物理卡分区、评测 TP/PP>1、自然硬件故障和月级长跑超出已有实测范围。

### 16.3 正式发布 TODO，不阻塞本阶段集成

- 统一包、Chart、文档、tag、镜像口径，修正过时的不可变约束与发布状态。
- 整理提交最新配置，合并配置特性分支，固定实际 digest，收回直接部署造成的 GitOps 漂移。
- 处理旧 Worker 大层上传，迁移 8889 测试镜像至 30670。
- 补齐 Backstage 机器人权限；整理 LightEval 干净源码、依赖和可追溯构建输入。
- 完成正式构建、合同校验、配置 PR、发布自动化，不将运行时训练控制交给 CI/CD。
- 将临时 W&B relay 纳入平台出网；若采用外部密钥托管，继续避免凭据进入源码/普通配置。

### 16.4 后续功能演进 TODO

- 若要求每个 checkpoint 都评测，增加 FIFO、保留引用和结束 drain。
- 扩大评测样本/任务集，区分 acc、acc_norm 和小样本波动。
- 明确 W&B online 不可用时失败、重试或降级策略，不假设已有自动容错。
- 按需求补 tokens/s、设备指标和告警，不把框架未产生的数据当已采集。
- 改善可变 Profile 下恢复的可复现性，例如固化本次有效配置，不重新阻碍用户修改模板。
- 按长期使用量完善 checkpoint、评测、TensorBoard 和日志保留；归档继续与删除分离。

本轮按要求只整理报告和检查文档，不执行以上待办，不启动新的训练验收。

## 十七、工程入口与使用方式

| 入口 | 内容 |
| --- | --- |
| [原生 API](../src/kcc_training/api_v1beta1.py) | Profile、Recipe、Run 当前合同 |
| [Controller](../src/kcc_training/controller.py) | 状态机、停止恢复、清理、依赖 |
| [训练协调器](../src/kcc_training/runtime/coordinator.py) | HCCL/训练阶段、证据、控制协议 |
| [评测协调器](../src/kcc_training/runtime/evaluation_coordinator.py) | checkpoint 触发和 pending 策略 |
| [旧 Supervisor](../ray_startup_bundle/recovery_supervisor.py) | 旧 CLI 恢复与兼容行为 |
| [旧 Ray 提交](../ray_startup_bundle/ray_training_submit.py) | 原训练提交路径 |
| [stable Chart](../deploy/helm/kcc-training-stable/Chart.yaml) | 唯一稳定发布面 |
| [集成方案](release/material-crossplane-integration-plan.md) | 平台架构和接入计划 |
| [标准 canary](/home/ywj/model-platform-config/environments/production/training-system/canary/README.md) | 不依赖 AI 的训练与恢复入口 |
| [工作单](/home/ywj/Material/production/model-platform/training/WORK.md) | 历史部署、后续推进、TODO |
| [指标脚本回归](/home/ywj/model-platform-config/tests/test_training_metrics.py) | W&B/TensorBoard 参数和副本检查 |

用户日常从 Backstage 选择模板，调整节点、参数和输出，按需启用评测/W&B，提交后看过程和训练输出；退出时明确选择 Immediate 或 AfterCheckpoint。平台维护者使用落盘 TrainingRequest 与 Runbook，不需在 AI 会话临时拼分布式启动命令。

## 十八、项目复盘与总结

### 18.1 推进过程中形成的方法

第一，以真实用户路径揭示问题。许多错误只有从 Backstage → Request → Run → RayCluster 完整走一次才出现，例如 JSON 解析、参数覆盖未执行、catalog 默认模板缺失和日志网络策略。底层命令单独能跑，不代表平台链路可用。

第二，用分层证据缩小范围。Pod Running、HCCL PASS、checkpoint 存在、W&B run 创建各自只证明一部分；交叉对照阶段、日志和实际产物，才区分出“真卡住”和“看不到”。

第三，保留稳定主体，逐步扩展。真实 MindSpeed、LightEval、串行和 W&B 分别加入，不为了附属功能重新设计已经验证的训练算法和恢复机制。

第四，把修复转成可维护资产。每轮尽可能留下代码、模板、测试、canary、构建脚本和工作记录，避免只有参与对话的人能再次运行。

### 18.2 项目级成果表述

本项目完成的不是给训练脚本增加几个按钮，而是建立从资源准备、通信验证、分布式训练，到 checkpoint 提交、停止恢复、备用接替，再到平台自助操作和结果观测的一套训练系统。

最重要的工作，是把“某次机器上跑通”转成稳定接口与可重复操作，把“卡住了”拆成可定位阶段，把“有 checkpoint”转成可恢复的提交证据，并在现有平台中保持控制权边界与协作兼容。

当前已有真实训练和关键恢复链路的阶段性证据，用户可通过 Backstage 使用主要能力。下一阶段重点是补齐明确缺失的验收、完善评测/指标策略和统一正式交付，而不是重新设计已经工作的训练主体。
