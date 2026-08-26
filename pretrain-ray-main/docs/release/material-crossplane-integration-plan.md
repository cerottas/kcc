# Backstage / Crossplane / KCC 架构集成计划（Material 仓库）

> 状态：控制面 GitOps staging 已形成，待发布配置 Git 并由 Argo 手动同步
>
> 形成日期：2026-08-25
>
> KCC 基线：1.1.0，`training.kcc.io/v1beta1`
>
> 范围：平台架构、控制边界、运行协议和分阶段验收；本文不代表任何对象已部署到生产集群。

## 1. 结论

采用一条明确的控制链：

```text
Backstage frontend
        |
        v
Backstage backend plugin / Training API
        |
        v
TrainingRequest（platform.example.com/v1alpha1，平台 API）
        |
        v
Crossplane 2.3 Pipeline-mode Composition
        | 只创建和更新一个 TrainingRun
        v
TrainingRun（training.kcc.io/v1beta1）
        |
        v
KCC Controller
        | 创建 attempt ConfigMap、控制 ConfigMap 和独立 RayCluster
        v
现有 KubeRay Operator
        |
        v
Ray Head + ARM64 Ascend Worker Pods
        |
        +--> ClusterD RankTable -> HCCL gate -> 分布式训练
        +--> RWX PVC checkpoint/output
        +--> Artifact Gateway -> Artifact Keeper
```

TrainingRequest 是 Backstage 对用户的训练 API。Backstage backend plugin 创建和更新
TrainingRequest；Material 只是保存静态配置、部署清单和集成记录的 Git 仓库，不是运行时
服务。Crossplane 不直接创建 RayCluster，KCC 不处理租户、审批和配额，KubeRay 不理解训练
checkpoint 或故障换机。

现有 KubeRay Operator 1.6.0 已在生产确认 Ready；本方案只复用它，不部署第二套 KubeRay，
也不接管其 Helm release。

## 2. 对原架构图的修正

1. Argo CD 只管理静态控制面和 catalog；运行时 TrainingRequest、TrainingRun、
   RayCluster、Attempt ConfigMap 和 Result ConfigMap 不进入 Git，也不由 Argo 管理。
2. KCC 只有一个 stable Helm Chart 和一个 stable API 面；legacy
   `bin/kcc_ray`/`ray_startup_bundle` 不作为 Backstage 或 Training API 的调用入口。
3. Crossplane v2 namespaced XR 只能直接组合同 namespace 的 namespaced 资源。因此首期
   TrainingRequest 与 TrainingRun 都放在中央 `kcc-training` namespace，由 Backstage
   backend plugin 代表租户写入；普通租户不获得该 namespace 的直接写权限。
4. Result ConfigMap 是 KCC attempt 内部协议。Crossplane 只从 TrainingRun.status 回写
   TrainingRequest.status，Backstage 只读取 TrainingRequest.status；双方均不绑定 Result
   ConfigMap 名称或数据结构。
5. checkpoint/output 使用所有 Ray Worker 可读写的 RWX PVC；source/model/data 经
   Artifact Gateway 校验并物化，不能把宿主机路径、SSH、kubeconfig 或 Secret 放进
   Recipe/TrainingRequest。
6. 停止训练必须修改 TrainingRun 的声明状态。删除 TrainingRequest/TrainingRun 不是
   Stop API，也不能用删除 Pod 或 RayCluster 代替安全停止。

## 3. 已知生产基线和前置缺口

截至 2026-08-26 的生产只读证据：

- 目标是 `server-00` K3s `v1.34.6+k3s1`；生产写入仍必须使用
  `sudo k3s kubectl`，Helm 必须显式指定 K3s kubeconfig。
- Crossplane 2.3.4、provider-kubernetes 和 shared `function-patch-and-transform` 均 Healthy；
  Function 已由现有平台管理，本集成只引用其固定内部 package digest。
- 现有三个 ModelDeployment Composition 已使用同一个 Function；训练新增独立
  TrainingRequest XRD，不改变推理 API。
- Backstage 已部署；它是未来训练前端/API 入口，Material 只是仓库名。
- KubeRay 1.6.0 已由 Helm 管理并 Ready；KCC 不部署第二套 Operator。
- Argo CD 现有 control-plane AppProject/Application 保持人工同步，无 automated、prune 或
  self-heal；训练接入复用并扩展该项目。
- 集群尚无 KCC CRD、KCC controller、TrainingRequest 或 TrainingRun，因此当前没有对象或
  字段所有权冲突。
- KCC 1.1.0 源码已形成本地 Git 提交，controller AMD64 镜像已推送并固定 digest；该提交
  尚未发布到已确认所有权的远端。
- Artifact Gateway 兼容层、head/worker digest、RWX checkpoint、ClusterD/npu-exporter
  训练合同仍未完成；这些不阻塞零实例控制面，但阻塞首次 TrainingRequest。

这是原平台上的增量接入，不重建 Crossplane、KubeRay、Backstage、Artifact Keeper 或 Argo。

## 4. 三个对象边界

以下是控制器所有权边界，不新建独立 AppProject、NetworkPolicy 或准入控制器：

| 对象边界 | 唯一静态管理者 | 管理对象 | 不得管理 |
|---|---|---|---|
| `kcc-control-plane` | Argo CD | stable CRD、KCC Controller、SA/RBAC、production values | TrainingRun 实例、KubeRay release、PVC 数据、Secret 值 |
| `training-platform-api` | Argo CD | TrainingRequest XRD、Composition、TrainingRun namespaced RBAC | shared Function、TrainingRequest 实例、RayCluster、Pod、KCC ConfigMap |
| `training-catalog` | Argo CD | 不可变 TrainingRuntimeProfile、TrainingRecipe | TrainingRun、用户请求、运行状态 |

现有 `model-platform-control-plane` AppProject 只做最小增量授权，新的
`model-platform-training-system` Application 同时渲染 stable Chart 和 API 清单。
AppProject/Application 本身沿用 server-00 管理员 bootstrap；Application 内静态资源只由
Argo 写入，不再直接执行 Helm 或 kubectl apply。shared Function 和现有 KubeRay 只作为外部
依赖验证，不由本集成安装、升级或删除。

## 5. 资源所有权

| 对象/字段 | 唯一写入者 | 说明 |
|---|---|---|
| TrainingRequest.spec | Backstage backend plugin | 用户意图、审批结果和可变停止意图 |
| TrainingRequest.status | Crossplane Composition Function | 镜像 TrainingRun 的观察状态，不伪造运行结果 |
| TrainingRun.metadata/spec | Crossplane | 使用确定性名称；只按合同更新 suspend/suspendMode |
| TrainingRun.status | KCC Controller | phase、attempt、节点、checkpoint、output、condition |
| RayCluster | KCC Controller | 每个 attempt 一个独立集群 |
| attempt/control/result ConfigMap | KCC runtime/controller | 内部协议，不成为平台 API |
| TrainingRuntimeProfile | 平台运维 catalog | spec 不可变，变更时发布新名字 |
| TrainingRecipe | 应用发布 catalog | spec 不可变，变更时发布新名字 |
| source/model/data/output artifact | Artifact Gateway / Artifact Keeper | 内容寻址或不可变引用，不走 Kubernetes status 大对象 |
| checkpoint | KCC 约定的 RWX PVC | 训练脚本写入，runtime 校验，平台只展示摘要 |

Crossplane 通过 namespaced Role/RoleBinding 只管理 `kcc-training` 中的 TrainingRun；
不授予 `ray.io`、Pod、Secret、PVC 或 ConfigMap 权限。KCC 的 Role 仅在
`kcc-training` namespace 中管理它需要的 Training*、RayCluster、ConfigMap 和 Lease。

## 6. TrainingRequest API

建议新增：

```yaml
apiVersion: platform.example.com/v1alpha1
kind: TrainingRequest
metadata:
  name: qwen3-canary-001
  namespace: kcc-training
  labels:
    owner: demo-team
    project: pretrain
spec:
  projectRef: pretrain
  runtimeProfileRef: ascend-a3-v1
  recipeRef: qwen3-canary-v1
  workers: 2
  desiredState: Running       # Running | Stopped
  stopMode: AfterCheckpoint   # Immediate | AfterCheckpoint
  recovery:
    sameTopologyRetries: 1
    maxReplacements: 1
    noProgressSeconds: 1800
```

首期约束：

- 创建后 `projectRef/runtimeProfileRef/recipeRef/workers/recovery` 不可变；只允许
  `desiredState` 和 `stopMode` 改变。
- workers 范围和 recovery 总 attempt 预算与 TrainingRun CRD 完全一致。
- 名称必须满足 DNS label 并预留 KCC attempt 后缀长度；Composition 使用确定性名称，
  不使用 generateName。
- Profile/Recipe 必须在同一个 `kcc-training` namespace 中存在；Material 只展示已批准
  catalog 项，不接受任意镜像、命令、物理卡号或宿主机路径。
- XRD 使用 Crossplane 的 Automatic Composition 更新模式，保持标准升级兼容；重大映射
  变更仍先发布新 Composition 名称并从 canary 验证。

Composition 映射：

| TrainingRequest | TrainingRun |
|---|---|
| metadata.name/namespace | metadata.name/namespace |
| labels owner/project/request UID |相同标签和追踪 annotation |
| spec.runtimeProfileRef | spec.runtimeProfile |
| spec.recipeRef | spec.recipe |
| spec.workers | spec.workers |
| spec.recovery | spec.recovery |
| desiredState=Running | spec.suspend=false |
| desiredState=Stopped | spec.suspend=true |
| spec.stopMode | spec.suspendMode |

TrainingRequest.status 至少包含：

```yaml
status:
  phase: Stopping
  attempt: 0
  activeNodes: []
  spareNodes: []
  checkpoint: {}
  outputArtifact: artifact://...
  conditions: []
  observedTrainingRunGeneration: 2
```

Composition Function 从 observed TrainingRun.status 回写上述字段。Backstage frontend/backend 以
`status.phase` 为训练生命周期真相，不把 Crossplane 通用 `READY` 列当作“训练正在
运行”。建议状态语义：

- `Pending/Starting/Recovering/Stopping`：请求已同步但动作尚未完成；
- `Running`：训练正在运行；
- `Suspended`：停止意图已达成且可恢复；
- `Succeeded`：训练成功完成；
- `Stopped/ManualRequired`：需要人工处理，Ready=False。

## 7. KCC 四项核心能力

| 功能 | 1.1.0 接入位置 | 平台可见证据 | 集群验收 |
|---|---|---|---|
| HCCL 测试 | runtime 在训练子进程前执行 HCCL gate | Result 内 HCCL stage，失败进入 recovery/ManualRequired | 必做多节点成功、网络故障失败测试 |
| RankTable 注入 | ClusterD 原始表 -> KCC 校验/快照 -> `RANK_TABLE_FILE` | `rankTableSha256`、冻结 node rank/world size | digest 与 Worker 挂载文件一致 |
| checkpoint 后停止 | `suspend=true` + `suspendMode=AfterCheckpoint` | `Stopping`、baseline、新 iteration、checkpoint 摘要 | 必做下一 checkpoint 前不停、之后全部 Worker 停止 |
| 备用机重连 | npu-exporter 稳定故障证据 -> N-for-N 替换 -> 新 attempt | activeNodes/spareNodes/replacementsUsed/diagnosis | 必做一台故障、一台健康备机、同 checkpoint 恢复 |

这些能力仍由 KCC 承担，Crossplane 只翻译意图和汇总状态，不能在 Composition 中重写
HCCL、RankTable、checkpoint 或换机状态机。

## 8. checkpoint 后停止协议

平台请求：

```yaml
spec:
  desiredState: Stopped
  stopMode: AfterCheckpoint
```

执行序列：

```text
TrainingRequest generation N
 -> TrainingRun suspend=true,suspendMode=AfterCheckpoint
 -> KCC 先持久化 phase=Stopping + stopRequestGeneration=N
 -> KCC 将 StopAfterCheckpoint 写入 attempt control ConfigMap
 -> runtime 记录所有 Worker 当前一致 tracker iteration（baseline）
 -> 训练继续运行
 -> 所有 Worker 看到 iteration > baseline
 -> runtime 对同一 iteration 做完整一致 checkpoint snapshot
 -> 向全部训练进程组发送 SIGTERM，超时才 SIGKILL
 -> runtime 发布绑定 run UID/attempt/generation 的 STOPPED Result
 -> KCC 复核 generation、checkpointConsistent 和 iteration > baseline
 -> KCC 先写 TrainingRun.status.checkpoint/phase=Suspended
 -> 再删除该 attempt RayCluster
 -> Crossplane 回写 TrainingRequest.status
```

安全性质：

- 已存在 checkpoint 不会让新请求立即停止；必须等请求后的下一次一致 checkpoint。
- 任一 Worker tracker 缺失、iteration 不一致、snapshot 不一致或 Result 身份不匹配时，
  不得报告安全停止成功。
- 等待 checkpoint 不自动超时，避免平台在未得到恢复点时静默杀训练。操作员可明确改为
  `Immediate`，或把 desiredState 改回 Running 取消。
- 取消与 runtime 已接受停止发生竞态时，KCC 从确认 checkpoint 创建新 attempt，不消耗
  故障重试预算。
- 恢复将创建新 RayCluster/attempt，并通过 `KCC_RESUME_FROM` 使用已验证 checkpoint；
  不复用可能仍在写入的旧训练进程。

训练脚本必须把完整 shard 持久化后，最后原子更新
`latest_checkpointed_iteration.txt`。不满足该合同的 Recipe 只能使用 Immediate，不能
声明支持 AfterCheckpoint。

## 9. 数据与制品链路

```text
TrainingRecipe 中的 artifact:// source/model/data
        |
        v
Head 上的 Artifact Gateway client（只读凭据）
        |
        v
校验 manifest/digest 后物化为 KCC_SOURCE_DIR/KCC_MODEL_DIR/KCC_DATA_DIR

所有 Worker <-> RWX PVC
  KCC_CHECKPOINT_ROOT：恢复点与 tracker
  KCC_OUTPUT_ROOT：最终待发布文件
        |
        v
Head 打包并经 Artifact Gateway 发布不可变 output artifact
        |
        v
TrainingRun.status.outputArtifact -> TrainingRequest.status.outputArtifact
```

Artifact Gateway 是 KCC `artifact://` 合同到 Artifact Keeper API 的适配层，不能让
Recipe 直接依赖 Artifact Keeper 当前 NodePort/Token 细节。source/model/data 使用只读
凭据，output 使用限定项目路径的写凭据；Secret 仅通过引用注入，不进入 Git、status 或
日志。

checkpoint 首期必须使用真实 RWX 存储。Artifact Keeper 的本地模型缓存和训练 checkpoint
不是同一种存储，不得用节点 hostPath 模型缓存冒充多 Worker checkpoint PVC。

## 10. 删除、停止与回滚边界

当前 KCC TrainingRun 没有删除 finalizer。直接删除活动 TrainingRequest 会让 Crossplane
清理 composed TrainingRun，并可能立即级联清理 RayCluster，因此 Backstage 的正常删除流程
应先设置 `desiredState=Stopped`，等待 `Suspended` 后再删除。该顺序属于平台操作语义，
默认不安装强制 ValidatingAdmissionPolicy，以免限制管理员、Crossplane 和后续控制器的
兼容性；需要防误删的环境可选择启用 `optional/deletion-protection.optional.yaml`。

管理员直接删除活动对象属于明确的立即终止，不应显示为 AfterCheckpoint 成功。删除
TrainingRequest 不删除 RWX checkpoint、output artifact、Profile 或 Recipe；数据保留与
销毁继续使用独立策略。

回滚控制面时：停止新建 TrainingRequest，保留现有 CR/PVC/artifact；回退 Argo source commit
或 production values 中的 controller digest。静态资源只有 Argo 一个 writer，任何时刻也只允许
一个 stable controller 写 `training.kcc.io/v1beta1`。

## 11. 分阶段实施

控制面与运行时分开冻结：Argo 可以先安装 KCC CRD/controller 和 TrainingRequest API，
但在 runtime lock 关闭期间不创建 Profile、Recipe、TrainingRequest、TrainingRun 或 NPU workload。

### 阶段 0：生产只读取证与版本冻结（已完成控制面部分）

- 已确认 K3s v1.34.6、Crossplane 2.3.4、shared Patch and Transform Function Healthy、
  KubeRay 1.6.0、Backstage 和现有手动 Argo 应用；
- 已确认生产尚无 KCC CRD/controller、TrainingRequest、TrainingRun 和 RayCluster；
- KCC 1.1.0 stable audit 通过，源码提交固定为
  `5c1187c76a9cc15903c0eaae02554c739ba3886a`；
- AMD64 controller 已推送内部 Registry，并固定 digest
  `sha256:77f7c73a9f24f71eb2239bf2b559e1d30e2fb2b940b436ddb4a92881ffbd1542`；
- Artifact Gateway 使用保留 DNS，服务实现、head/worker、RWX 和 NPU 证据继续留在 runtime TODO。

验收：控制面输入已锁定；runtime lock 仍关闭；0 个训练实例。

### 阶段 1：发布 GitOps 源并创建 Argo Application

- 将 `training/gitops/repository/environments/production/training-system` 提交到生产
  `model-platform-config.git` 的同名路径；
- 在 server-00 应用现有 `model-platform-control-plane` AppProject 的增量清单和
  `model-platform-training-system` Application；
- Application 同时渲染 KCC stable Chart 和 TrainingRequest XRD/Composition/RBAC，
  保持 manual sync、prune=false、self-heal=false；
- shared `function-patch-and-transform` 只验证 Healthy 和锁定 digest，不安装、不升级、
  不删除；现有 KubeRay release 同样只复用；
- repository 下的静态对象不再使用直接 Helm 或 kubectl apply。

验收：Argo diff 只包含预期静态对象，操作员手动同步后 KCC controller Ready、XRD
Established/Offered、Function Healthy，TrainingRequest/TrainingRun/RayCluster 均为 0。

### 阶段 2：无实例合同与所有权验证

- 运行合同、manifest、Helm lint/template 和 production-ready kcc/api 检查；
- 验证 Crossplane 只能管理 TrainingRun metadata/spec，KCC 只写 TrainingRun.status 并管理
  RayCluster/KCC ConfigMap/Lease，KubeRay 只管理 RayCluster.status 和派生 Pod/Service；
- 使用离线输入覆盖 Running、Immediate Stop、AfterCheckpoint Stop、status 回写和非法预算；
- 不创建真实 TrainingRequest 或 TrainingRun。

验收：静态 API 链路成立且无字段所有权冲突，训练实例仍为 0。

### 阶段 3：catalog 与 Suspended smoke

- 发布一个目标集群专用、digest 固定的 RuntimeProfile 和一个短训练 Recipe。
- 创建首个 TrainingRequest 时使用 `desiredState=Stopped`，验证 Crossplane 只创建
  `suspend=true` 的 TrainingRun，不创建 RayCluster/NPU Pod。
- 核对 owner label、CompositionRevision、字段所有权和状态回写。

验收：平台 API 到 TrainingRun 的静态链路成立，NPU request 仍为 0。

### 阶段 4：HCCL、RankTable 和短训练 canary

- 经 NPU 窗口审批后把 canary 改为 Running。
- 验证独立 RayCluster、Head/Worker 架构、节点固定、RankTable digest、HCCL gate 和
  训练子进程环境变量。
- 让训练至少产生两个 checkpoint，并生成一个最小 output artifact。

验收：HCCL PASS 后才启动训练；status.activeNodes、attempt、checkpoint、outputArtifact
与实际一致。

### 阶段 5：checkpoint 后停止与恢复

- 在 iteration K 已一致时提交 AfterCheckpoint Stop。
- 证明 phase 进入 Stopping，iteration K 不触发停止。
- 等 K+1 完整提交后，证明所有 rank 停止、Result generation 匹配、phase=Suspended、
  RayCluster 被清理。
- 改回 Running，证明创建 attempt+1 并从 K+1 恢复。
- 分别验证等待中取消和已接受后的取消竞态。

验收：无“旧 checkpoint 立即停”、无单 Worker 提前停、无未验证 checkpoint 恢复。

### 阶段 6：备用机 N-for-N 恢复

- RuntimeProfile 使用 `healthProvider: npu-exporter`，配置互斥 active/spare 节点与
  `maxReplacements >= 1`。
- 注入一台活动节点故障，要求达到稳定故障证据阈值后才换机。
- 验证一坏一换、新 attempt、重新生成 RankTable、重新 HCCL gate，并从一致 checkpoint
  恢复；恢复预算和 replacement 计数准确。

验收：没有弱 Kubernetes 健康证据触发换机，没有部分拓扑继续训练。

### 阶段 7：Backstage 接入

- Backstage frontend 提供训练创建、停止、恢复、查询和日志跳转页面；
- Backstage backend plugin 调用 TrainingRequest API，不让前端或模板直接操作 Kubernetes；
- 表单只提供平台 catalog 中的 Profile/Recipe 和规模档位；展示 Stopping、checkpoint
  iteration、active/spare nodes、attempt、condition 和 output artifact；
- 沿用平台已有身份与项目边界；配额、审批和审计按现有能力接入，不作为 KCC API
  兼容性的前置条件；
- Material 仓库只保存 XRD、Composition、KCC Chart、catalog 和部署记录，不承载请求状态。

验收：普通用户没有 kcc-training namespace 写权限，但可经 Backstage 完成申请、观察、
安全停止和恢复。

### 阶段 8：受控生产开放

- 平台同时支持直接提交 Running 和 Stopped；首次生产 canary 仍可先用 Stopped 验证静态链路。
- 首批选择一个已验证 Profile/Recipe 完成一轮生命周期，再按实际容量扩展。
- 更新生产部署记录、版本矩阵、故障手册和回滚证据后，再逐步提高配额。

## 12. 总体验收清单

- [ ] 只存在一套目标 KubeRay Operator；KCC 不安装或接管 KubeRay release。
- [ ] Crossplane 只拥有 TrainingRun，无法创建 RayCluster/Pod/Secret/PVC/ConfigMap。
- [ ] Argo 不跟踪任何 TrainingRequest、TrainingRun、RayCluster 或 attempt ConfigMap。
- [ ] Profile/Recipe 不可变且所有镜像按目标架构固定 digest。
- [ ] 所有 Worker 使用同一冻结 RankTable，digest 与 HCCL 证据一致。
- [ ] HCCL 未通过时训练子进程绝不启动。
- [ ] AfterCheckpoint 在请求后的下一一致 checkpoint 前绝不停止。
- [ ] STOPPED Result 绑定 run UID、attempt 和请求 generation，iteration 严格大于 baseline。
- [ ] Suspended 状态先持久化 checkpoint，再清理 RayCluster；恢复使用新 attempt。
- [ ] npu-exporter 稳定证据才能触发 N-for-N 换机，换机后重新 RankTable/HCCL。
- [ ] source/model/data/output 经 Artifact Gateway，checkpoint 使用 RWX PVC。
- [ ] Backstage 正常删除流程先执行 Stop；可选准入保护不影响底层 API 兼容。
- [ ] Backstage 展示 TrainingRun 派生状态；Material 仓库不保存运行状态。
- [ ] 生产同步保持 commit-pinned、manual、prune=false、self-heal=false。

## 13. 明确的上线阻塞项

以下任一项未完成时，不开放生产自服务 Running：

1. KCC source commit 尚未发布到经确认的远端，training-system staging 尚未提交生产 Gitea；
2. 现有 AppProject 增量和 training-system Application 尚未由 server-00 管理员应用并手动同步；
3. head AMD64、worker ARM64 镜像和目标运行时兼容矩阵尚未固定；
4. ClusterD RankTable、npu-exporter、ARM64 节点和 RWX checkpoint PVC 尚未完成训练侧取证；
5. 保留的 Artifact Gateway DNS 尚无实现，resolve/download/upload v1 合同尚未 round-trip；
6. Profile/Recipe、Suspended smoke、HCCL/RankTable canary 尚未执行；
7. AfterCheckpoint、恢复和 N-for-N 尚未在目标集群演练；
8. Backstage TrainingRequest 插件、状态映射和首次 NPU canary 回滚尚未完成。

## 14. 参考

- Material 生产事实与安全规则：`/home/ywj/Material/AGENTS.md`
- Material 生产进度：`/home/ywj/Material/production/model-platform/progress-20260810.md`
- KCC 运行合同：`runtime-contract.md`
- KCC 运维操作：`stable-operations.md`
- KCC Artifact Gateway 合同：`artifact-gateway.md`
- Crossplane v2.3 Composition：<https://docs.crossplane.io/latest/composition/compositions/>
- Crossplane v2.3 XRD scope：<https://docs.crossplane.io/latest/composition/composite-resource-definitions/>
