# KCC Training

KCC Training 是面向 Kubernetes、KubeRay 和 Ascend NPU 的分布式训练控制面。平台只需提交
三个 `training.kcc.io/v1beta1` 资源，控制器负责创建 RayCluster、执行 HCCL gate、管理恢复
attempt，并将输出发布为不可变 `artifact://` 制品。

当前源码和分发面统一为 **1.1.3**。唯一受支持的发布入口是：

- Helm Chart：`deploy/helm/kcc-training-stable`
- Controller：`python -m kcc_training.controller_stable`
- Runtime coordinator：`python -m kcc_training.runtime.coordinator_stable`
- API：`training.kcc.io/v1beta1`
- 示例：`examples/*.yaml`

`bin/kcc_ray`、`ray_startup_bundle/` 的旧编排入口及其他同名 Chart 只用于迁移审计和紧急
回滚，不会进入 stable bundle，也不应由新集成调用。

源码既可保留为当前 monorepo 的 `pretrain-ray-main/` 组件，也可将该目录独立抽取为
GitHub 仓库；根 workflow 与本目录 workflow 使用同一 stable 门禁。

## 运行模型

```text
Material / GitOps
  -> TrainingRuntimeProfile（集群能力和不可变运行环境）
  -> TrainingRecipe（结构化命令和 artifact 引用）
  -> TrainingRun（规模、暂停和恢复预算）
  -> KCC Controller
  -> KubeRay -> HCCL gate -> torch distributed training
  -> checkpoint + immutable output Artifact
```

Recipe 和 RuntimeProfile 的 `spec` 不可原地修改；发布新版本时使用新资源名。
TrainingRun 创建后仅 `spec.suspend` 和 `spec.suspendMode` 可修改。这使重放、审计和
GitOps diff 保持确定；`Immediate` 为兼容默认值，`AfterCheckpoint` 会等待请求后的下一个
多 Worker 一致 checkpoint。

## 外部依赖

KCC 不打包集群基础设施。目标环境必须提供：

- Kubernetes 及 `ray.io/v1` KubeRay Operator；
- Ascend device plugin、与镜像匹配的驱动/CANN/HCCL，以及可选 RuntimeClass；
- ClusterD RankTable 集成；1.0 仅支持 `rankTableProvider: clusterd`；
- 所有 Ray Pod 可读写的 RWX PVC；
- 实现 `docs/release/artifact-gateway.md` 的 Artifact Gateway；
- 可按 digest 拉取 controller/head/worker 镜像的 OCI Registry。

`healthProvider: kubernetes` 不依赖 npu-exporter，但只支持调度/节点状态观察，必须设置
`maxReplacements: 0`。需要自动设备诊断和 N-for-N 换机时，使用
`healthProvider: npu-exporter`，并在 Helm values 中启用 `npuExporter.enabled`。

当平台必须固定 Ascend 物理卡时，在 RuntimeProfile 的 `accelerator` 中设置
`physicalDeviceIDs`。列表长度必须等于 `devicesPerNode`；stable renderer 会同时写入
`huawei.com/Ascend910` Pod 注解、物理 `ASCEND_VISIBLE_DEVICES`、稠密逻辑
`ASCEND_RT_VISIBLE_DEVICES` 和 Ray 防覆盖变量。省略该字段时继续由 device plugin 分配。

完整责任边界见 [依赖说明](docs/release/dependencies.md)。

## 本地检查

需要 Python 3.10+、Helm 3 和 PyYAML：

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[dev]'
make check
```

`make check` 执行单元测试、编译、v1beta1 合同验证、stable Helm 多配置渲染、wheel
安装检查和脚本静态检查；它不会连接集群或构建 CANN 镜像。

## 构建镜像

所有基础镜像必须固定 digest。显式选择目标架构：

```bash
scripts/build-images.sh registry.example/kcc 1.1.3 \
  python-base@sha256:... ray-head-base@sha256:... \
  ascend-worker-base@sha256:... kubectl@sha256:... \
  linux/amd64 linux/arm64
```

脚本只在本地加载镜像。推送后使用 registry 返回的 digest 更新 Helm values 和
RuntimeProfile。镜像兼容组合必须记录在
[兼容矩阵](docs/compatibility-matrix.md)并通过目标集群验收。

## 安装与首次任务

复制 stable values，替换 controller digest、Artifact Gateway、Secret 和目标集群调度配置：

```bash
cp deploy/helm/kcc-training-stable/values.yaml values-prod.yaml
helm upgrade --install kcc deploy/helm/kcc-training-stable \
  --namespace kcc-training --create-namespace \
  --values values-prod.yaml
```

将 `examples/runtime-profile.yaml` 中的镜像、节点、PVC、资源名和 RuntimeClass 改为目标
环境值。安装后执行只读预检：

```bash
scripts/stable-preflight.sh \
  values-prod.yaml kcc-training kcc examples/runtime-profile.yaml
```

随后按顺序提交资源：

```bash
kubectl apply -f examples/runtime-profile.yaml
kubectl apply -f examples/recipe.yaml
kubectl apply -f examples/training-run.yaml
kubectl -n kcc-training get trainingruns -w
```

示例包含占位 digest 和节点名，不能原样用于生产。

## Material 集成边界

Material 只需要：

1. 引用管理员维护的 RuntimeProfile；
2. 创建版本化 Recipe；
3. 通过平台 TrainingRequest API 提交、停止和恢复训练，由 Crossplane 创建 TrainingRun；
4. 读取映射后的 `phase/attempt/conditions/checkpoint/outputArtifact`；需要安全停点时
   选择 `AfterCheckpoint`，并展示 `Stopping` 阶段。

Material 不应直接创建 TrainingRun，也不应创建或删除 RayCluster、attempt ConfigMap、
Lease；不得向 KCC 传递宿主机路径、kubeconfig、SSH 凭据或 shell 字符串。可复制的
TrainingRun JSON Schema 位于 `contracts/`，完整平台映射与实施门禁见
[Material/Crossplane 集成计划](docs/release/material-crossplane-integration-plan.md)。

## 离线分发

镜像已按 digest 推送并拉取到构建机后：

```bash
scripts/build-stable-bundle.sh ./kcc-training-1.1.3 1.1.3 \
  registry/controller@sha256:... \
  registry/head@sha256:... \
  registry/worker@sha256:...
```

bundle 包含镜像归档、完整 Python wheel 依赖、stable Chart、contracts、示例、可编辑
values、镜像锁、安装/预检脚本、文档和 `SHA256SUMS`。使用前编辑 values，再运行：

```bash
kcc-training-1.1.3/scripts/install-stable.sh \
  ./kcc-training-1.1.3 kcc-training kcc ./values-prod.yaml
```

详细操作、切流与回滚见 [STABLE.md](STABLE.md) 和
[发布手册](docs/release/README.md)。

## 仓库结构

```text
src/kcc_training/                    控制器、运行时和外部适配器
deploy/helm/kcc-training-stable/     唯一发布 Chart 与 v1beta1 CRD
contracts/                           Material 可消费的 JSON Schema
examples/                            canonical v1beta1 示例
docker/                              controller/head/worker 镜像
scripts/                             构建、审计、预检和离线安装
tests/                               单元及发布语义测试
ray_startup_bundle/                  迁移期 HCCL 实现与 legacy 回滚材料
```

本仓库使用 Apache-2.0 许可证。源码通过 CI 和静态发布审计不等于目标集群验收完成；上线前
仍需按兼容矩阵完成真实 HCCL、短训练、checkpoint 恢复和故障演练。
