# Stable 1.1 发布入口

KCC Training 1.1 的唯一发布面是 `deploy/helm/kcc-training-stable`。包版本、Chart
`version/appVersion` 和 `kcc_training.__version__` 必须一致；当前为 1.1.5。

## 支持边界

- Kubernetes API：`training.kcc.io/v1beta1`；
- Kubernetes 兼容：1.1.5 的三个 CRD 已通过 K3s 1.34.6 API server dry-run；
  stable audit 禁止混用 `properties`/`additionalProperties` 和二次复杂度 `uniqueItems`；
- RankTable：仅 `clusterd`；
- 制品：仅 `artifact://namespace/name/version`；
- 镜像：生产配置必须使用 `@sha256`；
- 工作目录：新 Recipe 使用相对于 source artifact 的路径；
- 存储：训练数据与状态使用共享 PVC；Ascend worker 只读挂载宿主机匹配版本的
  `/usr/local/Ascend/driver`，不以 hostPath 保存训练数据或控制状态；
- 控制状态：TrainingRun status 和 Lease，不依赖管理节点本地文件；
- 可选物理卡固定：`accelerator.physicalDeviceIDs` 仅用于 `huawei.com/Ascend910`，
  数量必须等于 `devicesPerNode`，并渲染设备注解及物理/逻辑可见设备环境变量。
- Volcano：Ascend RuntimeProfile 的 CPU-only Ray Head 自动标记
  `huawei.com/skip-ascend-plugin=enabled`；NPU Worker 仍由 Ascend-for-Volcano
  按资源数量和拓扑分配设备。

RuntimeProfile 和 Recipe 不可变；TrainingRun 仅允许修改 `suspend`。需要改变镜像、节点、
命令或恢复预算时，创建新的版本化对象或新的 TrainingRun。

## 健康与恢复

`healthProvider: kubernetes` 是无 exporter 的基础模式，不提供设备级故障证据，因此
TrainingRun 必须使用 `maxReplacements: 0`。它仍可按预算进行同拓扑重试。

`healthProvider: npu-exporter` 支持设备诊断和 N-for-N 换机。部署时同时设置：

```yaml
npuExporter:
  enabled: true
  namespace: npu-exporter
  app: npu-exporter
  port: 8082
  rbac: {create: true}
```

当 exporter RBAC 由平台预先管理时，可将 `rbac.create` 设为 false。

## 发布门禁

```bash
make check
scripts/build-stable-bundle.sh --metadata-only /tmp/kcc-bundle 1.1.5 \
  controller@sha256:... head@sha256:... worker@sha256:...
```

普通 CI 不构建依赖 CANN 的镜像；镜像由手动 workflow 或目标架构构建机完成。生产发布还
必须执行：

1. 三类镜像构建、推送并记录 registry digest；
2. 目标环境 values/Profile 定制；
3. stable-preflight 只读检查；
4. HCCL gate、短训练、checkpoint 恢复、暂停/恢复和故障演练；
5. 记录兼容矩阵和验收证据后再切流。

## 分发内容

正式 bundle 只包含 stable Chart、canonical `examples/`、v1beta1 `contracts/`、
Python wheel 及依赖、三类镜像归档、镜像锁、安装/预检脚本、文档和校验和。历史 Chart、
旧 Supervisor 和本机运行状态不会进入 bundle。

具体命令见 [README.md](README.md)，集群操作见
[stable-operations.md](docs/release/stable-operations.md)，切流见
[cutover.md](docs/release/cutover.md)。

