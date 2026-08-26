# 工程化基线

## 分层

```text
Material / GitOps
  -> v1beta1 CRD 与 contracts
  -> controller state machine
  -> Kubernetes / KubeRay / Ray Jobs / ClusterD / Device Health
  -> structured runtime
  -> checkpoint and Artifact Gateway
```

集成方只拥有 TrainingRuntimeProfile、TrainingRecipe、TrainingRun 三个公共对象。内部
RayCluster、ConfigMap、Lease、Ray submission ID 和 result ConfigMap 不是公共 API。

## 工程约束

- 命令始终使用字符串数组和 `shell=False`；
- Kubernetes 访问使用 in-cluster API、resourceVersion 和 UID precondition；
- 不使用 SSH、宿主机 kubeconfig、固定管理节点或 hostPath；
- source/model/data/output 使用版本化 artifact URI 和共享 PVC；
- 镜像及基础镜像以 digest 固定；
- Recipe/Profile 不可变，attempt 的输入与拓扑可重放；
- 常规网络抖动保持当前 phase 并重试；只有无效合同、所有权冲突、恢复预算耗尽或明确的
  checkpoint 冲突进入 ManualRequired；
- 自动换机必须有稳定设备证据，不能把一般进程/网络失败推断为硬件损坏。

## 发布约束

包、Chart 和模块版本使用同一 SemVer。唯一发布 Chart 是
`deploy/helm/kcc-training-stable`。其他 Chart 标记 `deprecated: true`，不进入审计或
bundle。

GitHub PR 执行单元测试、编译、合同、Helm、wheel 和 metadata bundle 检查；实际 Ascend
镜像构建和集群验收独立执行，避免普通 CI 因无 CANN 环境被阻塞。

## 外部实现边界

KCC 依赖 KubeRay、ClusterD、Ascend runtime/device plugin、RWX CSI、Artifact Gateway 和
OCI registry，但不负责安装这些平台能力。npu-exporter 是启用 N-for-N 换机时的可选依赖。
详细版本和协议见 `docs/release/`。

旧 `bin/kcc_ray` 与 `ray_startup_bundle/` 保留一个回滚窗口；新旧编排不得同时写同一
训练输出。

