# KCC Training 1.1 发布手册

唯一发布实现是：

- `deploy/helm/kcc-training-stable`
- `kcc_training.controller_stable`
- `kcc_training.runtime.coordinator_stable`
- `examples/*.yaml`
- `scripts/stable-audit.sh`、`stable-preflight.sh` 和
  `build-stable-bundle.sh`

本目录中的文档服务于 stable 入口；文件名含 release 只是历史目录名。
`deploy/helm/kcc-training-release`、`-v1`、`-v2`、`-final` 以及 bootstrap Chart
均标记为 deprecated，既不进入 bundle，也不参加发布审计。

## 链路

1. Material 创建 TrainingRequest，Crossplane 将其组合为引用不可变 RuntimeProfile 和 Recipe 的 TrainingRun。
2. Controller 冻结 attempt 拓扑和结构化运行规范，创建 RayCluster。
3. Head 从 Artifact Gateway 校验并物化 source/model/data。
4. ClusterD 提供原始 RankTable，runtime 完成清洗、HCCL gate 和拓扑绑定。
5. 每个 Ray actor 固定到一个节点，以结构化环境启动训练。
6. 软件/网络失败按预算同拓扑重试；只有稳定 npu-exporter 证据才能换机。
7. 输出被确定性打包为不可变 artifact，状态写入 checkpoint 和 outputArtifact。

## 文档

- `dependencies.md`：集群责任边界；
- `artifact-gateway.md`：唯一外部 HTTP 制品协议；
- `runtime-contract.md`：训练脚本必须消费的输入、输出、checkpoint 与拓扑环境变量；
- `stable-operations.md`：安装、预检和日常操作；
- `cutover.md`：canary、切流和回滚；
- `material-crossplane-integration-plan.md`：Material、Crossplane、KCC 与 KubeRay 的架构边界和分阶段集成计划；
- `../compatibility-matrix.md`：环境认证记录。

