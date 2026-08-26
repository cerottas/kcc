# 迁移状态

## 已收敛

- 公共 API 已统一为 `training.kcc.io/v1beta1`；
- 唯一 Chart 为 `deploy/helm/kcc-training-stable`；
- 控制器和 runtime 使用 stable composition root；
- 输入/输出已改为 Artifact Gateway + RWX PVC；
- 状态持久化使用 TrainingRun status/Lease；
- 镜像、合同、示例、CI 和离线 bundle 已形成标准分发面；
- 旧 Chart 和脚本明确标为 deprecated。

## 保留内容

`bin/kcc_ray`、旧 Supervisor 和 `ray_startup_bundle/` 的部分模块仍保留为迁移证据和
一个回滚窗口。stable runtime 可复用其中经过测试的 HCCL pipeline，但不依赖旧 CLI、
hostPath 状态、SSH 或固定 server-00 编排。

## 未完成的环境工作

源码工程化完成不代表任意 Ascend 集群已认证。每个目标环境仍需完成镜像兼容矩阵、
ClusterD RankTable、HCCL、短训练、checkpoint、暂停恢复、故障演练和 Artifact round-trip
验收。完成后才能将 Material 默认入口切到 TrainingRun。

