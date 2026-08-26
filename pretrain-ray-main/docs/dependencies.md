# 外部依赖

权威依赖和责任边界见 [release/dependencies.md](release/dependencies.md)。

简要结论：stable 1.0 必需 Kubernetes、KubeRay、Ascend device plugin/CANN/HCCL、
ClusterD、RWX PVC、Artifact Gateway 和 OCI Registry；npu-exporter 仅在启用设备诊断与
自动 N-for-N 换机时需要。系统不再依赖管理机 kubeconfig、SSH、hostPath、
`/mnt/models` 或固定 server-00。

