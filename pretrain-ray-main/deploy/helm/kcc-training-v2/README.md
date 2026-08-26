# KCC Training Controller

该 Chart 安装 `v1beta1` CRD、双副本 Lease 选主控制器、运行时 ServiceAccount 和最小
RBAC。它不会创建任何 TrainingRun，也不会自动创建 PVC、KubeRay Operator、Ascend
Device Plugin、ClusterD 或 npu-exporter。

安装前必须提供 digest 固定的控制器镜像：

```bash
helm upgrade --install kcc-training deploy/helm/kcc-training-v2 \
  --namespace kcc-training --create-namespace \
  --set-string controller.image=REGISTRY/kcc/controller@sha256:DIGEST
```

生产 GitOps 中应将 `--create-namespace` 改为声明式 Namespace，并在同步前运行
`scripts/preflight.py`。默认 `example.invalid` 镜像只用于 `helm lint`，不能部署。

