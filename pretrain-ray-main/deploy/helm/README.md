# Helm Charts

`kcc-training-stable/` 是唯一受支持、参与 CI 和进入离线 bundle 的 Chart。它发布
`training.kcc.io/v1beta1` CRD、namespace-scoped controller/runtime RBAC、最小 nodes
ClusterRole、release-scoped ServiceAccount 和 controller PDB。

`kcc-training/`、`kcc-training-final/`、`kcc-training-release/`、`kcc-training-v1/` 与
`kcc-training-v2/` 均在 `Chart.yaml` 标记 `deprecated: true`，只保留作迁移或回滚参考。
不要从这些目录发布新版本，也不要将其加入 stable bundle。

```bash
helm lint deploy/helm/kcc-training-stable
helm template kcc deploy/helm/kcc-training-stable --namespace kcc-training --include-crds
```
