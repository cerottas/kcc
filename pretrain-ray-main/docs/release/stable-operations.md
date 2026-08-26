# Stable 运行说明

## 1. 配置与静态检查

```bash
cp deploy/helm/kcc-training-stable/values.yaml values-prod.yaml
# 编辑 controller digest、Artifact Gateway、Secret、SA 和调度参数
make check
helm template kcc deploy/helm/kcc-training-stable \
  -n kcc-training -f values-prod.yaml --include-crds
```

controller 的 imagePullSecrets、nodeSelector、tolerations、affinity、priorityClassName、
topologySpreadConstraints、resources 和 ServiceAccount 均可通过 values 配置。
RuntimeProfile 的 `images.pullSecrets`、可选 RuntimeClass，以及 head/worker resources、
tolerations、priorityClassName、rayCpus 用于 Ray Pod。

## 2. 预检与安装

`stable-preflight.sh` 可在安装前或安装后运行。安装前检查当前操作者权限；安装后检查
Controller/Runtime ServiceAccount 的实际权限。传入 Profile 时还会检查确切 PVC、
RuntimeClass、节点设备资源和 pull Secret。

```bash
scripts/stable-preflight.sh \
  values-prod.yaml kcc-training kcc examples/runtime-profile.yaml

helm upgrade --install kcc deploy/helm/kcc-training-stable \
  -n kcc-training --create-namespace -f values-prod.yaml

scripts/stable-preflight.sh \
  values-prod.yaml kcc-training kcc examples/runtime-profile.yaml
```

Artifact Gateway、KubeRay、ClusterD、Ascend device plugin 和 PVC 必须由平台先提供。

## 3. 提交与观察

环境化 `examples/*.yaml`，依次应用 RuntimeProfile、Recipe 和 TrainingRun：

```bash
kubectl apply -f examples/runtime-profile.yaml
kubectl apply -f examples/recipe.yaml
kubectl apply -f examples/training-run.yaml
kubectl -n kcc-training get trainingruns -w
kubectl -n kcc-training get trainingrun qwen3-canary-001 -o yaml
```

Material 读取 `status.phase/attempt/conditions/checkpoint/outputArtifact`，不直接修改内部
RayCluster 或 ConfigMap。

## 4. 暂停与恢复

立即暂停沿用默认模式：

```bash
kubectl -n kcc-training patch trainingrun qwen3-canary-001 \
  --type merge -p '{"spec":{"suspend":true,"suspendMode":"Immediate"}}'
```

在下一个一致 checkpoint 后暂停：

```bash
kubectl -n kcc-training patch trainingrun qwen3-canary-001 \
  --type merge -p '{"spec":{"suspend":true,"suspendMode":"AfterCheckpoint"}}'
```

第二种模式先进入 `Stopping`。runtime 记录当前 tracker，只在所有 Worker 一致看到更大的
iteration、并完成 checkpoint 快照校验后停止训练，随后 Controller 写入
`status.checkpoint`、进入 `Suspended` 并清理 RayCluster。该等待不自动超时；如果长期没有
checkpoint，操作员可将 `suspendMode` 改成 `Immediate` 强制停止，或将 `suspend` 改回
`false` 取消请求。

恢复时将 `suspend` 设为 `false`。运行中暂停后的恢复会创建新 attempt，并从已验证
checkpoint 重建 Ray/HCCL；不会复用仍可能写入的旧进程。
## 5. 离线环境

正式 bundle 包含 `images/kcc-training-images.tar`。可将 archive 导入内部 registry，
更新 values/Profile 为 registry 返回的新 digest；或在每个可能调度 Pod 的节点执行：

```bash
IMAGE_ENGINE=nerdctl NERDCTL_NAMESPACE=k8s.io \
  ./scripts/load-images.sh /path/to/bundle
```

Docker 节点使用默认 `IMAGE_ENGINE=docker`。随后运行 `install-stable.sh` 安装 Chart。

Chart 卸载不会删除 CRD。删除 CRD、PVC 或 artifact 属于独立的数据销毁操作，不在自动
卸载流程中。

