# kcc-training Helm Chart

`0.1.0` 只安装现有 Supervisor 所需的 ServiceAccount 和最小 RBAC，不创建训练、NPU
工作负载或 CRD。安装前必须已有目标 namespace、KubeRay CRD；启用 exporter reader 时
还必须已有 exporter namespace。

```bash
helm lint deploy/helm/kcc-training
helm template kcc-training deploy/helm/kcc-training \
  --namespace pretrain-ray
```

完整 controller Deployment、Lease、CRD 会在旧恢复状态机迁入可测试的 application 层
以后加入，避免先发布无法 reconcile 的 API。
