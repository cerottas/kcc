# kcc

KCC 集群工具共同项目。

当前可交付组件：

- [`pretrain-ray-main`](pretrain-ray-main/README.md)：可移植的 Kubernetes/KubeRay/Ascend
  分布式训练控制面，提供版本化 v1beta1 API、自动恢复、checkpoint 与 Artifact 发布。

该组件当前唯一 stable 入口是
[`deploy/helm/kcc-training-stable`](pretrain-ray-main/deploy/helm/kcc-training-stable)，
版本、依赖、构建和离线分发说明见
[`pretrain-ray-main/STABLE.md`](pretrain-ray-main/STABLE.md) 与组件 README。历史脚本和
其他 Chart 仅用于迁移/回滚，不是发布入口。

运行日志、恢复状态、缓存和测试归档不属于源码发行内容。
