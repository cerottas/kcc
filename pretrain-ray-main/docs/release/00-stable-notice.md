# Stable 入口说明

最终发布只使用仓库根 [STABLE.md](../../STABLE.md) 和
`deploy/helm/kcc-training-stable`。本目录名保留历史兼容，但内容均描述 stable 1.0。

静态审计证明源码、合同、Helm、wheel 和 bundle 元数据一致；它不替代目标架构镜像构建
或真实集群验收。Head/worker 必须在具备匹配 Ray、CANN/HCCL 和编译工具的发布环境构建，
并将验证结果写入兼容矩阵。

