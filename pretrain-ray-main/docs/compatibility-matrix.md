# 运行时兼容矩阵

生产镜像发布前必须把下表中的 `TBD` 替换为经过 HCCL gate 和最小训练验证的确切版本；
不能仅记录 `latest` 或镜像 tag。

| Bundle | 架构 | K3s | KubeRay | Ray | CANN | HCCL | PyTorch | torch-npu | MindSpeed-LLM | 镜像 digest | 状态 |
|---|---|---|---|---|---|---|---|---|---|---|---|
| ascend-a3-v1 | aarch64 | TBD | TBD | TBD | TBD | TBD | TBD | TBD | v2.3.0 | TBD | 待认证 |
| ascend-910b3-v1 | aarch64 | TBD | TBD | TBD | TBD | TBD | TBD | TBD | v2.3.0 | TBD | 待认证 |

每个 Bundle 的准入测试至少包括：镜像签名/digest、设备枚举、单机 HCCL、多机 HCCL、
Ray Job 提交与重连、checkpoint 写入与一致性、故障后同拓扑重试、稳定证据换机和人工停止。

兼容性证据还必须记录 worker 的运行身份与设备权限方案。worker 不要求固定为 root，但容器
必须能执行 `hccn_tool` 并访问已分配 Ascend devices；用户组、capabilities 或平台安全策略
应通过预检和 HCCL canary 实测，而不是只按 UID 判断。

