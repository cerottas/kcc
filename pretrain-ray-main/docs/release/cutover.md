# Shadow、切流与回滚手册

## 发布冻结

1. 为旧流程保存成功、HCCL 失败、训练失败和恢复案例的归一证据。
2. 在目标架构构建三类镜像，推送并记录 digest。
3. 运行 `make check`，生成 stable bundle 和 SHA256SUMS。
4. 定制 values/Profile，在隔离命名空间执行安装前和安装后 preflight。

## Canary

1. 使用不会写生产 URI 的短 Recipe。
2. 观察 `Pending -> Starting -> Running -> Succeeded`。
3. 从 Artifact Gateway 重新解析输出，校验 URI、size 和 digest。
4. 演练无 checkpoint 的早期失败、同拓扑恢复、暂停/恢复和控制器 leader 重启。
5. 只有启用 npu-exporter 时才演练节点替换；证据不完整必须保持重试或转人工，不能猜测。
6. 用 `scripts/shadow-compare.py legacy.json stable.json` 比较 world size、RankTable、
   checkpoint 和输出摘要。

## 切流

停止向旧入口提交新任务，等待现有任务结束；不迁移运行中的进程。Material 默认入口只改为
三个 v1beta1 CR。新旧编排不得同时使用同一个 outputSubpath 或共享 run 目录。

逐步扩大 canary，至少跨越一个真实 checkpoint 周期，并记录兼容矩阵后再完全切流。
旧入口保留一个明确的回滚窗口，但标为只读/拒绝新提交。

## 回滚

1. 停止新提交，将未执行或可安全停止的 TrainingRun 设置 `suspend: true`。
2. 等待 Controller 删除当前 RayCluster，确认共享 workspace 没有写进程。
3. 将 stable Controller 缩容为 0，保留 CR、PVC、checkpoint 和 Artifact。
4. 仅从已验证 checkpoint 启动旧入口；不要与 stable 同时写。
5. 记录原因、attempt、checkpoint/output digest，修复后从 canary 重新开始。

卸载 Chart 不删除 CRD；删除 CRD 或数据需要单独审批。

