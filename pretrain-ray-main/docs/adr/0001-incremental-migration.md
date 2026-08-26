# ADR-0001：采用端口适配器进行渐进迁移

- 状态：Accepted
- 日期：2026-08-19

## 决策

保留已经运行过的六阶段脚本作为兼容基线，在 `src/kcc_training` 建立纯领域层和外部
端口。迁移顺序从只读操作开始，最后才替换资源写入与清理。

Material 或其他平台只依赖版本化契约，不导入 `ray_startup_bundle`，也不感知
ClusterD ConfigMap 名称、物理节点、hostPath 或 Ray Pod 细节。

## 原因

恢复 Supervisor 同时承担状态存储、资源所有权、Ray 重连、诊断和换机。一次性重写会
让错误从单个模块扩散到 checkpoint 安全边界。端口适配器允许新旧逻辑对同一只读证据
做 shadow comparison，并让每次切换都可单独回滚。

## 结果

- 迁移期会短暂存在新旧两套实现，需要 parity tests 控制语义漂移。
- 新领域层禁止依赖 kubectl 文本输出、shell 模板和固定集群资源名。
- 所有具有删除、替换节点或发布制品副作用的 adapter 必须先实现所有权前置条件。
