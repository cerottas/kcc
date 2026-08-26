# 目标集群依赖与责任边界

| 依赖 | 必需能力 | KCC 是否打包 | 发布前证据 |
|---|---|---:|---|
| Kubernetes | CRD、RBAC、Lease、PVC、RuntimeClass（可选） | stable Chart 打包自身 CRD/RBAC | API discovery、Helm render、auth can-i |
| KubeRay | `ray.io/v1 RayCluster` | 否 | CRD Established、RayCluster Ready |
| Ascend 平台 | device plugin、驱动、固件、CANN/HCCL | 否 | allocatable、容器设备枚举、HCCL gate |
| ClusterD | 为每个 RayCluster 提供可清洗的 RankTable 输入 | 否 | RankTable 拓扑与 worker/device 数一致 |
| RWX 存储 | Head 和全部 worker 共享一个 PVC | 否 | PVC Bound、多 Pod 读写 canary |
| Artifact Gateway | resolve/download/upload v1 协议 | 客户端在 KCC；服务不打包 | 输入下载与输出上传 round-trip |
| OCI Registry | 保存三类 digest 镜像 | 镜像定义/构建脚本 | pull by digest |
| npu-exporter | 设备健康和进程证据 | 否；RBAC 可由 Chart 创建 | 仅自动换机模式需要 |

## 必需语义

1.1 的 `rankTableProvider` 仅支持 `clusterd`。native/static 不属于公开合同，集成层
不得生成这些值。

`healthProvider: kubernetes` 无需 exporter，只提供基础节点/调度状态，TrainingRun 应使用
`maxReplacements: 0`。`healthProvider: npu-exporter` 才支持设备诊断和 N-for-N 换机；
此时 values 必须设置 `npuExporter.enabled: true`，或由平台提供等价 RBAC。

共享存储必须支持所有 Ray Pod 同时读写。容量应覆盖输入缓存、每个 attempt 日志、
checkpoint 和待上传输出；KCC 不负责创建或扩容 PVC。

Artifact Gateway 必须在返回成功前持久化内容和 manifest，并对相同 URI/digest 幂等，
对相同 URI/不同 digest 返回冲突。

## 镜像基础环境

- controller base：Python、pip、setuptools/wheel；
- head base：Python、Ray、kubectl 运行依赖；
- worker base：Python、Ray、PyTorch/torch-npu、CANN/HCCL，并在构建阶段提供
  `make`、C++ 编译器和 CANN headers；
- worker CANN 路径可通过构建环境变量 `CANN_ASCEND_DIR` 指定；
- control/head 与 worker 可分别构建为 amd64 和 arm64。


worker 运行身份不要求固定为 root，但必须能执行 `hccn_tool` 并访问分配给 Pod 的 Ascend
设备。目标平台可通过用户组、capabilities 或安全策略提供权限，并以预检和 HCCL canary
验证；不要仅依据 UID 推断兼容性。
版本组合必须记录 Kubernetes、KubeRay、Ray、CANN、HCCL、驱动、固件、device plugin、
ClusterD、npu-exporter、三个镜像 digest 和 Chart 版本。

目标集群不需要管理机上的 `/home/ywj`、`/mnt/models`、用户 kubeconfig、SSH 免密、
server-00 本地状态或 hostPath。

