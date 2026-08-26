# 不可变镜像

三个镜像职责分离：

- `controller`：in-cluster Kubernetes API、Lease 和 stable reconciler；
- `head`：Ray Head、stable coordinator、Artifact materializer、HCCL pipeline 和 kubectl；
- `worker`：Ray Worker、训练栈、CANN/HCCL 和构建期 native probe。

Controller 镜像默认入口是 `python -m kcc_training.controller_stable`。Helm 也显式声明
相同入口。

## 基础镜像要求

- controller：Python 3.10+、pip、setuptools/wheel；
- head：Python、Ray，以及运行 kubectl/HCCL Python pipeline 所需系统库；
- worker：Python、Ray、PyTorch/torch-npu、CANN/HCCL；构建阶段还必须提供 make、C++
  编译器和 CANN headers。

所有基础镜像必须固定 `@sha256`，并与目标平台匹配。worker 的 CANN 安装目录通过
`CANN_ASCEND_DIR` 传递，默认
`/usr/local/Ascend/cann/ascend-toolkit/latest`。

## 分平台构建

控制面和 Ascend 节点可以使用不同架构，例如 amd64 controller/head 与 arm64 worker：

```bash
CANN_ASCEND_DIR=/usr/local/Ascend/cann/ascend-toolkit/latest \
scripts/build-images.sh registry.example/kcc 1.0.0 \
  controller-base@sha256:... head-base@sha256:... \
  worker-base@sha256:... kubectl@sha256:... \
  linux/amd64 linux/arm64
```

脚本使用 buildx `--load`，不会 push。推送后必须取得 registry digest，并更新 stable
values 和 RuntimeProfile。构建参数、目标架构及 Ray/CANN/HCCL/PyTorch 组合写入兼容矩阵。

离线 bundle 保存已经构建的镜像 archive；可用 `scripts/load-images.sh` 导入每个节点的
Docker/containerd 镜像缓存，或导入内部 registry 后使用新的 digest。

