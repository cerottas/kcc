# 发布入口已统一

历史文档曾将 `kcc-training-release` 或 `kcc-training-v1` 描述为发布入口；这些路径
现已弃用并仅用于迁移参考。

当前唯一发布入口：

- [STABLE.md](STABLE.md)
- `deploy/helm/kcc-training-stable`
- `scripts/stable-audit.sh`
- `scripts/build-stable-bundle.sh`

兼容脚本 `build-offline-bundle.sh` 和 `release-preflight.sh` 只会委托对应 stable
脚本，不再维护另一套实现。

