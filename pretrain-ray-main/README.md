# pretrain-ray-platform

当前目录只保留 Ray 正式预训练启动链路，可直接作为 launcher 镜像的构建上下文。
测试、旧控制面、smoke 资产、历史产物和报告已迁移到：

```text
/home/ywj/pretrain-ray-platform-workspace-archive
```

## 运行目录

```text
ray_startup_bundle/
  start_ray.py
  environment_check.py
  render_raycluster.py
  ray_cluster_start.py
  hccl_gate.py
  inject_training_params.py
  ray_training_submit.py
  ray_training_driver.py
  raycluster.yaml
  hccl_runtime/
  training_templates/
log/
requirements.txt
```

`hccl_runtime/` 中的 Python、C++ 和 Makefile，以及 `training_templates/` 中的
正式 Shell 模板都是启动依赖，不能从镜像中排除。

## 启动

launcher Python 需要 3.10 或更高版本，并安装：

```bash
python3 -m pip install -r requirements.txt
```

正式入口：

```bash
cd /home/ywj/pretrain-ray-platform/ray_startup_bundle
PYTHONUNBUFFERED=1 ./start_ray.py \
  --node <worker-1> \
  --node <worker-2> \
  --allow-topology-change \
  --confirm-checkpoint-exclusive
```

完整参数和生命周期说明见
[`ray_startup_bundle/README.md`](ray_startup_bundle/README.md)。

## 日志

控制端的证据、参数注入清单和执行结果统一写入：

```text
log/hccl-startup/<run-id>/
log/training-runs/<run-id>/
```

制作 launcher 镜像时，应把当前 `log/` 挂载为持久化可写卷。各 Ray worker
的完整训练日志写入所有工作节点共享的：

```text
/mnt/models/pretrain-ray-platform/log/<run-id>/
```

正式链仍依赖外部 kubeconfig、kubectl、KubeRay/Ascend 集群能力、已发布的
Ray head/worker 镜像，以及各 worker 上共享的 `/mnt/models`。
