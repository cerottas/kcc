#!/usr/bin/env python3
"""Run environment, Ray, HCCL, parameter injection, and formal training."""

from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path
import re
import secrets
import shlex
import subprocess
import sys
import time
from typing import Callable, Sequence


BUNDLE_DIR = Path(__file__).resolve().parent
DEFAULT_MANIFEST = BUNDLE_DIR / "raycluster.yaml"
DEFAULT_RUNTIME_SOURCE = BUNDLE_DIR / "hccl_runtime"
DEFAULT_TRAIN_SCRIPT = (
    BUNDLE_DIR / "training_templates" / "pretrain_150M-22.sh"
)
DEFAULT_LOG_ROOT = BUNDLE_DIR.parent / "log"
DEFAULT_EVIDENCE_ROOT = DEFAULT_LOG_ROOT / "hccl-startup"
DEFAULT_TRAINING_ARTIFACT_ROOT = (
    DEFAULT_LOG_ROOT / "training-runs"
)
DEFAULT_TRAINING_CWD = "/mnt/models/CODE/MindSpeed-LLM-v2.3.0"
DEFAULT_NODES = ("110.129.0.20", "110.129.0.22")
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")
Stage = tuple[str, Sequence[str]]


def new_run_id() -> str:
    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"train-{timestamp}-{secrets.token_hex(4)}"


def execute_pipeline(
    stages: Sequence[Stage],
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    failure_handler: Callable[[int, str], None] | None = None,
) -> int:
    for index, (name, command) in enumerate(stages, start=1):
        print(f"=== Stage {index}/{len(stages)}: {name} ===", flush=True)
        print("$ " + shlex.join(command), flush=True)
        result = runner(list(command), check=False, shell=False)
        if result.returncode != 0:
            if failure_handler is not None:
                try:
                    failure_handler(index, name)
                except KeyboardInterrupt:
                    raise
                except Exception as error:
                    print(
                        f"WARNING: failure cleanup did not complete: {error}",
                        file=sys.stderr,
                    )
            print(
                f"STOP: stage {index} failed ({name}); later stages were not invoked.",
                file=sys.stderr,
            )
            return result.returncode or 1
    print(
        "PASS: formal training workflow completed successfully.",
        flush=True,
    )
    return 0


def source_declared_workers(path: Path) -> int:
    if not path.is_file() or path.is_symlink():
        raise ValueError(f"training script is not a regular file: {path}")
    try:
        source = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise ValueError(f"cannot read training script: {error}") from error
    matches = re.findall(
        r"^[ \t]*(?:export[ \t]+)?NNODES=([0-9]+)[ \t]*(?:#.*)?$",
        source,
        flags=re.MULTILINE,
    )
    if len(matches) != 1 or int(matches[0]) <= 0:
        raise ValueError("training script must have one positive literal NNODES")
    return int(matches[0])


def source_checkpoint_save_dir(path: Path) -> str | None:
    try:
        source = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise ValueError(f"cannot read training script: {error}") from error
    if re.search(
        r"^[ \t]*--save(?:[ \t=]|$)",
        source,
        flags=re.MULTILINE,
    ) is None:
        return None
    matches = re.findall(
        r"^[ \t]*(?:export[ \t]+)?CKPT_SAVE_DIR="
        r'(?:\"([^\"]+)\"|\'([^\']+)\'|([^ \t#]+))'
        r"[ \t]*(?:#.*)?$",
        source,
        flags=re.MULTILINE,
    )
    if len(matches) != 1:
        raise ValueError(
            "active --save requires one literal CKPT_SAVE_DIR assignment"
        )
    save_dir = next(part for part in matches[0] if part)
    if not save_dir.startswith("/"):
        raise ValueError("CKPT_SAVE_DIR must be an absolute path")
    return save_dir


def retain_then_delete_cluster(
    args: argparse.Namespace,
) -> None:
    retention_seconds = args.failure_retention_seconds
    if retention_seconds < 0:
        print("FAILED RESOURCE RETENTION: RayCluster is retained indefinitely.")
        return
    print(
        "FAILED RESOURCE RETENTION: parameter injection failed; "
        f"keeping RayCluster for {retention_seconds} seconds."
    )
    deadline = time.monotonic() + retention_seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(60.0, remaining))
    kubectl = shlex.split(args.kubectl_command)
    if not kubectl:
        raise RuntimeError("kubectl command is empty")
    kubectl.extend(("--kubeconfig", str(args.kubeconfig)))
    command = [
        *kubectl,
        "delete",
        "raycluster",
        args.cluster,
        "-n",
        args.namespace,
        "--ignore-not-found=true",
        "--wait=false",
    ]
    print("$ " + shlex.join(command), flush=True)
    result = subprocess.run(
        command,
        check=False,
        shell=False,
        timeout=60,
    )
    if result.returncode != 0:
        raise RuntimeError("RayCluster cleanup command failed")


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--node",
        action="append",
        help=(
            "target node name or InternalIP; repeat as needed "
            "(defaults to gpu-server-00/01 IPs)"
        ),
    )
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument(
        "--runtime-source-dir",
        type=Path,
        default=DEFAULT_RUNTIME_SOURCE,
    )
    parser.add_argument(
        "--runtime-configmap",
        default="pretrain-gpu00-gpu01-hccl-runtime",
    )
    parser.add_argument(
        "--kubectl-command",
        default="/usr/local/bin/k3s kubectl",
    )
    parser.add_argument(
        "--kubeconfig",
        type=Path,
        default=Path("/home/ywj/.kube/k3s-learning.yaml"),
    )
    parser.add_argument("--namespace", default="pretrain-ray")
    parser.add_argument("--cluster", default="pretrain-gpu00-gpu01")
    parser.add_argument(
        "--expected-workers",
        type=int,
        help="optional assertion; normally derived from the repeated --node values",
    )
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument("--hccl-timeout-seconds", type=int, default=3600)
    parser.add_argument(
        "--expected-world-size",
        type=int,
        help="optional strict rank count; omitted uses HCCL topology discovery",
    )
    parser.add_argument(
        "--hccl-evidence-root",
        type=Path,
        default=DEFAULT_EVIDENCE_ROOT,
    )
    parser.add_argument(
        "--failure-retention-seconds",
        type=int,
        default=1800,
        help="failed Ray/HCCL startup retention; -1 keeps resources indefinitely",
    )
    parser.add_argument(
        "--run-id",
        help="caller-owned run ID; omitted generates a unique ID",
    )
    parser.add_argument(
        "--train-script",
        type=Path,
        default=DEFAULT_TRAIN_SCRIPT,
        help="formal training template copied and injected per worker",
    )
    parser.add_argument(
        "--training-cwd",
        default=DEFAULT_TRAINING_CWD,
        help="absolute training source directory inside every Ray worker",
    )
    parser.add_argument(
        "--training-artifact-root",
        type=Path,
        default=DEFAULT_TRAINING_ARTIFACT_ROOT,
    )
    parser.add_argument(
        "--master-port",
        type=int,
        help="optional override; omitted preserves the source script value",
    )
    parser.add_argument(
        "--allow-topology-change",
        action="store_true",
        help="explicitly approve discovered NNODES/NPUS differing from the source",
    )
    parser.add_argument(
        "--confirm-checkpoint-exclusive",
        action="store_true",
        help="attest that no other job writes the source CKPT_SAVE_DIR",
    )
    parser.add_argument(
        "--training-timeout-seconds",
        type=int,
        default=0,
        help="per-worker formal training timeout; 0 means no timeout",
    )
    parser.add_argument(
        "--keep-success-resources",
        action="store_true",
        help="leave the RayCluster allocated after successful training",
    )
    return parser


def build_stage_commands(
    args: argparse.Namespace,
    *,
    run_id: str,
) -> tuple[Stage, ...]:
    nodes = tuple(args.node or DEFAULT_NODES)
    worker_count = len(nodes)
    script_dir = Path(__file__).resolve().parent
    hccl_run_dir = args.hccl_evidence_root.resolve() / run_id
    training_run_dir = args.training_artifact_root.resolve() / run_id
    injection_dir = training_run_dir / "injection"
    result_path = training_run_dir / "execution-result.json"
    rendered_manifest = training_run_dir / "raycluster.yaml"

    preflight_command = [
        sys.executable,
        str(script_dir / "environment_check.py"),
    ]
    for node in nodes:
        preflight_command.extend(("--node", node))
    preflight_command.extend(
        (
            "--kubectl-command",
            args.kubectl_command,
            "--kubeconfig",
            str(args.kubeconfig),
        )
    )

    render_command = [
        sys.executable,
        str(script_dir / "render_raycluster.py"),
        "--base-manifest",
        str(args.manifest.resolve()),
        "--output-manifest",
        str(rendered_manifest),
        "--kubectl-command",
        args.kubectl_command,
        "--kubeconfig",
        str(args.kubeconfig),
        "--namespace",
        args.namespace,
        "--cluster",
        args.cluster,
        "--runtime-configmap",
        args.runtime_configmap,
        "--run-id",
        run_id,
    ]
    for node in nodes:
        render_command.extend(("--node", node))

    launch_command = [
        sys.executable,
        str(script_dir / "ray_cluster_start.py"),
        "--manifest",
        str(rendered_manifest),
        "--runtime-source-dir",
        str(args.runtime_source_dir.resolve()),
        "--runtime-configmap",
        args.runtime_configmap,
        "--kubectl-command",
        args.kubectl_command,
        "--kubeconfig",
        str(args.kubeconfig),
        "--namespace",
        args.namespace,
        "--cluster",
        args.cluster,
        "--expected-workers",
        str(worker_count),
        "--timeout-seconds",
        str(args.timeout_seconds),
        "--failure-retention-seconds",
        str(args.failure_retention_seconds),
    ]
    hccl_command = [
        sys.executable,
        str(script_dir / "hccl_gate.py"),
        "--kubectl-command",
        args.kubectl_command,
        "--kubeconfig",
        str(args.kubeconfig),
        "--namespace",
        args.namespace,
        "--cluster",
        args.cluster,
        "--expected-workers",
        str(worker_count),
        "--timeout-seconds",
        str(args.hccl_timeout_seconds),
        "--evidence-root",
        str(args.hccl_evidence_root.resolve()),
        "--run-id",
        run_id,
        "--failure-retention-seconds",
        str(args.failure_retention_seconds),
    ]
    if args.expected_world_size is not None:
        hccl_command.extend(
            ("--expected-world-size", str(args.expected_world_size))
        )

    injection_command = [
        sys.executable,
        str(script_dir / "inject_training_params.py"),
        "--source",
        str(args.train_script.resolve()),
        "--hccl-evidence",
        str(hccl_run_dir / "03-hccl.json"),
        "--hccl-ping-evidence",
        str(hccl_run_dir / "01-ping.json"),
        "--output-dir",
        str(injection_dir),
        "--run-id",
        run_id,
        "--training-cwd",
        args.training_cwd,
    ]
    if args.master_port is not None:
        injection_command.extend(("--master-port", str(args.master_port)))
    if args.allow_topology_change:
        injection_command.append("--allow-topology-change")
    if args.confirm_checkpoint_exclusive:
        injection_command.append("--confirm-checkpoint-exclusive")
    if getattr(args, "require_resumable_checkpoint", False):
        injection_command.append("--require-resumable-checkpoint")

    training_command = [
        sys.executable,
        str(script_dir / "ray_training_submit.py"),
        "--injection-dir",
        str(injection_dir),
        "--kubectl-command",
        args.kubectl_command,
        "--kubeconfig",
        str(args.kubeconfig),
        "--namespace",
        args.namespace,
        "--cluster",
        args.cluster,
        "--result",
        str(result_path),
        "--timeout-seconds",
        str(args.training_timeout_seconds),
        "--failure-retention-seconds",
        str(args.failure_retention_seconds),
    ]
    if args.keep_success_resources:
        training_command.append("--keep-success-resources")
    return (
        ("environment check", preflight_command),
        ("per-run RayCluster rendering", render_command),
        ("Ray cluster startup", launch_command),
        ("topology, RankTable, and HCCL gate", hccl_command),
        ("formal training parameter injection", injection_command),
        ("formal Ray training", training_command),
    )


def validate_args(args: argparse.Namespace, run_id: str) -> None:
    if RUN_ID_PATTERN.fullmatch(run_id) is None:
        raise ValueError("run ID contains unsupported characters")
    if (
        args.timeout_seconds <= 0
        or args.hccl_timeout_seconds <= 0
    ):
        raise ValueError("workers and startup/HCCL timeouts must be positive")
    nodes = tuple(args.node or DEFAULT_NODES)
    if len(set(nodes)) != len(nodes):
        raise ValueError("worker node targets must be unique")
    if args.expected_workers is not None and args.expected_workers != len(nodes):
        raise ValueError(
            "expected workers differs from the number of selected --node targets"
        )
    source_workers = source_declared_workers(args.train_script.resolve())
    if source_workers != len(nodes) and not args.allow_topology_change:
        raise ValueError(
            "selected worker count differs from source NNODES "
            f"({source_workers} -> {len(nodes)}); "
            "--allow-topology-change is required before any cluster is started"
        )
    checkpoint_save_dir = source_checkpoint_save_dir(
        args.train_script.resolve()
    )
    if (
        checkpoint_save_dir is not None
        and not args.confirm_checkpoint_exclusive
    ):
        raise ValueError(
            "training writes checkpoints to "
            f"{checkpoint_save_dir}; --confirm-checkpoint-exclusive is "
            "required before any cluster is started"
        )
    if (
        args.expected_world_size is not None
        and args.expected_world_size <= 0
    ):
        raise ValueError("expected world size must be positive")
    if args.failure_retention_seconds < -1:
        raise ValueError("failure retention must be -1 or non-negative")
    if args.training_timeout_seconds < 0:
        raise ValueError("training timeout must be zero or positive")
    if args.master_port is not None and not 1024 <= args.master_port <= 65535:
        raise ValueError("master port must be within 1024..65535")
    if not args.training_cwd.startswith("/"):
        raise ValueError("training cwd must be an absolute worker path")


def main(argv: Sequence[str] | None = None) -> int:
    args = make_parser().parse_args(argv)
    run_id = args.run_id or new_run_id()
    try:
        validate_args(args, run_id)
        stages = build_stage_commands(args, run_id=run_id)
    except ValueError as error:
        print(f"STOP: invalid workflow arguments: {error}", file=sys.stderr)
        return 2
    print(f"Run ID: {run_id}", flush=True)
    print(
        "Training result: "
        f"{args.training_artifact_root.resolve() / run_id / 'execution-result.json'}",
        flush=True,
    )
    try:
        return execute_pipeline(
            stages,
            failure_handler=(
                lambda _index, name: retain_then_delete_cluster(args)
                if name == "formal training parameter injection"
                else None
            ),
        )
    except KeyboardInterrupt:
        print(
            "STOP: interrupted; existing Ray resources and checkpoints were left in place.",
            file=sys.stderr,
        )
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
