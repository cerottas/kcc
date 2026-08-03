#!/usr/bin/env python3
"""Submit injected formal training scripts to the ready Ray cluster."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time
from typing import Any, Mapping, Sequence


BUNDLE_DIR = Path(__file__).resolve().parent
DRIVER_PATH = BUNDLE_DIR / "ray_training_driver.py"
DEFAULT_KUBECONFIG = Path("/home/ywj/.kube/k3s-learning.yaml")
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class SubmitError(RuntimeError):
    pass


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def require_file(path: Path, label: str) -> None:
    if not path.is_file() or path.is_symlink():
        raise SubmitError(f"{label} is not a regular file: {path}")


def load_injection(
    injection_dir: Path,
) -> tuple[Mapping[str, Any], tuple[Path, ...]]:
    manifest_path = injection_dir / "injection.json"
    require_file(manifest_path, "injection manifest")
    try:
        injection = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise SubmitError(f"cannot parse injection manifest: {error}") from error
    if (
        not isinstance(injection, dict)
        or injection.get("schemaVersion") != "training-injection/v1"
    ):
        raise SubmitError("unsupported injection manifest")
    run_id = injection.get("runId")
    if not isinstance(run_id, str) or RUN_ID_PATTERN.fullmatch(run_id) is None:
        raise SubmitError("injection runId is invalid")
    topology = injection.get("topology")
    nodes = injection.get("nodes")
    if not isinstance(topology, dict) or not isinstance(nodes, list):
        raise SubmitError("injection topology or nodes are invalid")
    worker_count = topology.get("workers")
    if not isinstance(worker_count, int) or worker_count <= 0:
        raise SubmitError("injection worker count is invalid")
    if len(nodes) != worker_count:
        raise SubmitError("injection node count differs from topology")

    scripts_dir = injection_dir / "scripts"
    scripts: list[Path] = []
    ranks: set[int] = set()
    for node in nodes:
        if not isinstance(node, dict):
            raise SubmitError("injection node must be an object")
        node_rank = node.get("nodeRank")
        if (
            not isinstance(node_rank, int)
            or not 0 <= node_rank < worker_count
            or node_rank in ranks
        ):
            raise SubmitError("injection node ranks are invalid or duplicated")
        ranks.add(node_rank)
        script_name = node.get("script")
        if not isinstance(script_name, str) or Path(script_name).name != script_name:
            raise SubmitError("injection script name is invalid")
        script_path = scripts_dir / script_name
        require_file(script_path, f"node rank {node_rank} script")
        expected_sha256 = node.get("scriptSha256")
        if (
            not isinstance(expected_sha256, str)
            or SHA256_PATTERN.fullmatch(expected_sha256) is None
            or sha256_file(script_path) != expected_sha256
        ):
            raise SubmitError(f"node rank {node_rank} script digest differs")
        scripts.append(script_path)
    if ranks != set(range(worker_count)):
        raise SubmitError("injection node ranks are not contiguous")
    require_file(DRIVER_PATH, "Ray training driver")
    return injection, tuple(scripts)


def kubectl_prefix(command_text: str, kubeconfig: Path | None) -> list[str]:
    command = shlex.split(command_text)
    if not command:
        raise SubmitError("kubectl command is empty")
    if kubeconfig is not None:
        command.extend(("--kubeconfig", str(kubeconfig)))
    return command


def run_command(
    command: Sequence[str],
    *,
    timeout: int | None,
    check: bool = True,
    print_output: bool = True,
) -> subprocess.CompletedProcess[str]:
    print("$ " + shlex.join(command), flush=True)
    try:
        result = subprocess.run(
            list(command),
            check=False,
            shell=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise SubmitError(f"cannot run {shlex.join(command)}: {error}") from error
    if print_output and result.stdout:
        print(result.stdout.rstrip())
    if print_output and result.stderr:
        print(result.stderr.rstrip(), file=sys.stderr)
    if check and result.returncode != 0:
        raise SubmitError(
            f"command failed with exit code {result.returncode}: "
            f"{shlex.join(command)}"
        )
    return result


def find_ready_head(
    kubectl: Sequence[str],
    *,
    namespace: str,
    cluster: str,
) -> str:
    result = run_command(
        [
            *kubectl,
            "get",
            "pods",
            "-n",
            namespace,
            "-l",
            f"ray.io/cluster={cluster},ray.io/node-type=head",
            "-o",
            "json",
        ],
        timeout=30,
        print_output=False,
    )
    try:
        pods = json.loads(result.stdout).get("items", [])
    except (AttributeError, json.JSONDecodeError) as error:
        raise SubmitError(f"cannot parse Ray head Pod response: {error}") from error
    ready = [
        pod
        for pod in pods
        if any(
            condition.get("type") == "Ready"
            and condition.get("status") == "True"
            for condition in pod.get("status", {}).get("conditions", [])
        )
    ]
    if len(ready) != 1:
        raise SubmitError(f"expected one ready Ray head Pod, found {len(ready)}")
    pod_name = ready[0].get("metadata", {}).get("name")
    if not isinstance(pod_name, str) or not pod_name:
        raise SubmitError("Ray head Pod name is missing")
    return pod_name


def write_result_create_only(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("x", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
    except FileExistsError as error:
        raise SubmitError(f"refusing to overwrite result: {path}") from error


def export_result(
    kubectl: Sequence[str],
    *,
    namespace: str,
    head_pod: str,
    remote_result: str,
    local_result: Path,
) -> None:
    exported = run_command(
        [
            *kubectl,
            "exec",
            "-n",
            namespace,
            head_pod,
            "-c",
            "ray-head",
            "--",
            "cat",
            remote_result,
        ],
        timeout=120,
        print_output=False,
    )
    try:
        payload = json.loads(exported.stdout)
    except json.JSONDecodeError as error:
        raise SubmitError(f"remote training result is invalid JSON: {error}") from error
    if (
        not isinstance(payload, dict)
        or payload.get("schemaVersion") != "ray-training-result/v1"
    ):
        raise SubmitError("remote training result has an unsupported schema")
    write_result_create_only(local_result, payload)


def delete_ray_cluster(
    kubectl: Sequence[str],
    *,
    namespace: str,
    cluster: str,
) -> None:
    result = run_command(
        [
            *kubectl,
            "delete",
            "raycluster",
            cluster,
            "-n",
            namespace,
            "--ignore-not-found=true",
            "--wait=false",
        ],
        timeout=60,
        check=False,
    )
    if result.returncode != 0:
        raise SubmitError(f"cannot delete RayCluster {namespace}/{cluster}")


def retain_then_delete_failed_cluster(
    kubectl: Sequence[str],
    *,
    namespace: str,
    cluster: str,
    retention_seconds: int,
) -> None:
    if retention_seconds < 0:
        print("FAILED RESOURCE RETENTION: RayCluster is retained indefinitely.")
        return
    print(
        "FAILED RESOURCE RETENTION: "
        f"keeping RayCluster and Pods for {retention_seconds} seconds."
    )
    deadline = time.monotonic() + retention_seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(60.0, remaining))
    print(
        "FAILED RESOURCE CLEANUP: "
        f"deleting RayCluster {namespace}/{cluster}; checkpoints are untouched."
    )
    delete_ray_cluster(kubectl, namespace=namespace, cluster=cluster)


def prepare_remote_submission(
    kubectl: Sequence[str],
    *,
    namespace: str,
    cluster: str,
    run_id: str,
    injection_dir: Path,
    scripts: Sequence[Path],
) -> tuple[str, str]:
    head_pod = find_ready_head(kubectl, namespace=namespace, cluster=cluster)
    remote_parent = "/tmp/pretrain-ray-submit"
    remote_dir = f"{remote_parent}/{run_id}"
    remote_target = f"{namespace}/{head_pod}:{remote_dir}"
    run_command(
        [
            *kubectl,
            "exec",
            "-n",
            namespace,
            head_pod,
            "-c",
            "ray-head",
            "--",
            "mkdir",
            "-p",
            "--",
            remote_parent,
        ],
        timeout=30,
    )
    run_command(
        [
            *kubectl,
            "exec",
            "-n",
            namespace,
            head_pod,
            "-c",
            "ray-head",
            "--",
            "mkdir",
            "--",
            remote_dir,
        ],
        timeout=30,
    )
    transfer_files = [
        injection_dir / "injection.json",
        *scripts,
        DRIVER_PATH,
    ]
    for source in transfer_files:
        run_command(
            [
                *kubectl,
                "cp",
                str(source),
                f"{remote_target}/{source.name}",
                "-c",
                "ray-head",
            ],
            timeout=120,
        )
    return head_pod, remote_dir


def submit(
    *,
    injection_dir: Path,
    injection: Mapping[str, Any],
    scripts: Sequence[Path],
    kubectl_command: str,
    kubeconfig: Path | None,
    namespace: str,
    cluster: str,
    result_path: Path,
    timeout_seconds: int,
    failure_retention_seconds: int,
    keep_success_resources: bool,
) -> None:
    if result_path.exists() or result_path.is_symlink():
        raise SubmitError(f"refusing to overwrite result: {result_path}")
    kubectl = kubectl_prefix(kubectl_command, kubeconfig)
    run_id = str(injection["runId"])
    try:
        head_pod, remote_dir = prepare_remote_submission(
            kubectl,
            namespace=namespace,
            cluster=cluster,
            run_id=run_id,
            injection_dir=injection_dir,
            scripts=scripts,
        )
    except SubmitError:
        retain_then_delete_failed_cluster(
            kubectl,
            namespace=namespace,
            cluster=cluster,
            retention_seconds=failure_retention_seconds,
        )
        raise

    remote_result = f"{remote_dir}/execution-result.json"
    driver_command = [
        *kubectl,
        "exec",
        "-n",
        namespace,
        head_pod,
        "-c",
        "ray-head",
        "--",
        "/home/ray/anaconda3/bin/python",
        f"{remote_dir}/{DRIVER_PATH.name}",
        "--injection",
        f"{remote_dir}/injection.json",
        "--scripts-dir",
        remote_dir,
        "--result",
        remote_result,
        "--timeout-seconds",
        str(timeout_seconds),
    ]
    driver_result: subprocess.CompletedProcess[str] | None = None
    driver_error: SubmitError | None = None
    try:
        driver_result = run_command(
            driver_command,
            timeout=None,
            check=False,
        )
    except SubmitError as error:
        driver_error = error

    export_error: SubmitError | None = None
    try:
        export_result(
            kubectl,
            namespace=namespace,
            head_pod=head_pod,
            remote_result=remote_result,
            local_result=result_path,
        )
    except SubmitError as error:
        export_error = error

    if driver_error is not None:
        suffix = f"; result export also failed: {export_error}" if export_error else ""
        print(
            "FAILED RESOURCE RETENTION: training state is uncertain; "
            "RayCluster is retained indefinitely.",
            file=sys.stderr,
        )
        raise SubmitError(f"training driver did not complete: {driver_error}{suffix}")
    if driver_result is None:
        raise SubmitError("training driver did not return")
    if driver_result.returncode != 0:
        suffix = (
            f"; failure result exported to {result_path}"
            if export_error is None
            else f"; result export failed: {export_error}"
        )
        if export_error is None:
            retain_then_delete_failed_cluster(
                kubectl,
                namespace=namespace,
                cluster=cluster,
                retention_seconds=failure_retention_seconds,
            )
        else:
            print(
                "FAILED RESOURCE RETENTION: result export failed; "
                "RayCluster is retained indefinitely.",
                file=sys.stderr,
            )
        raise SubmitError(
            f"training driver failed with exit code {driver_result.returncode}{suffix}"
        )
    if export_error is not None:
        print(
            "FAILED RESOURCE RETENTION: successful result could not be exported; "
            "RayCluster is retained indefinitely.",
            file=sys.stderr,
        )
        raise export_error
    if keep_success_resources:
        print("SUCCESS RESOURCE RETENTION: RayCluster was left in place.")
    else:
        print(
            "SUCCESS RESOURCE CLEANUP: deleting the RayCluster; "
            "all checkpoints and training logs are retained."
        )
        delete_ray_cluster(kubectl, namespace=namespace, cluster=cluster)
    print(f"PASS: training result exported to {result_path}")


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--injection-dir", type=Path, required=True)
    parser.add_argument(
        "--kubectl-command",
        default="/usr/local/bin/k3s kubectl",
    )
    parser.add_argument("--kubeconfig", type=Path, default=DEFAULT_KUBECONFIG)
    parser.add_argument("--namespace", default="pretrain-ray")
    parser.add_argument("--cluster", default="pretrain-gpu00-gpu01")
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=0,
        help="per-worker training timeout; 0 means no timeout",
    )
    parser.add_argument(
        "--failure-retention-seconds",
        type=int,
        default=1800,
        help="failed training retention; -1 keeps the RayCluster indefinitely",
    )
    parser.add_argument(
        "--keep-success-resources",
        action="store_true",
        help="do not delete the RayCluster after successful training",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = make_parser().parse_args(argv)
    try:
        if args.timeout_seconds < 0 or args.failure_retention_seconds < -1:
            raise SubmitError(
                "timeout must be zero or positive and retention must be -1 or non-negative"
            )
        injection_dir = args.injection_dir.resolve()
        injection, scripts = load_injection(injection_dir)
        submit(
            injection_dir=injection_dir,
            injection=injection,
            scripts=scripts,
            kubectl_command=args.kubectl_command,
            kubeconfig=args.kubeconfig.resolve(),
            namespace=args.namespace,
            cluster=args.cluster,
            result_path=args.result.resolve(),
            timeout_seconds=args.timeout_seconds,
            failure_retention_seconds=args.failure_retention_seconds,
            keep_success_resources=args.keep_success_resources,
        )
    except (SubmitError, OSError, UnicodeError) as error:
        print(f"STOP: Ray training submission failed: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print(
            "STOP: interrupted; Ray resources and checkpoints were left in place.",
            file=sys.stderr,
        )
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
