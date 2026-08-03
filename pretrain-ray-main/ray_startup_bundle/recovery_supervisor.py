#!/usr/bin/env python3
"""Recover a failed whole-world training run with a bounded spare-node pool."""

from __future__ import annotations

import argparse
import copy
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import secrets
import shlex
import subprocess
import sys
import time
from typing import Any, Mapping, Sequence

import recovery_diagnostics
import start_ray


DEFAULT_STATE_ROOT = start_ray.DEFAULT_LOG_ROOT / "training-jobs"


class RecoveryError(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def attempt_run_id(job_id: str, attempt: int) -> str:
    if attempt < 0:
        raise RecoveryError("attempt must be non-negative")
    candidate = f"{job_id}-a{attempt:02d}"
    if start_ray.RUN_ID_PATTERN.fullmatch(candidate) is None:
        raise RecoveryError(
            "job ID cannot form a valid attempt run ID; shorten the job ID"
        )
    return candidate


def replace_active_node(
    active_nodes: Sequence[str],
    spare_nodes: Sequence[str],
    *,
    bad_target: str,
    replacement_target: str,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    active = tuple(active_nodes)
    spares = tuple(spare_nodes)
    if active.count(bad_target) != 1:
        raise RecoveryError("diagnosed bad target is not exactly one active node")
    if replacement_target not in spares:
        raise RecoveryError("selected replacement is not in the spare pool")
    updated_active = tuple(
        replacement_target if target == bad_target else target for target in active
    )
    updated_spares = tuple(target for target in spares if target != replacement_target)
    if len(set(updated_active)) != len(updated_active):
        raise RecoveryError("replacement produced duplicate active nodes")
    return updated_active, updated_spares


def write_state(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(4)}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def load_training_result(
    path: Path,
    *,
    expected_run_id: str,
    expected_status: str,
) -> Mapping[str, Any]:
    if expected_status not in {"PASS", "FAIL"}:
        raise RecoveryError("expected training result status is invalid")
    if not path.is_file() or path.is_symlink():
        raise RecoveryError(
            "formal training did not export a regular execution-result.json; "
            "training state is uncertain"
        )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise RecoveryError(f"cannot read formal training result: {error}") from error
    if (
        not isinstance(payload, dict)
        or payload.get("schemaVersion") != "ray-training-result/v1"
        or payload.get("status") != expected_status
        or payload.get("runId") != expected_run_id
    ):
        raise RecoveryError(
            "execution result is not a formal "
            f"{expected_status} for the current attempt"
        )
    return payload


def load_failed_training_result(
    path: Path,
    *,
    expected_run_id: str,
) -> Mapping[str, Any]:
    return load_training_result(
        path,
        expected_run_id=expected_run_id,
        expected_status="FAIL",
    )


def load_successful_training_result(
    path: Path,
    *,
    expected_run_id: str,
) -> Mapping[str, Any]:
    return load_training_result(
        path,
        expected_run_id=expected_run_id,
        expected_status="PASS",
    )


def require_successful_checkpoint_resume(
    result: Mapping[str, Any],
    *,
    expected_workers: int,
) -> None:
    checkpoint = result.get("checkpoint")
    if not isinstance(checkpoint, Mapping) or checkpoint.get("status") != "PASS":
        raise RecoveryError(
            "successful recovery result lacks a passed checkpoint preflight"
        )
    policy = checkpoint.get("policy")
    if (
        not isinstance(policy, Mapping)
        or policy.get("requiredForRecovery") is not True
        or policy.get("selection") != "megatron-tracker"
    ):
        raise RecoveryError("successful recovery result has an invalid checkpoint policy")
    selected_iteration = checkpoint.get("selectedIteration")
    selected_dir = checkpoint.get("selectedDir")
    if (
        not isinstance(selected_iteration, int)
        or isinstance(selected_iteration, bool)
        or selected_iteration <= 0
        or not isinstance(selected_dir, str)
        or not selected_dir.startswith("/")
    ):
        raise RecoveryError("successful recovery result has no resumable checkpoint")
    views = checkpoint.get("nodes")
    if not isinstance(views, list) or len(views) != expected_workers:
        raise RecoveryError(
            "successful recovery result lacks checkpoint evidence from every worker"
        )
    for view in views:
        if (
            not isinstance(view, Mapping)
            or view.get("status") != "AVAILABLE_RESUME"
            or view.get("iteration") != selected_iteration
            or view.get("selectedDir") != selected_dir
        ):
            raise RecoveryError(
                "successful recovery result contains inconsistent checkpoint evidence"
            )


def kubectl_prefix(command_text: str, kubeconfig: Path | None) -> list[str]:
    command = shlex.split(command_text)
    if not command:
        raise RecoveryError("kubectl command is empty")
    if kubeconfig is not None:
        command.extend(("--kubeconfig", str(kubeconfig)))
    return command


def run_cleanup_query(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            list(command),
            check=False,
            shell=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise RecoveryError(f"cannot query failed Ray resources: {error}") from error
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RecoveryError(f"failed Ray resource query returned an error: {detail}")
    return result


def wait_for_cluster_cleanup(
    *,
    kubectl_command: str,
    kubeconfig: Path | None,
    namespace: str,
    cluster: str,
    run_id: str,
    timeout_seconds: int,
    poll_seconds: float = 2.0,
) -> None:
    kubectl = kubectl_prefix(kubectl_command, kubeconfig)
    # ``ray_training_submit.py`` normally requests this deletion first.  If
    # the object still exists, verify the run-id annotation written by the
    # existing renderer before repeating that same idempotent delete.  A
    # different annotation means another launcher owns the cluster name.
    current_cluster = run_cleanup_query(
        [
            *kubectl,
            "get",
            "raycluster",
            cluster,
            "-n",
            namespace,
            "--ignore-not-found",
            "-o",
            "json",
        ]
    )
    if current_cluster.stdout.strip():
        try:
            cluster_document = json.loads(current_cluster.stdout)
            annotations = cluster_document.get("metadata", {}).get(
                "annotations", {}
            )
        except (AttributeError, json.JSONDecodeError) as error:
            raise RecoveryError(
                f"cannot parse failed RayCluster ownership: {error}"
            ) from error
        if not isinstance(annotations, Mapping):
            raise RecoveryError("failed RayCluster annotations are not an object")
        if annotations.get("trainctl.io/run-id") != run_id:
            raise RecoveryError(
                "refusing to delete a RayCluster whose run-id annotation "
                "does not match the failed attempt"
            )
        # Deleting the RayCluster lets Kubernetes stop only that cluster's
        # Pods; this intentionally does not scan or kill arbitrary host PIDs.
        run_cleanup_query(
            [
                *kubectl,
                "delete",
                "raycluster",
                cluster,
                "-n",
                namespace,
                "--ignore-not-found=true",
                "--wait=false",
            ]
        )
    deadline = time.monotonic() + timeout_seconds
    last_reason = "cleanup has not been observed"
    while time.monotonic() < deadline:
        cluster_result = run_cleanup_query(
            [
                *kubectl,
                "get",
                "raycluster",
                cluster,
                "-n",
                namespace,
                "--ignore-not-found",
                "-o",
                "name",
            ]
        )
        pod_result = run_cleanup_query(
            [
                *kubectl,
                "get",
                "pods",
                "-n",
                namespace,
                "-l",
                f"ray.io/cluster={cluster}",
                "-o",
                "json",
            ]
        )
        service_result = run_cleanup_query(
            [
                *kubectl,
                "get",
                "service",
                "-n",
                namespace,
                "-l",
                f"ray.io/cluster={cluster}",
                "-o",
                "json",
            ]
        )
        configmap_result = run_cleanup_query(
            [
                *kubectl,
                "get",
                "configmap",
                f"job-summary-{cluster}",
                f"hccl-sanitized-{cluster}",
                "-n",
                namespace,
                "--ignore-not-found",
                "-o",
                "name",
            ]
        )
        try:
            pods = json.loads(pod_result.stdout).get("items", [])
            services = json.loads(service_result.stdout).get("items", [])
        except (AttributeError, json.JSONDecodeError) as error:
            raise RecoveryError(
                f"cannot parse cleanup Pod/Service response: {error}"
            ) from error
        remaining: list[str] = []
        if cluster_result.stdout.strip():
            remaining.append("RayCluster")
        if pods:
            remaining.append(f"{len(pods)} Pod(s)")
        if services:
            remaining.append(f"{len(services)} Service(s)")
        if configmap_result.stdout.strip():
            remaining.append("RankTable ConfigMap(s)")
        if not remaining:
            return
        last_reason = ", ".join(remaining) + " still exist"
        time.sleep(poll_seconds)
    raise RecoveryError(
        f"timed out after {timeout_seconds}s waiting for failed cluster cleanup: "
        f"{last_reason}"
    )


def source_declared_npus(path: Path) -> int:
    try:
        source = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise RecoveryError(f"cannot read training script: {error}") from error
    import re

    matches = re.findall(
        r"^[ \t]*(?:export[ \t]+)?NPUS_PER_NODE=([0-9]+)[ \t]*(?:#.*)?$",
        source,
        flags=re.MULTILINE,
    )
    if len(matches) != 1 or int(matches[0]) <= 0:
        raise RecoveryError(
            "training script must have one positive literal NPUS_PER_NODE"
        )
    return int(matches[0])


def make_parser() -> argparse.ArgumentParser:
    parser = start_ray.make_parser()
    parser.description = __doc__
    parser.add_argument(
        "--spare-node",
        action="append",
        required=True,
        help="standby Kubernetes node name or InternalIP; repeat as needed",
    )
    parser.add_argument(
        "--max-recoveries",
        type=int,
        help=(
            "maximum number of spare machines that may be consumed; "
            "defaults to the initial spare count"
        ),
    )
    parser.add_argument(
        "--cleanup-timeout-seconds",
        type=int,
        default=300,
        help=(
            "wait for the failed RayCluster, Pods, Service, and RankTable "
            "objects to disappear"
        ),
    )
    parser.add_argument(
        "--recovery-state-root",
        type=Path,
        default=DEFAULT_STATE_ROOT,
        help="persistent root for logical-job recovery state",
    )
    return parser


def validate_node_pool(
    active_nodes: Sequence[str],
    spare_nodes: Sequence[str],
) -> None:
    combined = tuple(active_nodes) + tuple(spare_nodes)
    if not active_nodes:
        raise RecoveryError("at least one active node is required")
    if not spare_nodes:
        raise RecoveryError("at least one spare node is required")
    if len(set(combined)) != len(combined):
        raise RecoveryError("active and spare node targets must be unique")


def run_supervisor(args: argparse.Namespace) -> int:
    job_id = args.run_id or start_ray.new_run_id()
    active_nodes = tuple(args.node or start_ray.DEFAULT_NODES)
    spare_nodes = tuple(args.spare_node)
    validate_node_pool(active_nodes, spare_nodes)
    max_replacements = (
        len(spare_nodes) if args.max_recoveries is None else args.max_recoveries
    )
    if not 0 <= max_replacements <= len(spare_nodes):
        raise RecoveryError(
            "max recoveries must be between zero and the initial spare count"
        )
    if args.cleanup_timeout_seconds <= 0:
        raise RecoveryError("cleanup timeout must be positive")
    expected_npus = source_declared_npus(args.train_script.resolve())
    # Validate the longest possible attempt ID before creating persistent state.
    # Each recovery round consumes at least one spare, so it cannot exceed this.
    attempt_run_id(job_id, max_replacements)
    for possible_attempt in range(max_replacements + 1):
        possible_result = (
            args.training_artifact_root.resolve()
            / attempt_run_id(job_id, possible_attempt)
            / "execution-result.json"
        )
        if possible_result.exists() or possible_result.is_symlink():
            raise RecoveryError(
                "an execution result already exists for a possible attempt; "
                f"choose a new logical run ID: {possible_result}"
            )

    state_dir = args.recovery_state_root.resolve() / job_id
    try:
        state_dir.mkdir(parents=True, exist_ok=False)
    except FileExistsError as error:
        raise RecoveryError(
            f"recovery job state already exists; refusing a duplicate supervisor: {state_dir}"
        ) from error
    state_path = state_dir / "state.json"
    state: dict[str, Any] = {
        "schemaVersion": "ray-training-recovery/v1",
        "jobId": job_id,
        "status": "STARTING",
        "activeNodes": list(active_nodes),
        "spareNodes": list(spare_nodes),
        "initialSpareCount": len(spare_nodes),
        "remainingSpareCount": len(spare_nodes),
        "maxReplacementCount": max_replacements,
        "replacementCount": 0,
        "quarantinedNodes": [],
        "attempts": [],
        "createdAt": utc_now(),
        "updatedAt": utc_now(),
    }
    write_state(state_path, state)
    print(f"Recovery job ID: {job_id}", flush=True)
    print(f"Recovery state: {state_path}", flush=True)
    if args.failure_retention_seconds != 0:
        print(
            "RECOVERY MODE: failed-resource retention is forced to 0 seconds; "
            "diagnosis starts only after cleanup completes.",
            file=sys.stderr,
        )

    quarantined: list[str] = []
    replacements_used = 0
    attempt = 0
    while True:
        run_id = attempt_run_id(job_id, attempt)
        attempt_args = copy.copy(args)
        attempt_args.node = list(active_nodes)
        attempt_args.failure_retention_seconds = 0
        attempt_args.require_resumable_checkpoint = True
        start_ray.validate_args(attempt_args, run_id)
        stages = start_ray.build_stage_commands(attempt_args, run_id=run_id)
        result_path = (
            attempt_args.training_artifact_root.resolve()
            / run_id
            / "execution-result.json"
        )
        attempt_state: dict[str, Any] = {
            "attempt": attempt,
            "runId": run_id,
            "status": "RUNNING",
            "activeNodes": list(active_nodes),
            "spareNodes": list(spare_nodes),
            "availableSpareCount": len(spare_nodes),
            "startedAt": utc_now(),
            "resultPath": str(result_path),
        }
        state["status"] = "RUNNING"
        state["activeNodes"] = list(active_nodes)
        state["spareNodes"] = list(spare_nodes)
        state["remainingSpareCount"] = len(spare_nodes)
        state["replacementCount"] = replacements_used
        state["quarantinedNodes"] = list(quarantined)
        state["attempts"].append(attempt_state)
        state["updatedAt"] = utc_now()
        write_state(state_path, state)

        failed_stage: dict[str, Any] = {}

        def handle_stage_failure(index: int, name: str) -> None:
            failed_stage["index"] = index
            failed_stage["name"] = name
            if name == "formal training parameter injection":
                start_ray.retain_then_delete_cluster(attempt_args)

        returncode = start_ray.execute_pipeline(
            stages,
            failure_handler=handle_stage_failure,
        )
        attempt_state["finishedAt"] = utc_now()
        attempt_state["returncode"] = returncode
        if returncode == 0:
            try:
                success_result = load_successful_training_result(
                    result_path,
                    expected_run_id=run_id,
                )
                require_successful_checkpoint_resume(
                    success_result,
                    expected_workers=len(active_nodes),
                )
            except RecoveryError as error:
                attempt_state["status"] = "MANUAL_REQUIRED"
                attempt_state["resultValidationFailure"] = str(error)
                state["status"] = "MANUAL_REQUIRED"
                state["activeNodes"] = list(active_nodes)
                state["spareNodes"] = list(spare_nodes)
                state["remainingSpareCount"] = len(spare_nodes)
                state["replacementCount"] = replacements_used
                state["quarantinedNodes"] = list(quarantined)
                state["updatedAt"] = utc_now()
                write_state(state_path, state)
                print(
                    "STOP: successful pipeline result could not prove checkpoint "
                    f"recovery: {error}",
                    file=sys.stderr,
                )
                return 1
            attempt_state["trainingResult"] = dict(success_result)
            attempt_state["status"] = "PASS"
            state["status"] = "PASS"
            state["updatedAt"] = utc_now()
            write_state(state_path, state)
            return 0

        attempt_state["status"] = "FAIL"
        attempt_state["failedStageIndex"] = failed_stage.get("index")
        attempt_state["failedStageName"] = failed_stage.get("name")
        try:
            if failed_stage.get("name") != "formal Ray training":
                raise RecoveryError(
                    "only a failure in the formal Ray training stage is "
                    "eligible for automatic node replacement"
                )
            failure_result = load_failed_training_result(
                result_path,
                expected_run_id=run_id,
            )
            attempt_state["trainingResult"] = dict(failure_result)

            state["status"] = "WAITING_FOR_CLEANUP"
            state["updatedAt"] = utc_now()
            write_state(state_path, state)
            wait_for_cluster_cleanup(
                kubectl_command=attempt_args.kubectl_command,
                kubeconfig=attempt_args.kubeconfig.resolve(),
                namespace=attempt_args.namespace,
                cluster=attempt_args.cluster,
                run_id=run_id,
                timeout_seconds=args.cleanup_timeout_seconds,
            )

            failure_class = failure_result.get("failureClass")
            if failure_class in {
                "DRIVER_INTERNAL_FAILURE",
                "DRIVER_PROTOCOL_FAILURE",
                "INTERRUPTED",
            }:
                raise RecoveryError(
                    "formal Ray driver failed internally or was interrupted; "
                    "automatic node replacement is not allowed"
                )
            if failure_class == "CHECKPOINT_UNAVAILABLE":
                checkpoint_result = failure_result.get("checkpoint")
                attempt_state["checkpointFailure"] = checkpoint_result
                raise RecoveryError(
                    "latest committed checkpoint is unavailable or differs "
                    "between active workers"
                )

            state["status"] = "DIAGNOSING"
            state["updatedAt"] = utc_now()
            write_state(state_path, state)
            diagnosis = recovery_diagnostics.diagnose_replacement(
                kubectl_command=attempt_args.kubectl_command,
                kubeconfig=attempt_args.kubeconfig.resolve(),
                active_nodes=active_nodes,
                spare_nodes=spare_nodes,
                expected_npus=expected_npus,
            )
            attempt_state["diagnosis"] = diagnosis
            if not diagnosis.get("replacementAllowed") or not diagnosis.get(
                "restartReady"
            ):
                reasons = diagnosis.get("reasons")
                detail = "; ".join(str(reason) for reason in reasons or [])
                raise RecoveryError(detail or "one-shot diagnosis found no safe replacement")

            replacements = diagnosis.get("replacements")
            if not isinstance(replacements, list) or not replacements:
                raise RecoveryError("diagnosis allowed replacement without a replacement list")
            if diagnosis.get("replacementCount") not in (None, len(replacements)):
                raise RecoveryError("diagnosis replacement count is inconsistent")
            if replacements_used + len(replacements) > max_replacements:
                raise RecoveryError(
                    "diagnosed failures exceed the configured spare replacement budget"
                )
            if len(replacements) > len(spare_nodes):
                raise RecoveryError(
                    "diagnosed failures exceed the currently recorded spare count"
                )

            # Build the complete N-for-N update on local immutable tuples first.
            # The supervisor state is changed only after every pair validates,
            # so a malformed second pair cannot leave a partial replacement.
            next_active = active_nodes
            next_spares = spare_nodes
            replacement_records: list[dict[str, str]] = []
            for replacement in replacements:
                if not isinstance(replacement, Mapping):
                    raise RecoveryError("diagnosis returned an invalid replacement entry")
                bad_value = replacement.get("failedNode")
                spare_value = replacement.get("spareNode")
                if not isinstance(bad_value, str) or not bad_value:
                    raise RecoveryError("diagnosis omitted a failed active target")
                if not isinstance(spare_value, str) or not spare_value:
                    raise RecoveryError("diagnosis omitted a spare target")
                next_active, next_spares = replace_active_node(
                    next_active,
                    next_spares,
                    bad_target=bad_value,
                    replacement_target=spare_value,
                )
                replacement_records.append(
                    {"badTarget": bad_value, "replacementTarget": spare_value}
                )

            active_nodes = next_active
            spare_nodes = next_spares
            replacements_used += len(replacement_records)
            quarantined.extend(item["badTarget"] for item in replacement_records)
            attempt_state["status"] = "REPLACED"
            attempt_state["replacements"] = replacement_records
            attempt_state["replacementCount"] = len(replacement_records)
            for replacement in replacement_records:
                print(
                    "RECOVERY: replacing failed active node "
                    f"{replacement['badTarget']} with spare "
                    f"{replacement['replacementTarget']}.",
                    flush=True,
                )
        except RecoveryError as error:
            attempt_state["status"] = "MANUAL_REQUIRED"
            attempt_state["recoveryFailure"] = str(error)
            state["status"] = "MANUAL_REQUIRED"
            state["activeNodes"] = list(active_nodes)
            state["spareNodes"] = list(spare_nodes)
            state["remainingSpareCount"] = len(spare_nodes)
            state["replacementCount"] = replacements_used
            state["quarantinedNodes"] = list(quarantined)
            state["updatedAt"] = utc_now()
            write_state(state_path, state)
            print(f"STOP: automatic recovery refused: {error}", file=sys.stderr)
            return returncode or 1

        attempt += 1
        state["status"] = "RETRYING"
        state["activeNodes"] = list(active_nodes)
        state["spareNodes"] = list(spare_nodes)
        state["remainingSpareCount"] = len(spare_nodes)
        state["replacementCount"] = replacements_used
        state["quarantinedNodes"] = list(quarantined)
        state["updatedAt"] = utc_now()
        write_state(state_path, state)


def main(argv: Sequence[str] | None = None) -> int:
    args = make_parser().parse_args(argv)
    try:
        return run_supervisor(args)
    except (RecoveryError, ValueError) as error:
        print(f"STOP: invalid recovery workflow: {error}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print(
            "STOP: recovery supervisor interrupted; existing resources and "
            "checkpoints were left in place.",
            file=sys.stderr,
        )
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
