from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from typing import Any, Mapping, Sequence

from .checkpoints import (
    MAX_TRACKER_BYTES,
    TRACKER,
    CheckpointUnavailable,
    discard_uncommitted,
    snapshot,
)


class WorkerError(RuntimeError):
    pass


_FAILURE_SCOPES = {"hardware", "infrastructure", "network", "global-stall", "software"}
_MAX_FAILURE_REPORT_BYTES = 4096


def failure_scope_from_report(path: Path) -> str | None:
    """Read an optional node-local failure hint emitted by the training process.

    Hardware hints are not authoritative: the controller still requires stable
    npu-exporter evidence before it replaces a node.
    """
    try:
        if (
            path.is_symlink()
            or not path.is_file()
            or path.stat().st_size > _MAX_FAILURE_REPORT_BYTES
        ):
            return None
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError, TypeError):
        return None
    if not isinstance(document, Mapping):
        return None
    scope = document.get("failureScope")
    return scope if isinstance(scope, str) and scope in _FAILURE_SCOPES else None


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class StructuredWorker:
    def __init__(self) -> None:
        self.process: subprocess.Popen[str] | None = None
        self.stop_reason: str | None = None

    def identity(self) -> dict[str, str]:
        result = {key: os.environ.get(key, "") for key in ("NODE_NAME", "POD_NAME", "POD_IP", "HOST_IP")}
        if any(not value for value in result.values()):
            raise WorkerError(f"worker identity is incomplete: {result}")
        return result

    def preflight(self, *, expected_node: str, cwd: str, ranktable: str, ranktable_sha256: str, devices: int) -> Mapping[str, Any]:
        identity = self.identity()
        if identity["NODE_NAME"] != expected_node:
            raise WorkerError("worker scheduling differs from frozen topology")
        working_directory = Path(cwd)
        table = Path(ranktable)
        if not working_directory.is_dir() or working_directory.is_symlink():
            raise WorkerError("training working directory is missing or unsafe")
        if not table.is_file() or file_sha256(table) != ranktable_sha256:
            raise WorkerError("worker RankTable is missing or changed")
        missing = [device for device in range(devices) if not Path(f"/dev/davinci{device}").exists()]
        if missing:
            raise WorkerError(f"worker NPU devices are missing: {missing}")
        return {"status": "PASS", "identity": identity}

    @staticmethod
    def _terminate(process: subprocess.Popen[str], grace: float = 15) -> None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        deadline = time.monotonic() + grace
        while time.monotonic() < deadline:
            if process.poll() is not None:
                return
            time.sleep(0.2)
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


    def checkpoint_iteration(self, checkpoint_root: str) -> Mapping[str, Any]:
        node_name = self.identity()["NODE_NAME"]
        tracker = Path(checkpoint_root) / TRACKER
        if not tracker.exists():
            return {"available": False, "nodeName": node_name}
        try:
            if tracker.is_symlink() or not tracker.is_file():
                raise WorkerError("checkpoint tracker is not a regular file")
            if tracker.stat().st_size > MAX_TRACKER_BYTES:
                raise WorkerError("checkpoint tracker is too large")
            iteration = int(tracker.read_text(encoding="utf-8").strip())
        except WorkerError:
            raise
        except (OSError, UnicodeError, ValueError) as error:
            raise WorkerError(f"cannot read checkpoint tracker: {error}") from error
        if iteration <= 0:
            raise WorkerError("checkpoint iteration must be positive")
        return {"available": True, "iteration": iteration, "nodeName": node_name}

    def checkpoint(self, checkpoint_root: str) -> Mapping[str, Any]:
        node_name = self.identity()["NODE_NAME"]
        try:
            report = snapshot(Path(checkpoint_root))
        except CheckpointUnavailable:
            return {"available": False, "nodeName": node_name}
        return {**report, "nodeName": node_name}

    def discard_uncommitted_checkpoints(
        self,
        checkpoint_root: str,
        keep_iteration: int | None,
    ) -> Mapping[str, Any]:
        return {
            **discard_uncommitted(Path(checkpoint_root), keep_iteration),
            "nodeName": self.identity()["NODE_NAME"],
        }

    def run(
        self,
        *,
        node_rank: int,
        workers: int,
        devices: int,
        master_addr: str,
        master_port: int,
        command: Sequence[str],
        cwd: str,
        environment: Mapping[str, str],
        ranktable: str,
        log_root: str,
        checkpoint_root: str,
        output_root: str,
        attempt_root: str,
        attempt: int,
        resume_from: str | None,
        no_progress_seconds: int,
    ) -> Mapping[str, Any]:
        rank_log = Path(log_root) / f"node-rank-{node_rank}"
        rank_log.mkdir(parents=True, exist_ok=False)
        stdout_path = rank_log / "stdout.log"
        stderr_path = rank_log / "stderr.log"
        failure_report_path = rank_log / "failure-report.json"
        argv = [
            sys.executable,
            "-m",
            "torch.distributed.run",
            "--no-python",
            f"--nnodes={workers}",
            f"--node-rank={node_rank}",
            f"--nproc-per-node={devices}",
            f"--master-addr={master_addr}",
            f"--master-port={master_port}",
            *command,
        ]
        child_env = dict(os.environ)
        child_env.update(environment)
        runtime_env = {
            "RANK_TABLE_FILE": ranktable,
            "NODE_RANK": str(node_rank),
            "WORLD_SIZE": str(workers * devices),
            "KCC_NODE_RANK": str(node_rank),
            "KCC_WORLD_SIZE": str(workers * devices),
            "KCC_ATTEMPT": str(attempt),
            "KCC_ATTEMPT_ROOT": attempt_root,
            "KCC_OUTPUT_ROOT": output_root,
            "KCC_CHECKPOINT_ROOT": checkpoint_root,
            "KCC_FAILURE_REPORT_PATH": str(failure_report_path),
            "PYTHONUNBUFFERED": "1",
        }
        if resume_from is None:
            child_env.pop("KCC_RESUME_FROM", None)
        else:
            runtime_env["KCC_RESUME_FROM"] = resume_from
        child_env.update(runtime_env)
        started = time.monotonic()
        last_progress = started
        last_signature: tuple[int, int, int] | None = None
        with stdout_path.open("x", encoding="utf-8") as stdout, stderr_path.open("x", encoding="utf-8") as stderr:
            self.process = subprocess.Popen(
                argv,
                cwd=cwd,
                env=child_env,
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                text=True,
                shell=False,
                start_new_session=True,
            )
            no_progress = False
            try:
                while self.process.poll() is None:
                    if self.stop_reason is not None:
                        self._terminate(self.process)
                        break
                    if node_rank == 0 and no_progress_seconds:
                        tracker = Path(checkpoint_root) / "latest_checkpointed_iteration.txt"
                        signature = (
                            stdout_path.stat().st_size,
                            stderr_path.stat().st_size,
                            tracker.stat().st_mtime_ns if tracker.exists() else 0,
                        )
                        if last_signature is None or signature != last_signature:
                            last_signature = signature
                            last_progress = time.monotonic()
                        elif time.monotonic() - last_progress >= no_progress_seconds:
                            no_progress = True
                            self._terminate(self.process)
                            break
                    time.sleep(1)
                returncode = self.process.wait()
            finally:
                if self.process is not None:
                    self._terminate(self.process, 3)
                self.process = None
        if self.stop_reason is not None:
            status = "STOPPED"
        elif returncode == 0:
            status = "PASS"
        elif no_progress:
            status = "NO_PROGRESS"
        else:
            status = "FAIL"
        reported_failure_scope = failure_scope_from_report(failure_report_path)
        return {
            "status": status,
            "failureScope": (
                None
                if status in {"PASS", "STOPPED"}
                else (
                    "global-stall" if no_progress else (reported_failure_scope or "software")
                )
            ),
            "stopReason": self.stop_reason,
            "nodeRank": node_rank,
            "nodeName": self.identity()["NODE_NAME"],
            "returncode": returncode,
            "durationSeconds": round(time.monotonic() - started, 3),
            "stdout": str(stdout_path),
            "stderr": str(stderr_path),
        }

    def stop(self, reason: str) -> None:
        self.stop_reason = reason
        if self.process is not None:
            self._terminate(self.process)
