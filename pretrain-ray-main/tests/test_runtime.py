from pathlib import Path
from types import SimpleNamespace
import json
import os
import tempfile
import unittest
from unittest.mock import patch

from kcc_training.runtime import coordinator as coordinator_module
from kcc_training.runtime.checkpoints import (
    CheckpointError,
    CheckpointUnavailable,
    require_consistent,
    snapshot,
)
from kcc_training.runtime.coordinator import (
    CoordinatorError,
    consistent_checkpoint_iteration,
    load_runtime_control,
    _node_ranks_from_hccl,
    failure_result,
    run_hccl_gate,
    wait_ranktable,
)
from kcc_training.runtime.spec import RuntimeSpec, RuntimeSpecError
from kcc_training.runtime.worker import StructuredWorker
from ray_startup_bundle.hccl_runtime.hccl_check import _ping as ping_module


def spec_document():
    return {
        "schemaVersion": "kcc-runtime/v1",
        "run": {"name": "run-1", "namespace": "training", "uid": "uid-1", "attempt": 0},
        "topology": {
            "workers": 2, "nodes": ["node-a", "node-b"], "devicesPerNode": 8,
            "resourceName": "huawei.com/Ascend910", "rankTablePath": "/etc/kcc/ranktable/hccl.json",
        },
        "training": {
            "framework": "mindspeed", "command": ["python", "pretrain.py"],
            "workingDirectory": "/workspace/source", "environment": {}, "noProgressSeconds": 3600,
        },
        "artifacts": {"provider": "workspace", "source": "artifact://s", "model": "artifact://m", "data": "artifact://d", "outputRoot": "/workspace/runs/run-1"},
    }


class RuntimeTests(unittest.TestCase):
    def test_spec_loads_structured_command(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "run.json"
            path.write_text(json.dumps(spec_document()), encoding="utf-8")
            spec = RuntimeSpec.load(path)
            self.assertEqual(spec.command, ("python", "pretrain.py"))
            self.assertEqual(spec.artifact_provider, "workspace")

    def test_spec_rejects_duplicate_topology(self):
        document = spec_document()
        document["topology"]["nodes"] = ["node-a", "node-a"]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "run.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(RuntimeSpecError, "unique"):
                RuntimeSpec.load(path)

    def test_checkpoint_uses_tracker_not_largest_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "latest_checkpointed_iteration.txt").write_text("7\n", encoding="utf-8")
            committed = root / "iter_0000007" / "mp_rank_00"
            committed.mkdir(parents=True)
            (committed / "model.pt").write_bytes(b"valid")
            incomplete = root / "iter_0000008"
            incomplete.mkdir()
            report = snapshot(root)
            self.assertEqual(report["iteration"], 7)
            self.assertNotIn("iter_0000008", str(report["files"]))

    def test_checkpoint_views_must_match(self):
        with self.assertRaisesRegex(CheckpointError, "different"):
            require_consistent([{"snapshotSha256": "a"}, {"snapshotSha256": "b"}], 2)

    def test_runtime_control_is_bound_to_run_attempt_and_generation(self):
        spec = SimpleNamespace(
            run_name="run-1",
            run_uid="uid-1",
            attempt=0,
        )
        payload = {
            "schemaVersion": "kcc-runtime-control/v1",
            "runName": "run-1",
            "runUid": "uid-1",
            "attempt": 0,
            "action": "StopAfterCheckpoint",
            "requestGeneration": 2,
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "control.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            self.assertEqual(
                load_runtime_control(path, spec)["requestGeneration"],
                2,
            )
            payload["runUid"] = "other"
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(CoordinatorError, "identity"):
                load_runtime_control(path, spec)

    def test_checkpoint_tracker_views_must_advance_consistently(self):
        self.assertEqual(
            consistent_checkpoint_iteration(
                [
                    {"available": True, "iteration": 8},
                    {"available": True, "iteration": 8},
                ],
                2,
            ),
            8,
        )
        self.assertIsNone(
            consistent_checkpoint_iteration(
                [{"available": False}, {"available": False}],
                2,
            )
        )
        with self.assertRaisesRegex(CheckpointError, "different"):
            consistent_checkpoint_iteration(
                [
                    {"available": True, "iteration": 8},
                    {"available": True, "iteration": 9},
                ],
                2,
            )

    def test_worker_reads_committed_checkpoint_iteration(self):
        identity = {
            "NODE_NAME": "node-a",
            "POD_NAME": "worker-a",
            "POD_IP": "10.0.0.2",
            "HOST_IP": "10.0.0.1",
        }
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, identity
        ):
            root = Path(directory)
            (root / "latest_checkpointed_iteration.txt").write_text(
                "7\n", encoding="utf-8"
            )
            self.assertEqual(
                StructuredWorker().checkpoint_iteration(str(root))["iteration"],
                7,
            )

    def test_ranktable_hashes_exact_projected_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "hccl.json"
            path.write_text('{"server_list":[]}', encoding="utf-8")
            self.assertEqual(len(wait_ranktable(path, 1)), 64)

    def test_missing_checkpoint_is_available_state_not_corruption(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(CheckpointUnavailable):
                snapshot(Path(directory))
        self.assertIsNone(
            require_consistent(
                [{"available": False}, {"available": False}],
                2,
            )
        )

    def test_checkpoint_digest_includes_file_content(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "latest_checkpointed_iteration.txt").write_text("1\n")
            shard = root / "iter_0000001" / "model.pt"
            shard.parent.mkdir()
            payload = bytearray(512 * 1024)
            shard.write_bytes(payload)
            first_report = snapshot(root)
            self.assertEqual(first_report["hashMode"], "sampled-v1")
            self.assertEqual(first_report["sampleBytesPerFile"], 192 * 1024)
            payload[len(payload) // 2] = 1
            shard.write_bytes(payload)
            second_report = snapshot(root)
            self.assertNotEqual(
                first_report["snapshotSha256"],
                second_report["snapshotSha256"],
            )

    def test_spec_accepts_explicit_checkpoint_root(self):
        document = spec_document()
        document["artifacts"]["checkpointRoot"] = "/workspace/checkpoints/run-1"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "run.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            self.assertEqual(
                RuntimeSpec.load(path).checkpoint_root,
                Path("/workspace/checkpoints/run-1"),
            )

    def test_training_node_rank_comes_from_hccl_rank_start(self):
        result = {
            "stages": [
                {
                    "name": "hccl",
                    "result": {
                        "preflight": {
                            "workers": [
                                {"node_name": "node-b", "rank_start": 0},
                                {"node_name": "node-a", "rank_start": 8},
                            ]
                        }
                    },
                }
            ]
        }
        spec = SimpleNamespace(
            nodes=("node-a", "node-b"),
            workers=2,
            devices_per_node=8,
        )
        self.assertEqual(
            _node_ranks_from_hccl(result, spec),
            {"node-a": 1, "node-b": 0},
        )

    def test_hccl_gate_runs_before_ranktable_exists_and_passes_probe(self):
        spec = SimpleNamespace(
            namespace="training",
            run_name="run-1",
            attempt=0,
            workers=2,
            devices_per_node=8,
            ranktable_path=Path("/etc/kcc/ranktable/hccl.json"),
        )
        completed = SimpleNamespace(
            returncode=0,
            stdout=json.dumps({"status": "PASS"}),
            stderr="",
        )
        with tempfile.TemporaryDirectory() as directory:
            with patch(
                "kcc_training.runtime.coordinator.subprocess.run",
                return_value=completed,
            ) as invoked:
                self.assertEqual(
                    run_hccl_gate(spec, Path(directory))["status"],
                    "PASS",
                )
        command = invoked.call_args.args[0]
        self.assertEqual(command[1:3], ["-m", "hccl_check"])
        probe_index = command.index("--probe-binary") + 1
        self.assertEqual(
            command[probe_index],
            "/opt/kcc-hccl/bin/ranktable_allreduce_probe",
        )

        device_ids_index = command.index("--device-ids") + 1
        self.assertEqual(command[device_ids_index], "0,1,2,3,4,5,6,7")
    def test_failure_before_first_checkpoint_is_retryable_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            spec = SimpleNamespace(
                checkpoint_root=Path(directory) / "checkpoints",
                run_name="run-1",
                namespace="training",
                run_uid="uid-1",
                attempt=0,
            )
            result = failure_result(
                spec,
                CoordinatorError("Ray unavailable", scope="infrastructure"),
            )
        self.assertTrue(result["checkpointConsistent"])
        self.assertFalse(result["checkpointAvailable"])
        self.assertEqual(result["failureScope"], "infrastructure")


    def test_result_configmap_payload_is_bounded(self):
        result = {
            "schemaVersion": "kcc-runtime-result/v1",
            "runName": "run-1",
            "namespace": "training",
            "runUid": "uid-1",
            "attempt": 0,
            "status": "FAIL",
            "checkpointConsistent": True,
            "failureScope": "software",
            "failedNodes": [],
            "failure": "x" * (2 * 1024 * 1024),
            "hccl": {"status": "FAIL", "failure": "y" * (2 * 1024 * 1024)},
        }
        payload, truncated, digest = coordinator_module._result_payload(result)
        self.assertLessEqual(
            len(payload.encode("utf-8")),
            coordinator_module.MAX_RESULT_CONFIGMAP_BYTES,
        )
        self.assertTrue(truncated)
        self.assertEqual(len(digest), 64)

    def test_result_configmap_has_nonblocking_trainingrun_owner(self):
        result = {
            "schemaVersion": "kcc-runtime-result/v1",
            "runName": "run-1",
            "namespace": "training",
            "runUid": "uid-1",
            "attempt": 0,
            "status": "FAIL",
            "checkpointConsistent": True,
            "failureScope": "software",
            "failedNodes": [],
        }
        with patch.object(coordinator_module, "KubernetesApi") as api_type:
            coordinator_module._publish(result)
        document = api_type.return_value.upsert.call_args.args[2]
        self.assertEqual(
            document["metadata"]["ownerReferences"],
            [
                {
                    "apiVersion": "training.kcc.io/v1beta1",
                    "kind": "TrainingRun",
                    "name": "run-1",
                    "uid": "uid-1",
                    "controller": False,
                    "blockOwnerDeletion": False,
                }
            ],
        )

    def test_runtime_exception_after_spec_load_is_published(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "run.json"
            path.write_text(json.dumps(spec_document()), encoding="utf-8")
            spec = RuntimeSpec.load(path)
            with patch.object(
                coordinator_module.RuntimeSpec,
                "load",
                return_value=spec,
            ), patch.object(
                coordinator_module,
                "execute",
                side_effect=ValueError("ray setup exploded"),
            ), patch.object(coordinator_module, "_publish") as publish, patch(
                "builtins.print"
            ):
                returncode = coordinator_module.main(["--spec", str(path)])
        self.assertEqual(returncode, 1)
        published = publish.call_args.args[0]
        self.assertEqual(published["status"], "FAIL")
        self.assertTrue(published["checkpointConsistent"])
        self.assertFalse(published["checkpointAvailable"])
        self.assertIn("ray setup exploded", published["failure"])

    def test_worker_injects_standard_runtime_paths(self):
        captured = {}

        class CompletedProcess:
            pid = 12345

            def poll(self):
                return 0

            def wait(self):
                return 0

        def start_process(argv, **kwargs):
            captured["argv"] = argv
            captured["env"] = kwargs["env"]
            return CompletedProcess()

        identity = {
            "NODE_NAME": "node-a",
            "POD_NAME": "worker-a",
            "POD_IP": "10.0.0.2",
            "HOST_IP": "10.0.0.1",
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch.dict(os.environ, identity), patch(
                "kcc_training.runtime.worker.subprocess.Popen",
                side_effect=start_process,
            ), patch.object(StructuredWorker, "_terminate"):
                outcome = StructuredWorker().run(
                    node_rank=1,
                    workers=2,
                    devices=8,
                    master_addr="10.0.0.3",
                    master_port=29501,
                    command=("python", "train.py"),
                    cwd=str(root),
                    environment={"KCC_RESUME_FROM": "stale"},
                    ranktable="/etc/kcc/ranktable/hccl.json",
                    log_root=str(root / "logs"),
                    checkpoint_root="/workspace/checkpoints/run-1",
                    output_root="/workspace/runs/run-1",
                    attempt_root="/workspace/runs/run-1/attempt-01",
                    attempt=1,
                    resume_from="/workspace/checkpoints/run-1/iter_0000007",
                    no_progress_seconds=0,
                )
        self.assertEqual(outcome["status"], "PASS")
        self.assertEqual(captured["env"]["KCC_OUTPUT_ROOT"], "/workspace/runs/run-1")
        self.assertEqual(
            captured["env"]["KCC_CHECKPOINT_ROOT"],
            "/workspace/checkpoints/run-1",
        )
        self.assertEqual(
            captured["env"]["KCC_RESUME_FROM"],
            "/workspace/checkpoints/run-1/iter_0000007",
        )
        self.assertEqual(captured["env"]["KCC_ATTEMPT"], "1")
        self.assertEqual(captured["env"]["NODE_RANK"], "1")
        self.assertEqual(captured["env"]["WORLD_SIZE"], "16")
        self.assertEqual(
            captured["env"]["RANK_TABLE_FILE"],
            "/etc/kcc/ranktable/hccl.json",
        )

    def test_ping_setup_failure_is_infrastructure(self):
        payload = {
            "status": "FAIL",
            "failed_stage": "ping",
            "failure": "hccn_tool is missing or not executable",
        }
        spec = SimpleNamespace(
            namespace="training",
            run_name="run-1",
            attempt=0,
            workers=2,
            devices_per_node=8,
            ranktable_path=Path("/etc/kcc/ranktable/hccl.json"),
        )
        completed = SimpleNamespace(
            returncode=1,
            stdout=json.dumps(payload),
            stderr="",
        )
        with tempfile.TemporaryDirectory() as directory:
            with patch(
                "kcc_training.runtime.coordinator.subprocess.run",
                return_value=completed,
            ):
                with self.assertRaises(CoordinatorError) as raised:
                    run_hccl_gate(spec, Path(directory))
        self.assertEqual(raised.exception.scope, "infrastructure")
        self.assertEqual(raised.exception.failed_nodes, ())

    def test_hccl_hardware_scope_requires_non_peer_failed_node(self):
        payload = {
            "status": "FAIL",
            "failed_stage": "hccl",
            "failure": "one rank failed",
            "stages": [
                {
                    "name": "hccl",
                    "result": {
                        "preflight": {
                            "workers": [
                                {"node_name": "node-a", "rank_start": 0},
                                {"node_name": "node-b", "rank_start": 8},
                            ]
                        },
                        "workers": [
                            {"status": "FAIL", "rank_start": 0, "failure": "allreduce failed"},
                            {"status": "FAIL", "rank_start": 8, "failure": "stop requested by Ray driver"},
                        ],
                    },
                }
            ],
        }
        spec = SimpleNamespace(
            namespace="training",
            run_name="run-1",
            attempt=0,
            workers=2,
            devices_per_node=8,
            ranktable_path=Path("/etc/kcc/ranktable/hccl.json"),
        )
        completed = SimpleNamespace(
            returncode=1,
            stdout=json.dumps(payload),
            stderr="",
        )
        with tempfile.TemporaryDirectory() as directory:
            with patch(
                "kcc_training.runtime.coordinator.subprocess.run",
                return_value=completed,
            ):
                with self.assertRaises(CoordinatorError) as raised:
                    run_hccl_gate(spec, Path(directory))
        self.assertEqual(raised.exception.scope, "hardware")
        self.assertEqual(raised.exception.failed_nodes, ("node-a",))


    def test_hccn_tool_preflight_allows_non_root_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            tool = Path(directory) / "hccn_tool"
            tool.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            tool.chmod(0o755)
            with patch.object(ping_module.os, "geteuid", return_value=65532):
                ping_module.require_hccn_tool(str(tool))

if __name__ == "__main__":
    unittest.main()
