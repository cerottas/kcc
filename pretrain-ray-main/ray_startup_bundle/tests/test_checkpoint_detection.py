from __future__ import annotations

import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


BUNDLE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BUNDLE_DIR))

import inject_training_params  # noqa: E402
import ray_training_driver  # noqa: E402
from inject_training_params import InjectionError  # noqa: E402
from ray_training_driver import (  # noqa: E402
    CheckpointError,
    ensure_matching_checkpoint_views,
    expected_legacy_rank_dirs,
    inspect_committed_checkpoint,
)


class CommittedCheckpointTests(unittest.TestCase):
    load_dir = "/mnt/models/checkpoints/job-42"

    @classmethod
    def policy(cls, **overrides: object) -> dict[str, object]:
        policy: dict[str, object] = {
            "enabled": True,
            "loadDir": cls.load_dir,
            "trackerFilename": "latest_checkpointed_iteration.txt",
            "selection": "megatron-tracker",
            "format": "torch",
            "tensorParallelSize": 1,
            "pipelineParallelSize": 1,
            "distributedOptimizer": True,
            "requiredForRecovery": True,
        }
        policy.update(overrides)
        return policy

    @staticmethod
    def write_candidate(
        root: Path,
        *,
        iteration: int,
        tensor_parallel_size: int = 1,
        pipeline_parallel_size: int = 1,
        model_bytes: bytes = b"model",
        optimizer_bytes: bytes | None = b"optimizer",
    ) -> Path:
        candidate = root / f"iter_{iteration:07d}"
        for rank_name in expected_legacy_rank_dirs(
            tensor_parallel_size,
            pipeline_parallel_size,
        ):
            rank_dir = candidate / rank_name
            rank_dir.mkdir(parents=True)
            (rank_dir / "model_optim_rng.pt").write_bytes(model_bytes)
            if optimizer_bytes is not None:
                (rank_dir / "distrib_optim.pt").write_bytes(optimizer_bytes)
        return candidate

    def inspect(
        self,
        root: Path,
        policy: dict[str, object] | None = None,
    ) -> dict[str, object]:
        selected_policy = self.policy() if policy is None else policy

        def worker_path(value: object) -> Path:
            if value == self.load_dir:
                return root
            return Path(value)  # type: ignore[arg-type]

        # /mnt/models exists only inside a Ray worker in production.  Map that
        # worker path to a temporary fixture without making the control host's
        # filesystem part of checkpoint selection.
        with mock.patch.object(
            ray_training_driver,
            "Path",
            side_effect=worker_path,
        ):
            return inspect_committed_checkpoint(selected_policy)

    def test_tracker_selects_committed_iteration_and_ignores_newer_partial_dir(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "latest_checkpointed_iteration.txt").write_text(
                "1000\n",
                encoding="utf-8",
            )
            self.write_candidate(root, iteration=1000)
            partial = root / "iter_0002000" / "mp_rank_00"
            partial.mkdir(parents=True)
            (partial / "model_optim_rng.pt").write_bytes(b"partial")

            report = self.inspect(root)

        self.assertEqual(report["status"], "AVAILABLE_RESUME")
        self.assertEqual(report["trackerValue"], "1000")
        self.assertEqual(report["iteration"], 1000)
        self.assertTrue(str(report["selectedDir"]).endswith("iter_0001000"))
        self.assertEqual(
            [entry["path"] for entry in report["files"]],  # type: ignore[index]
            [
                str(
                    Path("iter_0001000")
                    / "mp_rank_00"
                    / "model_optim_rng.pt"
                ),
                str(
                    Path("iter_0001000")
                    / "mp_rank_00"
                    / "distrib_optim.pt"
                ),
            ],
        )
        self.assertNotIn("iter_0002000", str(report))

    def test_missing_tracker_is_not_replaced_by_directory_scanning(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.write_candidate(root, iteration=2000)

            with self.assertRaisesRegex(CheckpointError, "tracker is missing"):
                self.inspect(root)

    def test_invalid_tracker_values_are_rejected(self) -> None:
        for tracker_value in ("", "not-an-iteration", "0", "-7", "1\n2\n"):
            with self.subTest(tracker_value=tracker_value):
                with tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary)
                    (root / "latest_checkpointed_iteration.txt").write_text(
                        tracker_value,
                        encoding="utf-8",
                    )

                    with self.assertRaises(CheckpointError):
                        self.inspect(root)

    def test_release_is_rejected_for_strict_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "latest_checkpointed_iteration.txt").write_text(
                "release\n",
                encoding="utf-8",
            )
            (root / "release").mkdir()

            with self.assertRaisesRegex(CheckpointError, "not a resumable"):
                self.inspect(root)

    def test_tracker_target_directory_must_exist(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "latest_checkpointed_iteration.txt").write_text(
                "1000\n",
                encoding="utf-8",
            )
            self.write_candidate(root, iteration=900)

            with self.assertRaisesRegex(CheckpointError, "target directory is missing"):
                self.inspect(root)

    def test_model_shard_is_required(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "latest_checkpointed_iteration.txt").write_text(
                "1000\n",
                encoding="utf-8",
            )
            rank_dir = root / "iter_0001000" / "mp_rank_00"
            rank_dir.mkdir(parents=True)
            (rank_dir / "distrib_optim.pt").write_bytes(b"optimizer")

            with self.assertRaisesRegex(CheckpointError, "model_optim_rng.pt"):
                self.inspect(root)

    def test_distributed_optimizer_shard_is_required(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "latest_checkpointed_iteration.txt").write_text(
                "1000\n",
                encoding="utf-8",
            )
            self.write_candidate(
                root,
                iteration=1000,
                optimizer_bytes=None,
            )

            with self.assertRaisesRegex(CheckpointError, "distrib_optim.pt"):
                self.inspect(root)

    def test_empty_checkpoint_shard_is_rejected(self) -> None:
        for empty_file in ("model_optim_rng.pt", "distrib_optim.pt"):
            with self.subTest(empty_file=empty_file):
                with tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary)
                    (root / "latest_checkpointed_iteration.txt").write_text(
                        "1000\n",
                        encoding="utf-8",
                    )
                    candidate = self.write_candidate(root, iteration=1000)
                    (candidate / "mp_rank_00" / empty_file).write_bytes(b"")

                    with self.assertRaisesRegex(CheckpointError, "shard is empty"):
                        self.inspect(root)

    def test_tp_pp_layout_is_validated_for_every_model_parallel_rank(self) -> None:
        self.assertEqual(expected_legacy_rank_dirs(1, 1), ("mp_rank_00",))
        self.assertEqual(
            expected_legacy_rank_dirs(2, 1),
            ("mp_rank_00", "mp_rank_01"),
        )
        self.assertEqual(
            expected_legacy_rank_dirs(1, 2),
            ("mp_rank_00_000", "mp_rank_00_001"),
        )
        self.assertEqual(
            expected_legacy_rank_dirs(2, 2),
            (
                "mp_rank_00_000",
                "mp_rank_00_001",
                "mp_rank_01_000",
                "mp_rank_01_001",
            ),
        )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "latest_checkpointed_iteration.txt").write_text(
                "1000\n",
                encoding="utf-8",
            )
            self.write_candidate(
                root,
                iteration=1000,
                tensor_parallel_size=2,
                pipeline_parallel_size=2,
            )
            report = self.inspect(
                root,
                self.policy(tensorParallelSize=2, pipelineParallelSize=2),
            )

        self.assertEqual(len(report["files"]), 8)  # type: ignore[arg-type]

    def test_invalid_tp_pp_sizes_are_rejected(self) -> None:
        for tp, pp in ((0, 1), (1, 0), (-1, 1)):
            with self.subTest(tp=tp, pp=pp):
                with self.assertRaises(CheckpointError):
                    expected_legacy_rank_dirs(tp, pp)


class MatchingWorkerViewTests(unittest.TestCase):
    @staticmethod
    def view() -> dict[str, object]:
        return {
            "status": "AVAILABLE_RESUME",
            "loadDir": "/mnt/models/checkpoints/job-42",
            "tracker": (
                "/mnt/models/checkpoints/job-42/"
                "latest_checkpointed_iteration.txt"
            ),
            "trackerValue": "1000",
            "trackerSha256": "a" * 64,
            "iteration": 1000,
            "selectedDir": "/mnt/models/checkpoints/job-42/iter_0001000",
            "files": [
                {
                    "path": "iter_0001000/mp_rank_00/model_optim_rng.pt",
                    "size": 100,
                },
                {
                    "path": "iter_0001000/mp_rank_00/distrib_optim.pt",
                    "size": 200,
                },
            ],
        }

    def test_identical_worker_views_are_accepted(self) -> None:
        first = self.view()
        second = copy.deepcopy(first)

        ensure_matching_checkpoint_views(
            [first, second],
            expected_workers=2,
        )

    def test_one_different_worker_view_is_rejected(self) -> None:
        first = self.view()
        second = copy.deepcopy(first)
        second["trackerValue"] = "2000"
        second["iteration"] = 2000

        with self.assertRaisesRegex(CheckpointError, "different checkpoint"):
            ensure_matching_checkpoint_views(
                [first, second],
                expected_workers=2,
            )

    def test_missing_worker_view_is_rejected(self) -> None:
        with self.assertRaisesRegex(CheckpointError, "missing from one or more"):
            ensure_matching_checkpoint_views(
                [self.view()],
                expected_workers=2,
            )


class CheckpointLoadSourceTests(unittest.TestCase):
    def test_active_load_must_reference_literal_load_directory(self) -> None:
        source = """
CKPT_LOAD_DIR="/mnt/models/checkpoints/job-42"
torchrun pretrain_gpt.py \\
    --load ${CKPT_LOAD_DIR} \\
    --exit-on-missing-checkpoint
"""

        self.assertEqual(
            inject_training_params.source_checkpoint_load_dir(source),
            "/mnt/models/checkpoints/job-42",
        )

    def test_direct_literal_load_path_is_supported(self) -> None:
        source = """
CKPT_LOAD_DIR='/mnt/models/checkpoints/job-42'
    --load '/mnt/models/checkpoints/job-42' \\
"""

        self.assertEqual(
            inject_training_params.source_checkpoint_load_dir(source),
            "/mnt/models/checkpoints/job-42",
        )

    def test_no_active_load_returns_none(self) -> None:
        source = """
CKPT_LOAD_DIR="/mnt/models/checkpoints/job-42"
#     --load ${CKPT_LOAD_DIR} \\
"""

        self.assertIsNone(
            inject_training_params.source_checkpoint_load_dir(source)
        )

    def test_load_token_must_match_declared_directory(self) -> None:
        source = """
CKPT_LOAD_DIR="/mnt/models/checkpoints/job-42"
    --load /mnt/models/checkpoints/another-job \\
"""

        with self.assertRaisesRegex(InjectionError, "does not reference"):
            inject_training_params.source_checkpoint_load_dir(source)

    def test_multiple_active_load_options_are_rejected(self) -> None:
        source = """
CKPT_LOAD_DIR="/mnt/models/checkpoints/job-42"
    --load ${CKPT_LOAD_DIR} \\
    --load ${CKPT_LOAD_DIR} \\
"""

        with self.assertRaisesRegex(InjectionError, "at most one"):
            inject_training_params.source_checkpoint_load_dir(source)


class CheckpointInjectionManifestTests(unittest.TestCase):
    @staticmethod
    def source(*, include_load: bool) -> str:
        load_option = (
            "    --load ${CKPT_LOAD_DIR} \\\n"
            if include_load
            else ""
        )
        return f"""#!/bin/bash
export RANK_TABLE_FILE=/tmp/old-ranktable.json
NPUS_PER_NODE=8
MASTER_ADDR=10.42.0.1
MASTER_PORT=26011
NNODES=1
NODE_RANK=0
TP=1
PP=1
CKPT_SAVE_DIR="/mnt/models/checkpoints/job-42"
CKPT_LOAD_DIR="/mnt/models/checkpoints/job-42"
LOG_FILE="logs/train.log"
torchrun --nproc_per_node 8 pretrain_gpt.py \\
    --use-distributed-optimizer \\
    --save ${{CKPT_SAVE_DIR}} \\
{load_option}    --exit-on-missing-checkpoint
"""

    @staticmethod
    def write_evidence(root: Path) -> tuple[Path, Path]:
        hccl = root / "03-hccl.json"
        ping = root / "01-ping.json"
        hccl.write_text(
            json.dumps(
                {
                    "status": "PASS",
                    "result": {
                        "status": "PASS",
                        "mode": "execute",
                        "preflight": {
                            "status": "PASS",
                            "ranktable_sha256": "a" * 64,
                            "server_count": 1,
                            "world_size": 8,
                            "workers": [
                                {
                                    "pod_name": "ray-worker-0",
                                    "node_name": "worker-00",
                                    "server_id": "10.0.0.1",
                                    "ray_node_id": "ray-node-id-0",
                                    "npu_count": 8,
                                    "rank_start": 0,
                                }
                            ],
                        },
                    },
                }
            ),
            encoding="utf-8",
        )
        ping.write_text(
            json.dumps(
                {
                    "status": "PASS",
                    "result": {
                        "status": "PASS",
                        "mode": "execute",
                        "worker_count": 1,
                        "workers": [
                            {
                                "pod": "ray-worker-0",
                                "node_id": "ray-node-id-0",
                                "resource_count": 8,
                                "ray_node_ip": "10.42.0.1",
                            }
                        ],
                    },
                }
            ),
            encoding="utf-8",
        )
        return hccl, ping

    def create_manifest(
        self,
        root: Path,
        *,
        include_load: bool,
        require_resumable_checkpoint: bool,
    ) -> dict[str, object]:
        source_path = root / "train.sh"
        source_path.write_text(
            self.source(include_load=include_load),
            encoding="utf-8",
        )
        hccl, ping = self.write_evidence(root)
        manifest_path = inject_training_params.create_injection(
            source_path=source_path,
            hccl_evidence=hccl,
            ping_evidence=ping,
            output_dir=root / "injection",
            run_id="checkpoint-policy-test",
            training_cwd="/mnt/models/CODE/MindSpeed-LLM-v2.3.0",
            master_port=None,
            allow_topology_change=False,
            confirm_checkpoint_exclusive=True,
            require_resumable_checkpoint=require_resumable_checkpoint,
        )
        return json.loads(manifest_path.read_text(encoding="utf-8"))

    def test_strict_recovery_manifest_freezes_checkpoint_policy(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            manifest = self.create_manifest(
                Path(temporary),
                include_load=True,
                require_resumable_checkpoint=True,
            )

        self.assertEqual(
            manifest["checkpointLoad"],
            {
                "enabled": True,
                "loadDir": "/mnt/models/checkpoints/job-42",
                "trackerFilename": "latest_checkpointed_iteration.txt",
                "selection": "megatron-tracker",
                "requiredForRecovery": True,
                "format": "torch",
                "tensorParallelSize": 1,
                "pipelineParallelSize": 1,
                "distributedOptimizer": True,
            },
        )
        self.assertEqual(
            manifest["checkpointWrite"],
            {
                "enabled": True,
                "saveDir": "/mnt/models/checkpoints/job-42",
                "exclusiveConfirmed": True,
            },
        )

    def test_normal_launch_without_load_keeps_checkpoint_check_disabled(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            manifest = self.create_manifest(
                Path(temporary),
                include_load=False,
                require_resumable_checkpoint=False,
            )

        checkpoint_load = manifest["checkpointLoad"]
        self.assertFalse(checkpoint_load["enabled"])
        self.assertIsNone(checkpoint_load["loadDir"])
        self.assertFalse(checkpoint_load["requiredForRecovery"])
        self.assertTrue(manifest["checkpointWrite"]["enabled"])


class DriverMainFailureResultTests(unittest.TestCase):
    def test_execute_failure_after_injection_preserves_current_run_id(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            injection_path = root / "injection.json"
            scripts_dir = root / "scripts"
            result_path = root / "execution-result.json"
            injection = {
                "schemaVersion": "training-injection/v1",
                "runId": "pretrain-resume-a01",
            }

            with (
                mock.patch.object(
                    ray_training_driver,
                    "load_injection",
                    return_value=(injection, {}, {}),
                ) as load_injection,
                mock.patch.object(
                    ray_training_driver,
                    "execute_on_ray",
                    side_effect=RuntimeError("worker actor disconnected"),
                ) as execute_on_ray,
            ):
                status = ray_training_driver.main(
                    [
                        "--injection",
                        str(injection_path),
                        "--scripts-dir",
                        str(scripts_dir),
                        "--result",
                        str(result_path),
                    ]
                )

            self.assertEqual(status, 1)
            load_injection.assert_called_once_with(
                injection_path.resolve(),
                scripts_dir.resolve(),
            )
            execute_on_ray.assert_called_once()
            result = json.loads(result_path.read_text(encoding="utf-8"))
            self.assertEqual(result["schemaVersion"], "ray-training-result/v1")
            self.assertEqual(result["status"], "FAIL")
            self.assertEqual(
                result["failureClass"],
                "DRIVER_INTERNAL_FAILURE",
            )
            self.assertEqual(result["runId"], "pretrain-resume-a01")
            self.assertIn("worker actor disconnected", result["failure"])


if __name__ == "__main__":
    unittest.main()
