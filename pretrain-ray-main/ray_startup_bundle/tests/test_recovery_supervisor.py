from __future__ import annotations

import json
from pathlib import Path
import re
from types import SimpleNamespace
import sys
import tempfile
import unittest
from unittest import mock


BUNDLE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BUNDLE_DIR))

import recovery_supervisor  # noqa: E402
from recovery_supervisor import (  # noqa: E402
    RecoveryError,
    attempt_run_id,
    replace_active_node,
    validate_node_pool,
)
from recovery_diagnostics import select_replacement  # noqa: E402


class AttemptRunIdTests(unittest.TestCase):
    def test_attempt_run_id_is_stable_unique_and_valid(self) -> None:
        first = attempt_run_id("pretrain-job-42", 0)
        retry = attempt_run_id("pretrain-job-42", 1)

        self.assertEqual(first, "pretrain-job-42-a00")
        self.assertEqual(retry, "pretrain-job-42-a01")
        self.assertEqual(first, attempt_run_id("pretrain-job-42", 0))
        self.assertNotEqual(first, retry)
        self.assertRegex(first, re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$"))
        self.assertRegex(retry, re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$"))

    def test_attempt_run_id_rejects_negative_attempt(self) -> None:
        with self.assertRaises(RecoveryError):
            attempt_run_id("pretrain-job-42", -1)

    def test_attempt_run_id_rejects_result_over_80_characters(self) -> None:
        with self.assertRaises(RecoveryError):
            attempt_run_id("j" * 77, 0)


class ActiveSpareReplacementTests(unittest.TestCase):
    def setUp(self) -> None:
        self.active = (
            "worker-00",
            "worker-01",
            "worker-02",
            "worker-03",
            "worker-04",
            "worker-05",
        )
        self.spares = ("worker-06", "worker-07")

    def test_unique_failed_node_is_replaced_in_the_same_rank_slot(self) -> None:
        new_active, new_spares = replace_active_node(
            self.active,
            self.spares,
            bad_target="worker-02",
            replacement_target="worker-06",
        )

        self.assertEqual(
            new_active,
            (
                "worker-00",
                "worker-01",
                "worker-06",
                "worker-03",
                "worker-04",
                "worker-05",
            ),
        )
        self.assertEqual(new_spares, ("worker-07",))
        self.assertEqual(self.active[2], "worker-02")
        self.assertEqual(self.spares, ("worker-06", "worker-07"))

    def test_second_replacement_consumes_the_remaining_spare(self) -> None:
        active, spares = replace_active_node(
            self.active,
            self.spares,
            bad_target="worker-02",
            replacement_target="worker-06",
        )
        active, spares = replace_active_node(
            active,
            spares,
            bad_target="worker-04",
            replacement_target="worker-07",
        )

        self.assertEqual(
            active,
            (
                "worker-00",
                "worker-01",
                "worker-06",
                "worker-03",
                "worker-07",
                "worker-05",
            ),
        )
        self.assertEqual(spares, ())

    def test_spare_exhaustion_stops_recovery(self) -> None:
        with self.assertRaises(RecoveryError):
            replace_active_node(
                self.active,
                (),
                bad_target="worker-02",
                replacement_target="worker-06",
            )

    def test_replacement_must_be_a_declared_spare(self) -> None:
        with self.assertRaises(RecoveryError):
            replace_active_node(
                self.active,
                self.spares,
                bad_target="worker-02",
                replacement_target="worker-99",
            )

    def test_failed_node_must_be_active(self) -> None:
        with self.assertRaises(RecoveryError):
            replace_active_node(
                self.active,
                self.spares,
                bad_target="worker-99",
                replacement_target="worker-06",
            )

    def test_overlapping_active_and_spare_pools_are_rejected(self) -> None:
        with self.assertRaises(RecoveryError):
            validate_node_pool(
                self.active,
                ("worker-05", "worker-06"),
            )


class ReplacementDecisionTests(unittest.TestCase):
    @staticmethod
    def active(target: str, status: str) -> dict[str, object]:
        return {
            "target": target,
            "nodeName": target,
            "status": status,
            "idle": status == "healthy",
        }

    @staticmethod
    def spare(
        target: str,
        *,
        status: str = "healthy",
        eligible: bool = True,
    ) -> dict[str, object]:
        return {
            "target": target,
            "nodeName": target,
            "status": status,
            "eligible": eligible,
        }

    def test_exactly_one_failed_active_selects_first_eligible_spare(self) -> None:
        active = [
            self.active("worker-00", "healthy"),
            self.active("worker-01", "unhealthy"),
            self.active("worker-02", "healthy"),
        ]
        spares = [self.spare("worker-06"), self.spare("worker-07")]

        decision = select_replacement(active, spares)

        self.assertTrue(decision["replacementAllowed"])
        self.assertEqual(decision["outcome"], "replacements_selected")
        self.assertEqual(
            decision["replacements"],
            [
                {
                    "failedNode": "worker-01",
                    "failedNodeName": "worker-01",
                    "spareNode": "worker-06",
                    "spareNodeName": "worker-06",
                }
            ],
        )
        self.assertEqual(decision["replacementCount"], 1)
        self.assertEqual(decision["replacement"], decision["replacements"][0])

    def test_no_failed_active_is_inconclusive(self) -> None:
        active = [
            self.active("worker-00", "healthy"),
            self.active("worker-01", "healthy"),
        ]

        decision = select_replacement(active, [self.spare("worker-06")])

        self.assertFalse(decision["replacementAllowed"])
        self.assertEqual(decision["replacements"], [])
        self.assertIn("no failed active node was identified", decision["reasons"])

    def test_two_failed_actives_pair_with_two_spares_in_input_order(self) -> None:
        active = [
            self.active("worker-00", "healthy"),
            self.active("worker-01", "unhealthy"),
            self.active("worker-02", "healthy"),
            self.active("worker-03", "healthy"),
            self.active("worker-04", "unhealthy"),
            self.active("worker-05", "healthy"),
        ]
        spares = [self.spare("worker-06"), self.spare("worker-07")]

        decision = select_replacement(active, spares)

        self.assertTrue(decision["replacementAllowed"])
        self.assertEqual(decision["replacementCount"], 2)
        self.assertIsNone(decision["replacement"])
        self.assertEqual(
            [
                (item["failedNode"], item["spareNode"])
                for item in decision["replacements"]
            ],
            [("worker-01", "worker-06"), ("worker-04", "worker-07")],
        )

        updated_active = tuple(node["target"] for node in active)
        updated_spares = tuple(node["target"] for node in spares)
        for replacement in decision["replacements"]:
            updated_active, updated_spares = replace_active_node(
                updated_active,
                updated_spares,
                bad_target=str(replacement["failedNode"]),
                replacement_target=str(replacement["spareNode"]),
            )
        self.assertEqual(
            updated_active,
            (
                "worker-00",
                "worker-06",
                "worker-02",
                "worker-03",
                "worker-07",
                "worker-05",
            ),
        )
        self.assertEqual(updated_spares, ())

    def test_three_failed_actives_with_two_spares_are_inconclusive(self) -> None:
        active = [
            self.active("worker-00", "unhealthy"),
            self.active("worker-01", "unhealthy"),
            self.active("worker-02", "unhealthy"),
            self.active("worker-03", "healthy"),
        ]

        decision = select_replacement(
            active,
            [self.spare("worker-06"), self.spare("worker-07")],
        )

        self.assertFalse(decision["replacementAllowed"])
        self.assertEqual(decision["replacements"], [])

    def test_unknown_active_prevents_replacement(self) -> None:
        active = [
            self.active("worker-00", "unhealthy"),
            self.active("worker-01", "unknown"),
        ]

        decision = select_replacement(active, [self.spare("worker-06")])

        self.assertFalse(decision["replacementAllowed"])
        self.assertEqual(decision["replacements"], [])

    def test_spare_exhaustion_is_inconclusive(self) -> None:
        active = [
            self.active("worker-00", "unhealthy"),
            self.active("worker-01", "healthy"),
        ]

        decision = select_replacement(active, [])

        self.assertFalse(decision["replacementAllowed"])
        self.assertEqual(decision["replacements"], [])
        self.assertIn("no healthy and idle spare is available", decision["reasons"])

    def test_busy_and_unknown_spares_are_not_selected(self) -> None:
        active = [
            self.active("worker-00", "unhealthy"),
            self.active("worker-01", "healthy"),
        ]
        spares = [
            self.spare("worker-06", eligible=False),
            self.spare("worker-07", status="unknown", eligible=False),
        ]

        decision = select_replacement(active, spares)

        self.assertFalse(decision["replacementAllowed"])
        self.assertEqual(decision["replacements"], [])

    def test_non_idle_healthy_survivor_prevents_restart(self) -> None:
        failed = self.active("worker-00", "unhealthy")
        survivor = self.active("worker-01", "healthy")
        survivor["idle"] = False

        decision = select_replacement(
            [failed, survivor],
            [self.spare("worker-06")],
        )

        self.assertFalse(decision["replacementAllowed"])
        self.assertFalse(decision["restartReady"])
        self.assertEqual(decision["replacements"], [])


class TrainingResultTests(unittest.TestCase):
    def test_failed_result_must_belong_to_the_current_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result_path = Path(temporary) / "execution-result.json"
            result_path.write_text(
                json.dumps(
                    {
                        "schemaVersion": "ray-training-result/v1",
                        "runId": "old-job-a00",
                        "status": "FAIL",
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaises(RecoveryError):
                recovery_supervisor.load_failed_training_result(
                    result_path,
                    expected_run_id="current-job-a00",
                )


class RecoveryLoopTests(unittest.TestCase):
    def test_successful_pipeline_without_a_valid_result_requires_manual_review(
        self,
    ) -> None:
        active = [f"worker-{index:02d}" for index in range(6)]
        spares = ["worker-06", "worker-07"]
        cases = (
            ("missing", None),
            (
                "wrong-run-id",
                {
                    "schemaVersion": "ray-training-result/v1",
                    "runId": "another-job-a00",
                    "status": "PASS",
                },
            ),
        )

        for case_name, exported_result in cases:
            with (
                self.subTest(case=case_name),
                tempfile.TemporaryDirectory() as temporary,
            ):
                root = Path(temporary)
                job_id = f"pretrain-success-{case_name}"
                args = SimpleNamespace(
                    run_id=job_id,
                    node=active,
                    spare_node=spares,
                    max_recoveries=2,
                    cleanup_timeout_seconds=30,
                    train_script=root / "train.sh",
                    recovery_state_root=root / "jobs",
                    failure_retention_seconds=0,
                    training_artifact_root=root / "training-runs",
                    kubectl_command="kubectl",
                    kubeconfig=root / "kubeconfig",
                    namespace="pretrain-ray",
                    cluster="pretrain-cluster",
                )

                def pass_pipeline(
                    _stages: object,
                    *,
                    failure_handler: object,
                ) -> int:
                    self.assertTrue(callable(failure_handler))
                    if exported_result is not None:
                        result_path = (
                            root
                            / "training-runs"
                            / f"{job_id}-a00"
                            / "execution-result.json"
                        )
                        result_path.parent.mkdir(parents=True, exist_ok=True)
                        result_path.write_text(
                            json.dumps(exported_result),
                            encoding="utf-8",
                        )
                    return 0

                with (
                    mock.patch.object(
                        recovery_supervisor,
                        "source_declared_npus",
                        return_value=8,
                    ),
                    mock.patch.object(
                        recovery_supervisor.start_ray,
                        "validate_args",
                    ),
                    mock.patch.object(
                        recovery_supervisor.start_ray,
                        "build_stage_commands",
                        return_value=(),
                    ),
                    mock.patch.object(
                        recovery_supervisor.start_ray,
                        "execute_pipeline",
                        side_effect=pass_pipeline,
                    ),
                    mock.patch.object(
                        recovery_supervisor,
                        "wait_for_cluster_cleanup",
                    ) as wait_for_cleanup,
                    mock.patch.object(
                        recovery_supervisor.recovery_diagnostics,
                        "diagnose_replacement",
                    ) as diagnose,
                ):
                    result = recovery_supervisor.run_supervisor(args)

                self.assertEqual(result, 1)
                wait_for_cleanup.assert_not_called()
                diagnose.assert_not_called()
                state_path = root / "jobs" / job_id / "state.json"
                state = json.loads(state_path.read_text(encoding="utf-8"))
                self.assertEqual(state["status"], "MANUAL_REQUIRED")
                self.assertEqual(state["activeNodes"], active)
                self.assertEqual(state["spareNodes"], spares)
                self.assertEqual(state["replacementCount"], 0)
                self.assertEqual(len(state["attempts"]), 1)
                attempt = state["attempts"][0]
                self.assertEqual(attempt["status"], "MANUAL_REQUIRED")
                self.assertIn("resultValidationFailure", attempt)
                self.assertNotIn("trainingResult", attempt)
                self.assertNotIn("diagnosis", attempt)

    def test_driver_internal_failure_is_cleaned_up_without_node_diagnosis(
        self,
    ) -> None:
        active = [f"worker-{index:02d}" for index in range(6)]
        spares = ["worker-06", "worker-07"]
        failure_result = {
            "schemaVersion": "ray-training-result/v1",
            "runId": "pretrain-driver-failure-a00",
            "status": "FAIL",
            "failureClass": "DRIVER_INTERNAL_FAILURE",
            "failure": "RuntimeError: driver bookkeeping failed",
        }

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            args = SimpleNamespace(
                run_id="pretrain-driver-failure",
                node=active,
                spare_node=spares,
                max_recoveries=2,
                cleanup_timeout_seconds=30,
                train_script=root / "train.sh",
                recovery_state_root=root / "jobs",
                failure_retention_seconds=0,
                training_artifact_root=root / "training-runs",
                kubectl_command="kubectl",
                kubeconfig=root / "kubeconfig",
                namespace="pretrain-ray",
                cluster="pretrain-cluster",
            )

            def fail_formal_training(
                _stages: object,
                *,
                failure_handler: object,
            ) -> int:
                self.assertTrue(callable(failure_handler))
                failure_handler(6, "formal Ray training")
                return 1

            with (
                mock.patch.object(
                    recovery_supervisor,
                    "source_declared_npus",
                    return_value=8,
                ),
                mock.patch.object(
                    recovery_supervisor.start_ray,
                    "validate_args",
                ),
                mock.patch.object(
                    recovery_supervisor.start_ray,
                    "build_stage_commands",
                    return_value=(),
                ),
                mock.patch.object(
                    recovery_supervisor.start_ray,
                    "execute_pipeline",
                    side_effect=fail_formal_training,
                ),
                mock.patch.object(
                    recovery_supervisor,
                    "load_failed_training_result",
                    return_value=failure_result,
                ) as load_result,
                mock.patch.object(
                    recovery_supervisor,
                    "wait_for_cluster_cleanup",
                ) as wait_for_cleanup,
                mock.patch.object(
                    recovery_supervisor.recovery_diagnostics,
                    "diagnose_replacement",
                ) as diagnose,
            ):
                result = recovery_supervisor.run_supervisor(args)

            self.assertEqual(result, 1)
            self.assertEqual(
                load_result.call_args.kwargs["expected_run_id"],
                "pretrain-driver-failure-a00",
            )
            wait_for_cleanup.assert_called_once()
            diagnose.assert_not_called()

            state_path = (
                root / "jobs" / "pretrain-driver-failure" / "state.json"
            )
            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(state["status"], "MANUAL_REQUIRED")
            self.assertEqual(state["activeNodes"], active)
            self.assertEqual(state["spareNodes"], spares)
            self.assertEqual(state["replacementCount"], 0)
            self.assertEqual(state["quarantinedNodes"], [])
            self.assertEqual(len(state["attempts"]), 1)
            attempt = state["attempts"][0]
            self.assertEqual(attempt["status"], "MANUAL_REQUIRED")
            self.assertEqual(attempt["trainingResult"], failure_result)
            self.assertNotIn("diagnosis", attempt)
            self.assertNotIn("replacements", attempt)

    def test_checkpoint_unavailable_is_cleaned_up_without_node_diagnosis(
        self,
    ) -> None:
        active = [f"worker-{index:02d}" for index in range(6)]
        spares = ["worker-06", "worker-07"]
        checkpoint_failure = {
            "status": "FAIL",
            "policy": {
                "loadDir": "/mnt/models/0717",
                "selection": "megatron-tracker",
            },
            "nodes": [],
            "failures": [
                {
                    "nodeRank": 0,
                    "failure": "checkpoint tracker is missing",
                }
            ],
        }

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            args = SimpleNamespace(
                run_id="pretrain-checkpoint-missing",
                node=active,
                spare_node=spares,
                max_recoveries=2,
                cleanup_timeout_seconds=30,
                train_script=root / "train.sh",
                recovery_state_root=root / "jobs",
                failure_retention_seconds=0,
                training_artifact_root=root / "training-runs",
                kubectl_command="kubectl",
                kubeconfig=root / "kubeconfig",
                namespace="pretrain-ray",
                cluster="pretrain-cluster",
            )

            def fail_checkpoint_preflight(
                _stages: object,
                *,
                failure_handler: object,
            ) -> int:
                self.assertTrue(callable(failure_handler))
                failure_handler(6, "formal Ray training")
                return 1

            with (
                mock.patch.object(
                    recovery_supervisor,
                    "source_declared_npus",
                    return_value=8,
                ),
                mock.patch.object(
                    recovery_supervisor.start_ray,
                    "validate_args",
                ) as validate_args,
                mock.patch.object(
                    recovery_supervisor.start_ray,
                    "build_stage_commands",
                    return_value=(),
                ),
                mock.patch.object(
                    recovery_supervisor.start_ray,
                    "execute_pipeline",
                    side_effect=fail_checkpoint_preflight,
                ),
                mock.patch.object(
                    recovery_supervisor,
                    "load_failed_training_result",
                    return_value={
                        "schemaVersion": "ray-training-result/v1",
                        "runId": "pretrain-checkpoint-missing-a00",
                        "status": "FAIL",
                        "failureClass": "CHECKPOINT_UNAVAILABLE",
                        "checkpoint": checkpoint_failure,
                    },
                ),
                mock.patch.object(
                    recovery_supervisor,
                    "wait_for_cluster_cleanup",
                ) as wait_for_cleanup,
                mock.patch.object(
                    recovery_supervisor.recovery_diagnostics,
                    "diagnose_replacement",
                ) as diagnose,
            ):
                result = recovery_supervisor.run_supervisor(args)

            self.assertEqual(result, 1)
            wait_for_cleanup.assert_called_once()
            diagnose.assert_not_called()
            self.assertTrue(
                validate_args.call_args.args[0].require_resumable_checkpoint
            )

            state_path = (
                root
                / "jobs"
                / "pretrain-checkpoint-missing"
                / "state.json"
            )
            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(state["status"], "MANUAL_REQUIRED")
            self.assertEqual(state["activeNodes"], active)
            self.assertEqual(state["spareNodes"], spares)
            self.assertEqual(state["replacementCount"], 0)
            self.assertEqual(len(state["attempts"]), 1)
            self.assertEqual(
                state["attempts"][0]["checkpointFailure"],
                checkpoint_failure,
            )
            self.assertNotIn("diagnosis", state["attempts"][0])

    def test_one_failure_replaces_node_then_starts_a_new_attempt(self) -> None:
        active = [f"worker-{index:02d}" for index in range(6)]
        spares = ["worker-06", "worker-07"]
        replacement_active = [
            "worker-00",
            "worker-01",
            "worker-06",
            "worker-03",
            "worker-04",
            "worker-05",
        ]
        diagnosis = {
            "schemaVersion": 1,
            "outcome": "replacements_selected",
            "replacementAllowed": True,
            "restartReady": True,
            "replacements": [
                {
                    "failedNode": "worker-02",
                    "failedNodeName": "worker-02",
                    "spareNode": "worker-06",
                    "spareNodeName": "worker-06",
                }
            ],
            "reasons": [],
        }

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            args = SimpleNamespace(
                run_id="pretrain-job-42",
                node=active,
                spare_node=spares,
                max_recoveries=1,
                cleanup_timeout_seconds=30,
                train_script=root / "train.sh",
                recovery_state_root=root / "jobs",
                failure_retention_seconds=0,
                training_artifact_root=root / "training-runs",
                kubectl_command="kubectl",
                kubeconfig=root / "kubeconfig",
                namespace="pretrain-ray",
                cluster="pretrain-cluster",
                hccl_evidence_root=root / "hccl-runs",
                manifest=root / "raycluster.yaml",
                runtime_configmap="ray-runtime-source",
                runtime_source_dir=root / "runtime",
                timeout_seconds=60,
                hccl_timeout_seconds=300,
                expected_world_size=48,
                training_cwd="/home/ywj/MindSpeed-LLM",
                master_port=None,
                allow_topology_change=False,
                confirm_checkpoint_exclusive=True,
                training_timeout_seconds=3600,
                keep_success_resources=False,
            )
            pipeline_attempt = 0
            built_stages: list[tuple[tuple[str, list[str]], ...]] = []
            exported_results: list[dict[str, object]] = []

            def execute_pipeline(
                stages: tuple[tuple[str, list[str]], ...],
                *,
                failure_handler: object,
            ) -> int:
                nonlocal pipeline_attempt
                pipeline_attempt += 1
                built_stages.append(stages)
                self.assertEqual(
                    [name for name, _command in stages],
                    [
                        "environment check",
                        "per-run RayCluster rendering",
                        "Ray cluster startup",
                        "topology, RankTable, and HCCL gate",
                        "formal training parameter injection",
                        "formal Ray training",
                    ],
                )
                injection_command = stages[4][1]
                self.assertIn("--require-resumable-checkpoint", injection_command)
                training_command = stages[5][1]
                result_path = Path(
                    training_command[training_command.index("--result") + 1]
                )
                run_id = result_path.parent.name
                checkpoint_iteration = 960 if pipeline_attempt == 1 else 1000
                checkpoint_dir = (
                    f"/mnt/models/0717/iter_{checkpoint_iteration:07d}"
                )
                checkpoint = {
                    "status": "PASS",
                    "policy": {
                        "requiredForRecovery": True,
                        "loadDir": "/mnt/models/0717",
                        "selection": "megatron-tracker",
                    },
                    "selectedIteration": checkpoint_iteration,
                    "selectedDir": checkpoint_dir,
                    "nodes": [
                        {
                            "status": "AVAILABLE_RESUME",
                            "identity": {"nodeName": node},
                            "iteration": checkpoint_iteration,
                            "selectedDir": checkpoint_dir,
                        }
                        for node in (
                            active if pipeline_attempt == 1 else replacement_active
                        )
                    ],
                }
                payload: dict[str, object] = {
                    "schemaVersion": "ray-training-result/v1",
                    "runId": run_id,
                    "status": "FAIL" if pipeline_attempt == 1 else "PASS",
                    "checkpoint": checkpoint,
                }
                exported_results.append(payload)
                result_path.parent.mkdir(parents=True, exist_ok=True)
                result_path.write_text(json.dumps(payload), encoding="utf-8")
                if pipeline_attempt == 1:
                    self.assertTrue(callable(failure_handler))
                    failure_handler(6, "formal Ray training")
                    return 1
                return 0

            with (
                mock.patch.object(
                    recovery_supervisor,
                    "source_declared_npus",
                    return_value=8,
                ),
                mock.patch.object(
                    recovery_supervisor.start_ray,
                    "validate_args",
                ) as validate_args,
                mock.patch.object(
                    recovery_supervisor.start_ray,
                    "build_stage_commands",
                    wraps=recovery_supervisor.start_ray.build_stage_commands,
                ) as build_stages,
                mock.patch.object(
                    recovery_supervisor.start_ray,
                    "execute_pipeline",
                    side_effect=execute_pipeline,
                ),
                mock.patch.object(
                    recovery_supervisor,
                    "wait_for_cluster_cleanup",
                ) as wait_for_cleanup,
                mock.patch.object(
                    recovery_supervisor.recovery_diagnostics,
                    "diagnose_replacement",
                    return_value=diagnosis,
                ) as diagnose,
            ):
                result = recovery_supervisor.run_supervisor(args)

            self.assertEqual(result, 0)
            self.assertEqual(
                [call.args[1] for call in validate_args.call_args_list],
                ["pretrain-job-42-a00", "pretrain-job-42-a01"],
            )
            self.assertEqual(build_stages.call_count, 2)
            self.assertEqual(len(built_stages), 2)
            self.assertTrue(
                all(
                    call.args[0].require_resumable_checkpoint
                    for call in build_stages.call_args_list
                )
            )
            self.assertEqual(
                build_stages.call_args_list[0].args[0].node,
                active,
            )
            self.assertEqual(
                build_stages.call_args_list[1].args[0].node,
                replacement_active,
            )
            wait_for_cleanup.assert_called_once()
            diagnose.assert_called_once()
            self.assertEqual(
                diagnose.call_args.kwargs["active_nodes"],
                tuple(active),
            )
            self.assertEqual(
                diagnose.call_args.kwargs["spare_nodes"],
                tuple(spares),
            )

            state_path = root / "jobs" / "pretrain-job-42" / "state.json"
            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(state["status"], "PASS")
            self.assertEqual(state["activeNodes"], replacement_active)
            self.assertEqual(state["spareNodes"], ["worker-07"])
            self.assertEqual(state["quarantinedNodes"], ["worker-02"])
            self.assertEqual(state["replacementCount"], 1)
            self.assertEqual(
                [attempt["runId"] for attempt in state["attempts"]],
                ["pretrain-job-42-a00", "pretrain-job-42-a01"],
            )
            self.assertEqual(state["attempts"][0]["failedStageIndex"], 6)
            self.assertEqual(
                state["attempts"][0]["failedStageName"],
                "formal Ray training",
            )
            self.assertEqual(
                state["attempts"][0]["trainingResult"]["checkpoint"][
                    "selectedIteration"
                ],
                960,
            )
            self.assertEqual(
                state["attempts"][0]["replacements"],
                [
                    {
                        "badTarget": "worker-02",
                        "replacementTarget": "worker-06",
                    }
                ],
            )
            resumed_attempt = state["attempts"][1]
            self.assertEqual(resumed_attempt["status"], "PASS")
            self.assertEqual(resumed_attempt["activeNodes"], replacement_active)
            self.assertEqual(
                resumed_attempt["trainingResult"],
                exported_results[1],
            )
            self.assertEqual(
                resumed_attempt["trainingResult"]["checkpoint"][
                    "selectedIteration"
                ],
                1000,
            )
            self.assertEqual(
                resumed_attempt["trainingResult"]["checkpoint"]["selectedDir"],
                "/mnt/models/0717/iter_0001000",
            )
            self.assertEqual(
                len(resumed_attempt["trainingResult"]["checkpoint"]["nodes"]),
                6,
            )

    def test_two_failures_consume_two_spares_in_one_recovery_attempt(self) -> None:
        active = [f"worker-{index:02d}" for index in range(6)]
        spares = ["worker-06", "worker-07"]
        diagnosis = {
            "schemaVersion": 1,
            "outcome": "replacements_selected",
            "replacementAllowed": True,
            "restartReady": True,
            "replacementCount": 2,
            "replacements": [
                {
                    "failedNode": "worker-01",
                    "failedNodeName": "worker-01",
                    "spareNode": "worker-06",
                    "spareNodeName": "worker-06",
                },
                {
                    "failedNode": "worker-04",
                    "failedNodeName": "worker-04",
                    "spareNode": "worker-07",
                    "spareNodeName": "worker-07",
                },
            ],
            "reasons": [],
        }

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            args = SimpleNamespace(
                run_id="pretrain-two-failures",
                node=active,
                spare_node=spares,
                max_recoveries=2,
                cleanup_timeout_seconds=30,
                train_script=root / "train.sh",
                recovery_state_root=root / "jobs",
                failure_retention_seconds=0,
                training_artifact_root=root / "training-runs",
                kubectl_command="kubectl",
                kubeconfig=root / "kubeconfig",
                namespace="pretrain-ray",
                cluster="pretrain-cluster",
            )
            pipeline_attempt = 0
            success_result = {
                "schemaVersion": "ray-training-result/v1",
                "runId": "pretrain-two-failures-a01",
                "status": "PASS",
                "checkpoint": {
                    "status": "PASS",
                    "policy": {
                        "requiredForRecovery": True,
                        "selection": "megatron-tracker",
                    },
                    "selectedIteration": 960,
                    "selectedDir": "/mnt/models/0717/iter_0000960",
                    "nodes": [
                        {
                            "status": "AVAILABLE_RESUME",
                            "nodeRank": node_rank,
                            "iteration": 960,
                            "selectedDir": "/mnt/models/0717/iter_0000960",
                        }
                        for node_rank in range(6)
                    ],
                },
            }

            def execute_pipeline(
                _stages: object,
                *,
                failure_handler: object,
            ) -> int:
                nonlocal pipeline_attempt
                pipeline_attempt += 1
                if pipeline_attempt == 1:
                    self.assertTrue(callable(failure_handler))
                    failure_handler(6, "formal Ray training")
                    return 1
                success_path = (
                    root
                    / "training-runs"
                    / "pretrain-two-failures-a01"
                    / "execution-result.json"
                )
                success_path.parent.mkdir(parents=True, exist_ok=True)
                success_path.write_text(
                    json.dumps(success_result),
                    encoding="utf-8",
                )
                return 0

            with (
                mock.patch.object(
                    recovery_supervisor,
                    "source_declared_npus",
                    return_value=8,
                ),
                mock.patch.object(
                    recovery_supervisor.start_ray,
                    "validate_args",
                ) as validate_args,
                mock.patch.object(
                    recovery_supervisor.start_ray,
                    "build_stage_commands",
                    return_value=(),
                ),
                mock.patch.object(
                    recovery_supervisor.start_ray,
                    "execute_pipeline",
                    side_effect=execute_pipeline,
                ),
                mock.patch.object(
                    recovery_supervisor,
                    "load_failed_training_result",
                    return_value={
                        "schemaVersion": "ray-training-result/v1",
                        "runId": "pretrain-two-failures-a00",
                        "status": "FAIL",
                    },
                ),
                mock.patch.object(
                    recovery_supervisor,
                    "wait_for_cluster_cleanup",
                ),
                mock.patch.object(
                    recovery_supervisor.recovery_diagnostics,
                    "diagnose_replacement",
                    return_value=diagnosis,
                ),
            ):
                result = recovery_supervisor.run_supervisor(args)

            self.assertEqual(result, 0)
            self.assertEqual(
                [call.args[1] for call in validate_args.call_args_list],
                ["pretrain-two-failures-a00", "pretrain-two-failures-a01"],
            )

            state_path = root / "jobs" / "pretrain-two-failures" / "state.json"
            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(len(state["attempts"]), 2)
            self.assertEqual(state["replacementCount"], 2)
            self.assertEqual(state["maxReplacementCount"], 2)
            self.assertEqual(state["remainingSpareCount"], 0)
            self.assertEqual(
                state["activeNodes"],
                [
                    "worker-00",
                    "worker-06",
                    "worker-02",
                    "worker-03",
                    "worker-07",
                    "worker-05",
                ],
            )
            self.assertEqual(state["spareNodes"], [])
            self.assertEqual(
                state["quarantinedNodes"],
                ["worker-01", "worker-04"],
            )
            self.assertEqual(state["attempts"][0]["replacementCount"], 2)
            self.assertEqual(
                state["attempts"][0]["replacements"],
                [
                    {
                        "badTarget": "worker-01",
                        "replacementTarget": "worker-06",
                    },
                    {
                        "badTarget": "worker-04",
                        "replacementTarget": "worker-07",
                    },
                ],
            )
            self.assertEqual(
                state["attempts"][1]["trainingResult"],
                success_result,
            )

    def test_early_stage_failure_ignores_stale_training_failure_result(self) -> None:
        active = [f"worker-{index:02d}" for index in range(6)]
        spares = ["worker-06", "worker-07"]

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            args = SimpleNamespace(
                run_id="pretrain-startup-failure",
                node=active,
                spare_node=spares,
                max_recoveries=2,
                cleanup_timeout_seconds=30,
                train_script=root / "train.sh",
                recovery_state_root=root / "jobs",
                failure_retention_seconds=0,
                training_artifact_root=root / "training-runs",
                kubectl_command="kubectl",
                kubeconfig=root / "kubeconfig",
                namespace="pretrain-ray",
                cluster="pretrain-cluster",
            )

            def fail_before_training(
                _stages: object,
                *,
                failure_handler: object,
            ) -> int:
                self.assertTrue(callable(failure_handler))
                failure_handler(3, "Ray cluster startup")
                return 1

            with (
                mock.patch.object(
                    recovery_supervisor,
                    "source_declared_npus",
                    return_value=8,
                ),
                mock.patch.object(
                    recovery_supervisor.start_ray,
                    "validate_args",
                ),
                mock.patch.object(
                    recovery_supervisor.start_ray,
                    "build_stage_commands",
                    return_value=(),
                ),
                mock.patch.object(
                    recovery_supervisor.start_ray,
                    "execute_pipeline",
                    side_effect=fail_before_training,
                ),
                mock.patch.object(
                    recovery_supervisor,
                    "load_failed_training_result",
                    return_value={
                        "schemaVersion": "ray-training-result/v1",
                        "runId": "some-old-attempt-a00",
                        "status": "FAIL",
                    },
                ) as load_result,
                mock.patch.object(
                    recovery_supervisor,
                    "wait_for_cluster_cleanup",
                ) as wait_for_cleanup,
                mock.patch.object(
                    recovery_supervisor.recovery_diagnostics,
                    "diagnose_replacement",
                ) as diagnose,
            ):
                result = recovery_supervisor.run_supervisor(args)

            self.assertEqual(result, 1)
            load_result.assert_not_called()
            wait_for_cleanup.assert_not_called()
            diagnose.assert_not_called()

            state_path = (
                root / "jobs" / "pretrain-startup-failure" / "state.json"
            )
            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(state["status"], "MANUAL_REQUIRED")
            self.assertEqual(state["activeNodes"], active)
            self.assertEqual(state["spareNodes"], spares)
            self.assertEqual(state["replacementCount"], 0)
            self.assertEqual(len(state["attempts"]), 1)
            self.assertEqual(state["attempts"][0]["failedStageIndex"], 3)
            self.assertEqual(
                state["attempts"][0]["failedStageName"],
                "Ray cluster startup",
            )


class ClusterCleanupTests(unittest.TestCase):
    @staticmethod
    def completed(stdout: str) -> object:
        return recovery_supervisor.subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=stdout,
            stderr="",
        )

    def test_cleanup_reissues_delete_then_waits_for_every_cluster_resource(self) -> None:
        owner = self.completed(
            json.dumps(
                {
                    "metadata": {
                        "annotations": {
                            "trainctl.io/run-id": "pretrain-job-42-a00"
                        }
                    }
                }
            )
        )
        first_poll = (
            self.completed(""),
            self.completed("raycluster.ray.io/pretrain-cluster\n"),
            self.completed('{"items":[{"metadata":{"name":"old-worker"}}]}'),
            self.completed('{"items":[{"metadata":{"name":"old-head"}}]}'),
            self.completed(
                "configmap/job-summary-pretrain-cluster\n"
                "configmap/hccl-sanitized-pretrain-cluster\n"
            ),
        )
        second_poll = (
            self.completed(""),
            self.completed('{"items":[]}'),
            self.completed('{"items":[]}'),
            self.completed(""),
        )

        with mock.patch.object(
            recovery_supervisor.subprocess,
            "run",
            side_effect=(owner, *first_poll, *second_poll),
        ) as run:
            recovery_supervisor.wait_for_cluster_cleanup(
                kubectl_command="kubectl",
                kubeconfig=None,
                namespace="pretrain-ray",
                cluster="pretrain-cluster",
                run_id="pretrain-job-42-a00",
                timeout_seconds=10,
                poll_seconds=0,
            )

        commands = [call.args[0] for call in run.call_args_list]
        self.assertEqual(
            commands[0],
            [
                "kubectl",
                "get",
                "raycluster",
                "pretrain-cluster",
                "-n",
                "pretrain-ray",
                "--ignore-not-found",
                "-o",
                "json",
            ],
        )
        self.assertEqual(
            commands[1],
            [
                "kubectl",
                "delete",
                "raycluster",
                "pretrain-cluster",
                "-n",
                "pretrain-ray",
                "--ignore-not-found=true",
                "--wait=false",
            ],
        )
        expected_queries = (
            [
                "kubectl",
                "get",
                "raycluster",
                "pretrain-cluster",
                "-n",
                "pretrain-ray",
                "--ignore-not-found",
                "-o",
                "name",
            ],
            [
                "kubectl",
                "get",
                "pods",
                "-n",
                "pretrain-ray",
                "-l",
                "ray.io/cluster=pretrain-cluster",
                "-o",
                "json",
            ],
            [
                "kubectl",
                "get",
                "service",
                "-n",
                "pretrain-ray",
                "-l",
                "ray.io/cluster=pretrain-cluster",
                "-o",
                "json",
            ],
            [
                "kubectl",
                "get",
                "configmap",
                "job-summary-pretrain-cluster",
                "hccl-sanitized-pretrain-cluster",
                "-n",
                "pretrain-ray",
                "--ignore-not-found",
                "-o",
                "name",
            ],
        )
        self.assertEqual(len(commands), 10)
        for query in expected_queries:
            self.assertEqual(commands.count(query), 2)

    def test_cleanup_refuses_to_delete_cluster_owned_by_another_run(self) -> None:
        other_owner = self.completed(
            json.dumps(
                {
                    "metadata": {
                        "annotations": {
                            "trainctl.io/run-id": "another-job-a00"
                        }
                    }
                }
            )
        )

        with mock.patch.object(
            recovery_supervisor.subprocess,
            "run",
            return_value=other_owner,
        ) as run:
            with self.assertRaises(RecoveryError):
                recovery_supervisor.wait_for_cluster_cleanup(
                    kubectl_command="kubectl",
                    kubeconfig=None,
                    namespace="pretrain-ray",
                    cluster="pretrain-cluster",
                    run_id="pretrain-job-42-a00",
                    timeout_seconds=10,
                    poll_seconds=0,
                )

        self.assertEqual(run.call_count, 1)
        only_command = run.call_args.args[0]
        self.assertEqual(only_command[1:3], ["get", "raycluster"])
        self.assertNotIn("delete", only_command)


if __name__ == "__main__":
    unittest.main()
