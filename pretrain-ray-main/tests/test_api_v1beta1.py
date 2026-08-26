import unittest

from kcc_training.api_v1beta1 import ApiValidationError, Recipe, Run, RuntimeProfile


META = {"namespace": "training", "uid": "uid-1", "resourceVersion": "7", "generation": 1}
IMAGE = "registry.local/kcc/image@sha256:" + "a" * 64


def resource(kind, name, spec):
    return {
        "apiVersion": "training.kcc.io/v1beta1",
        "kind": kind,
        "metadata": {"name": name, **META},
        "spec": spec,
    }


class ApiV1Beta1Tests(unittest.TestCase):
    def test_profile_is_cluster_owned_and_digest_pinned(self) -> None:
        profile = RuntimeProfile.from_resource(
            resource(
                "TrainingRuntimeProfile",
                "a3",
                {
                    "images": {"head": IMAGE, "worker": IMAGE},
                    "rayVersion": "2.49.0",
                    "accelerator": {
                        "resourceName": "huawei.com/Ascend910",
                        "devicesPerNode": 8,
                        "runtimeClassName": "ascend",
                    },
                    "workspace": {"claimName": "workspace", "mountPath": "/workspace"},
                    "scheduling": {
                        "activeNodes": ["node-a", "node-b"],
                        "spareNodes": ["node-c"],
                        "headSelector": {"kubernetes.io/arch": "amd64"},
                        "workerSelector": {"npu": "a3"},
                    },
                    "integrations": {
                        "rankTableProvider": "clusterd",
                        "healthProvider": "npu-exporter",
                    },
                },
            )
        )
        self.assertEqual(profile.devices_per_node, 8)
        self.assertEqual(profile.active_nodes, ("node-a", "node-b"))

    def test_profile_supports_portable_pod_options_and_empty_spares(self) -> None:
        profile = RuntimeProfile.from_resource(
            resource(
                "TrainingRuntimeProfile",
                "portable",
                {
                    "images": {"head": IMAGE, "worker": IMAGE, "pullSecrets": ["registry-credentials"]},
                    "rayVersion": "2.49.0",
                    "accelerator": {"resourceName": "huawei.com/Ascend910", "devicesPerNode": 8},
                    "workspace": {"claimName": "workspace", "mountPath": "/workspace"},
                    "scheduling": {
                        "activeNodes": ["node-a"],
                        "spareNodes": [],
                        "headSelector": {},
                        "workerSelector": {},
                    },
                    "integrations": {"rankTableProvider": "clusterd", "healthProvider": "kubernetes"},
                    "podTemplate": {
                        "head": {"rayCpus": 4, "priorityClassName": "training", "tolerations": [{"operator": "Exists"}]},
                        "worker": {
                            "rayCpus": 64,
                            "resources": {
                                "requests": {"cpu": "64", "memory": "256Gi"},
                                "limits": {"cpu": "64", "memory": "256Gi"},
                            },
                        },
                    },
                },
            )
        )
        self.assertEqual(profile.spare_nodes, ())
        self.assertIsNone(profile.runtime_class_name)
        self.assertEqual(profile.image_pull_secrets, ("registry-credentials",))
        self.assertEqual(profile.worker_ray_cpus, 64)

    def test_profile_rejects_unimplemented_ranktable_provider(self) -> None:
        document = resource(
            "TrainingRuntimeProfile",
            "bad-ranktable",
            {
                "images": {"head": IMAGE, "worker": IMAGE},
                "rayVersion": "2.49.0",
                "accelerator": {"resourceName": "huawei.com/Ascend910", "devicesPerNode": 8},
                "workspace": {"claimName": "workspace", "mountPath": "/workspace"},
                "scheduling": {"activeNodes": ["node-a"], "spareNodes": [], "headSelector": {}, "workerSelector": {}},
                "integrations": {"rankTableProvider": "static", "healthProvider": "kubernetes"},
            },
        )
        with self.assertRaisesRegex(ApiValidationError, "must be clusterd"):
            RuntimeProfile.from_resource(document)

    def test_recipe_rejects_controller_environment_override(self) -> None:
        with self.assertRaisesRegex(ApiValidationError, "controller-owned"):
            Recipe.from_resource(
                resource(
                    "TrainingRecipe",
                    "recipe",
                    {
                        "framework": "mindspeed",
                        "command": ["python", "pretrain.py"],
                        "workingDirectory": ".",
                        "environment": {"MASTER_ADDR": "wrong"},
                        "artifacts": {
                            "source": "artifact://training/source/v1",
                            "model": "artifact://training/model/v1",
                            "data": "artifact://training/data/v1",
                            "outputSubpath": "runs/x",
                        },
                    },
                )
            )

    def test_run_has_bounded_recovery_policy(self) -> None:
        run = Run.from_resource(
            resource(
                "TrainingRun",
                "run-1",
                {
                    "runtimeProfile": "a3",
                    "recipe": "recipe",
                    "workers": 2,
                    "recovery": {
                        "sameTopologyRetries": 2,
                        "maxReplacements": 1,
                        "noProgressSeconds": 3600,
                    },
                },
            )
        )
        self.assertEqual(run.workers, 2)
        self.assertFalse(run.suspended)

    def test_run_supports_checkpoint_suspend_mode(self) -> None:
        document = resource(
            "TrainingRun",
            "run-1",
            {
                "runtimeProfile": "a3",
                "recipe": "recipe",
                "workers": 2,
                "suspend": True,
                "suspendMode": "AfterCheckpoint",
                "recovery": {
                    "sameTopologyRetries": 1,
                    "maxReplacements": 0,
                    "noProgressSeconds": 3600,
                },
            },
        )
        self.assertEqual(
            Run.from_resource(document).suspend_mode,
            "AfterCheckpoint",
        )
        document["spec"]["suspendMode"] = "unsafe"
        with self.assertRaisesRegex(ApiValidationError, "suspendMode"):
            Run.from_resource(document)

    def test_run_rejects_excessive_combined_recovery_budget(self) -> None:
        document = resource(
            "TrainingRun",
            "run-1",
            {
                "runtimeProfile": "a3",
                "recipe": "recipe",
                "workers": 2,
                "recovery": {
                    "sameTopologyRetries": 10,
                    "maxReplacements": 100,
                    "noProgressSeconds": 3600,
                },
            },
        )
        with self.assertRaisesRegex(ApiValidationError, "permits 1111 attempts"):
            Run.from_resource(document)


if __name__ == "__main__":
    unittest.main()
