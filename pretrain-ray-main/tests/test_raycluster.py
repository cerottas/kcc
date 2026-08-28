import unittest
from dataclasses import replace

from kcc_training.api_v1beta1 import Recipe, Run, RuntimeProfile
from kcc_training.raycluster import attempt_name, render_attempt, render_control
from tests.test_api_v1beta1 import IMAGE, resource


def objects():
    profile = RuntimeProfile.from_resource(resource("TrainingRuntimeProfile", "a3", {
        "images": {"head": IMAGE, "worker": IMAGE}, "rayVersion": "2.49.0",
        "accelerator": {"resourceName": "huawei.com/Ascend910", "devicesPerNode": 8, "runtimeClassName": "ascend"},
        "workspace": {"claimName": "workspace", "mountPath": "/workspace"},
        "scheduling": {"activeNodes": ["node-a", "node-b"], "spareNodes": ["node-c"], "headSelector": {"arch": "amd64"}, "workerSelector": {"npu": "a3"}},
        "integrations": {"rankTableProvider": "clusterd", "healthProvider": "npu-exporter"},
    }))
    recipe = Recipe.from_resource(resource("TrainingRecipe", "recipe", {
        "framework": "mindspeed", "command": ["python", "pretrain.py"], "workingDirectory": ".", "environment": {},
        "artifacts": {"source": "artifact://training/source/v1", "model": "artifact://training/model/v1", "data": "artifact://training/data/v1", "outputSubpath": "runs/x"},
    }))
    run = Run.from_resource(resource("TrainingRun", "run-1", {
        "runtimeProfile": "a3", "recipe": "recipe", "workers": 2,
        "recovery": {"sameTopologyRetries": 2, "maxReplacements": 1, "noProgressSeconds": 3600},
    }))
    return run, profile, recipe


class RayClusterTests(unittest.TestCase):
    def test_attempt_name_reserves_kuberay_head_service_suffix(self) -> None:
        first = attempt_name("r" * 63, 12)
        second = attempt_name("s" * 63, 12)
        self.assertEqual(len(first), 54)
        self.assertEqual(len(f"{first}-head-svc"), 63)
        self.assertTrue(first.endswith("-a12"))
        self.assertNotEqual(first, second)

    def test_manifest_mounts_ascend_driver_without_kubeconfig(self) -> None:
        configmap, cluster = render_attempt(*objects(), attempt=0, active_nodes=("node-a", "node-b"), runtime_service_account="runtime")
        rendered = str((configmap, cluster))
        self.assertNotIn("kubeconfig", rendered.lower())
        worker = cluster["spec"]["workerGroupSpecs"][0]["template"]["spec"]
        volumes = worker["volumes"]
        self.assertTrue(any("persistentVolumeClaim" in volume for volume in volumes))
        self.assertIn(
            {
                "name": "ascend-driver",
                "hostPath": {
                    "path": "/usr/local/Ascend/driver",
                    "type": "Directory",
                },
            },
            volumes,
        )
        self.assertIn(
            {
                "name": "ascend-driver",
                "mountPath": "/usr/local/Ascend/driver",
                "readOnly": True,
            },
            worker["containers"][0]["volumeMounts"],
        )
        head_volumes = cluster["spec"]["headGroupSpec"]["template"]["spec"]["volumes"]
        self.assertNotIn("hostPath", str(head_volumes))


    def test_attempt_mounts_owned_runtime_control(self) -> None:
        run, profile, recipe = objects()
        control = render_control(
            run,
            attempt=0,
            action="StopAfterCheckpoint",
            request_generation=2,
        )
        self.assertEqual(control["metadata"]["name"], "run-1-a00-control")
        self.assertEqual(
            control["metadata"]["annotations"]["training.kcc.io/run-uid"],
            run.identity.uid,
        )
        _configmap, cluster = render_attempt(
            run,
            profile,
            recipe,
            attempt=0,
            active_nodes=("node-a", "node-b"),
            runtime_service_account="runtime",
        )
        volumes = cluster["spec"]["headGroupSpec"]["template"]["spec"]["volumes"]
        self.assertIn(
            {"name": "control", "configMap": {"name": "run-1-a00-control"}},
            volumes,
        )

    def test_portable_pod_options_are_rendered(self) -> None:
        run, profile, recipe = objects()
        profile = replace(
            profile,
            image_pull_secrets=("registry-credentials",),
            head_tolerations=({"operator": "Exists"},),
            worker_tolerations=({"key": "accelerator", "operator": "Exists"},),
            head_priority_class_name="training-head",
            worker_priority_class_name="training-worker",
            head_ray_cpus=4,
            worker_ray_cpus=64,
            runtime_class_name=None,
        )
        _configmap, cluster = render_attempt(
            run,
            profile,
            recipe,
            attempt=0,
            active_nodes=("node-a", "node-b"),
            runtime_service_account="runtime",
        )
        head = cluster["spec"]["headGroupSpec"]
        worker = cluster["spec"]["workerGroupSpecs"][0]
        self.assertEqual(head["rayStartParams"]["num-cpus"], "4")
        self.assertEqual(worker["rayStartParams"]["num-cpus"], "64")
        self.assertEqual(head["template"]["spec"]["imagePullSecrets"], [{"name": "registry-credentials"}])
        self.assertNotIn("runtimeClassName", worker["template"]["spec"])

    def test_cpu_only_ascend_head_skips_volcano_device_validation(self) -> None:
        _configmap, cluster = render_attempt(
            *objects(),
            attempt=0,
            active_nodes=("node-a", "node-b"),
            runtime_service_account="runtime",
        )
        head = cluster["spec"]["headGroupSpec"]["template"]
        worker = cluster["spec"]["workerGroupSpecs"][0]["template"]
        self.assertEqual(
            head["metadata"]["annotations"]["huawei.com/skip-ascend-plugin"],
            "enabled",
        )
        self.assertNotIn(
            "huawei.com/skip-ascend-plugin", worker["metadata"].get("annotations", {})
        )

    def test_worker_anti_affinity_does_not_exclude_the_head(self) -> None:
        _configmap, cluster = render_attempt(
            *objects(),
            attempt=0,
            active_nodes=("node-a", "node-b"),
            runtime_service_account="runtime",
        )
        head = cluster["spec"]["headGroupSpec"]["template"]
        worker = cluster["spec"]["workerGroupSpecs"][0]["template"]
        match_labels = worker["spec"]["affinity"]["podAntiAffinity"][
            "requiredDuringSchedulingIgnoredDuringExecution"
        ][0]["labelSelector"]["matchLabels"]
        self.assertEqual(head["metadata"]["labels"]["training.kcc.io/role"], "head")
        self.assertEqual(
            worker["metadata"]["labels"]["training.kcc.io/role"], "worker"
        )
        self.assertEqual(match_labels["training.kcc.io/role"], "worker")

    def test_worker_uses_explicit_ascend_physical_devices(self) -> None:
        run, profile, recipe = objects()
        profile = replace(
            profile,
            devices_per_node=2,
            physical_device_ids=(0, 1),
        )
        _configmap, cluster = render_attempt(
            run,
            profile,
            recipe,
            attempt=0,
            active_nodes=("node-a", "node-b"),
            runtime_service_account="runtime",
        )
        worker = cluster["spec"]["workerGroupSpecs"][0]["template"]
        self.assertEqual(
            worker["metadata"]["annotations"]["huawei.com/Ascend910"],
            "Ascend910-0,Ascend910-1",
        )
        environment = {
            item["name"]: item.get("value")
            for item in worker["spec"]["containers"][0]["env"]
        }
        self.assertEqual(environment["ASCEND_VISIBLE_DEVICES"], "0,1")
        self.assertEqual(environment["ASCEND_RT_VISIBLE_DEVICES"], "0,1")
        self.assertEqual(
            environment["RAY_EXPERIMENTAL_NOSET_ASCEND_RT_VISIBLE_DEVICES"],
            "1",
        )

    def test_nodes_outside_admin_profile_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "outside"):
            render_attempt(*objects(), attempt=0, active_nodes=("node-a", "rogue"), runtime_service_account="runtime")


if __name__ == "__main__":
    unittest.main()
