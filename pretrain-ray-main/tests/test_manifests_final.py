import json
import os
import unittest
from unittest.mock import patch

from kcc_training.manifests_final import render_attempt
from tests.test_raycluster import objects


class FinalManifestTests(unittest.TestCase):
    def test_head_materializes_artifacts_before_ray_starts(self):
        with patch.dict(os.environ, {"KCC_ARTIFACT_GATEWAY": "https://artifacts.internal"}, clear=False):
            configmap, cluster = render_attempt(
                *objects(), attempt=0, active_nodes=("node-a", "node-b"), runtime_service_account="runtime"
            )
        runtime = json.loads(configmap["data"]["run.json"])
        source_target = runtime["artifacts"]["source"]["target"]
        self.assertTrue(source_target.startswith("/workspace/.kcc/artifacts/source/"))
        self.assertEqual(runtime["training"]["workingDirectory"], source_target)
        worker_env = cluster["spec"]["workerGroupSpecs"][0]["template"]["spec"]["containers"][0]["env"]
        self.assertEqual({item["name"] for item in worker_env}, {"NODE_NAME", "POD_NAME", "POD_IP", "HOST_IP"})
        self.assertEqual(runtime["training"]["environment"]["KCC_CHECKPOINT_ROOT"], runtime["artifacts"]["checkpointRoot"])
        init = cluster["spec"]["headGroupSpec"]["template"]["spec"]["initContainers"][0]
        self.assertEqual(init["command"], ["python", "-m", "kcc_training.runtime.materializer"])
        self.assertNotIn("hostPath", str(cluster))

    def test_gateway_is_mandatory(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "GATEWAY"):
                render_attempt(*objects(), attempt=0, active_nodes=("node-a", "node-b"), runtime_service_account="runtime")


if __name__ == "__main__":
    unittest.main()
