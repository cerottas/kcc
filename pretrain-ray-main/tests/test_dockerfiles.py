from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class DockerfileTests(unittest.TestCase):
    def test_head_redeclares_kubectl_source_in_build_stage(self) -> None:
        dockerfile = (ROOT / "docker" / "Dockerfile.head").read_text()
        final_stage = dockerfile.split("FROM ${BASE_IMAGE}", 1)[1]
        self.assertIn("ARG KUBECTL_SOURCE=/bin/kubectl", final_stage)
        self.assertIn(
            "COPY --from=kubectl ${KUBECTL_SOURCE} /usr/local/bin/kubectl",
            final_stage,
        )


if __name__ == "__main__":
    unittest.main()
