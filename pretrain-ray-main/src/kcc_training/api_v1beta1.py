"""Validated Kubernetes API models for the deployable controller."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping, Sequence

from .recovery import RecoveryPolicyError, maximum_attempt_count


API_VERSION = "training.kcc.io/v1beta1"
_DNS = re.compile(r"^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$")
_IMAGE = re.compile(r"^\S+@sha256:[0-9a-f]{64}$")
_DNS_SUBDOMAIN = re.compile(r"^[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?$")
_ARTIFACT_URI = re.compile(r"^artifact://[^/?#\s]+/[^/?#\s]+/[^/?#\s]+$")
_ENV = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ApiValidationError(ValueError):
    pass


def mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ApiValidationError(f"{label} must be an object")
    return value


def exact(value: Mapping[str, Any], required: set[str], label: str, optional: set[str] | None = None) -> None:
    missing = required - set(value)
    unknown = set(value) - required - (optional or set())
    if missing:
        raise ApiValidationError(f"{label} is missing: {', '.join(sorted(missing))}")
    if unknown:
        raise ApiValidationError(f"{label} has unsupported keys: {', '.join(sorted(unknown))}")


def text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ApiValidationError(f"{label} must be a non-empty trimmed string")
    return value


def dns(value: Any, label: str) -> str:
    result = text(value, label)
    if len(result) > 63 or _DNS.fullmatch(result) is None:
        raise ApiValidationError(f"{label} must be a Kubernetes DNS label")
    return result


def positive(value: Any, label: str, *, minimum: int = 1, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ApiValidationError(f"{label} must be an integer >= {minimum}")
    if maximum is not None and value > maximum:
        raise ApiValidationError(f"{label} must be <= {maximum}")
    return value


def image(value: Any, label: str) -> str:
    result = text(value, label)
    if _IMAGE.fullmatch(result) is None:
        raise ApiValidationError(f"{label} must use @sha256 digest pinning")
    return result



def artifact_uri(value: Any, label: str) -> str:
    result = text(value, label)
    if _ARTIFACT_URI.fullmatch(result) is None:
        raise ApiValidationError(f"{label} must be artifact://namespace/name/version")
    return result
def string_map(value: Any, label: str) -> dict[str, str]:
    source = mapping(value, label)
    result: dict[str, str] = {}
    for key, item in source.items():
        if not isinstance(key, str) or not key or not isinstance(item, str):
            raise ApiValidationError(f"{label} must contain string keys and values")
        result[key] = item
    return result


def _string_list(value: Any, label: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ApiValidationError(f"{label} must be a list")
    result = tuple(text(item, f"{label} item") for item in value)
    if any(len(item) > 253 or _DNS_SUBDOMAIN.fullmatch(item) is None for item in result):
        raise ApiValidationError(f"{label} items must be Kubernetes DNS subdomain names")
    if len(result) != len(set(result)):
        raise ApiValidationError(f"{label} must not contain duplicates")
    return result


def _tolerations(value: Any, label: str) -> tuple[Mapping[str, Any], ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or not all(isinstance(item, Mapping) for item in value):
        raise ApiValidationError(f"{label} must be a list of objects")
    return tuple(dict(item) for item in value)


def _quantity_map(value: Any, label: str) -> dict[str, str | int]:
    source = mapping(value, label)
    result: dict[str, str | int] = {}
    for key, item in source.items():
        if not isinstance(key, str) or not key:
            raise ApiValidationError(f"{label} has an invalid resource name")
        if isinstance(item, bool) or not isinstance(item, (str, int)) or item == "":
            raise ApiValidationError(f"{label}.{key} must be a string or integer quantity")
        result[key] = item
    return result


def _resources(
    value: Any,
    label: str,
    default: Mapping[str, Mapping[str, str | int]],
) -> dict[str, dict[str, str | int]]:
    if value is None:
        return {name: dict(quantities) for name, quantities in default.items()}
    source = mapping(value, label)
    exact(source, {"requests", "limits"}, label)
    return {
        "requests": _quantity_map(source["requests"], f"{label}.requests"),
        "limits": _quantity_map(source["limits"], f"{label}.limits"),
    }


def metadata(document: Mapping[str, Any], kind: str) -> tuple[str, str, str, str, int]:
    if document.get("apiVersion") != API_VERSION or document.get("kind") != kind:
        raise ApiValidationError(f"object must be {API_VERSION} {kind}")
    meta = mapping(document.get("metadata"), "metadata")
    name = dns(meta.get("name"), "metadata.name")
    namespace = dns(meta.get("namespace"), "metadata.namespace")
    uid = text(meta.get("uid"), "metadata.uid")
    resource_version = text(meta.get("resourceVersion"), "metadata.resourceVersion")
    generation = positive(meta.get("generation", 1), "metadata.generation")
    return name, namespace, uid, resource_version, generation


@dataclass(frozen=True)
class ObjectIdentity:
    name: str
    namespace: str
    uid: str
    resource_version: str
    generation: int


@dataclass(frozen=True)
class RuntimeProfile:
    identity: ObjectIdentity
    head_image: str
    worker_image: str
    ray_version: str
    resource_name: str
    devices_per_node: int
    runtime_class_name: str | None
    workspace_claim: str
    workspace_mount_path: str
    active_nodes: tuple[str, ...]
    spare_nodes: tuple[str, ...]
    head_selector: Mapping[str, str]
    worker_selector: Mapping[str, str]
    ranktable_provider: str
    health_provider: str
    artifact_provider: str
    image_pull_secrets: tuple[str, ...]
    head_resources: Mapping[str, Mapping[str, str | int]]
    worker_resources: Mapping[str, Mapping[str, str | int]]
    head_tolerations: tuple[Mapping[str, Any], ...]
    worker_tolerations: tuple[Mapping[str, Any], ...]
    head_priority_class_name: str | None
    worker_priority_class_name: str | None
    head_ray_cpus: int
    worker_ray_cpus: int
    physical_device_ids: tuple[int, ...] = ()

    @classmethod
    def from_resource(cls, document: Mapping[str, Any]) -> "RuntimeProfile":
        identity = ObjectIdentity(*metadata(document, "TrainingRuntimeProfile"))
        spec = mapping(document.get("spec"), "spec")
        exact(
            spec,
            {"images", "rayVersion", "accelerator", "workspace", "scheduling", "integrations"},
            "spec",
            optional={"podTemplate"},
        )
        images = mapping(spec["images"], "spec.images")
        exact(images, {"head", "worker"}, "spec.images", optional={"pullSecrets"})
        accelerator = mapping(spec["accelerator"], "spec.accelerator")
        exact(
            accelerator,
            {"resourceName", "devicesPerNode"},
            "spec.accelerator",
            optional={"runtimeClassName", "physicalDeviceIDs"},
        )
        workspace = mapping(spec["workspace"], "spec.workspace")
        exact(workspace, {"claimName", "mountPath"}, "spec.workspace")
        scheduling = mapping(spec["scheduling"], "spec.scheduling")
        exact(scheduling, {"activeNodes", "spareNodes", "headSelector", "workerSelector"}, "spec.scheduling")
        integrations = mapping(spec["integrations"], "spec.integrations")
        exact(
            integrations,
            {"rankTableProvider", "healthProvider"},
            "spec.integrations",
            optional={"artifactProvider"},
        )
        active = tuple(text(item, "activeNodes item") for item in _list(scheduling["activeNodes"], "activeNodes"))
        raw_spares = scheduling["spareNodes"]
        if not isinstance(raw_spares, list):
            raise ApiValidationError("spareNodes must be a list")
        spare = tuple(text(item, "spareNodes item") for item in raw_spares)
        if len(active) != len(set(active)) or len(spare) != len(set(spare)) or set(active) & set(spare):
            raise ApiValidationError("activeNodes and spareNodes must be unique and disjoint")
        mount_path = text(workspace["mountPath"], "workspace.mountPath")
        if not mount_path.startswith("/"):
            raise ApiValidationError("workspace.mountPath must be absolute")
        rank = text(integrations["rankTableProvider"], "rankTableProvider")
        health = text(integrations["healthProvider"], "healthProvider")
        artifact = text(integrations.get("artifactProvider", "gateway"), "artifactProvider")
        if rank != "clusterd":
            raise ApiValidationError("rankTableProvider must be clusterd")
        if health not in {"npu-exporter", "kubernetes"}:
            raise ApiValidationError("unsupported healthProvider")
        if artifact not in {"gateway", "workspace"}:
            raise ApiValidationError("artifactProvider must be gateway or workspace")
        devices_per_node = positive(
            accelerator["devicesPerNode"], "devicesPerNode", maximum=64
        )
        raw_physical_device_ids = accelerator.get("physicalDeviceIDs", [])
        if not isinstance(raw_physical_device_ids, list) or any(
            isinstance(item, bool) or not isinstance(item, int) or not 0 <= item <= 63
            for item in raw_physical_device_ids
        ):
            raise ApiValidationError("physicalDeviceIDs must contain integers from 0 to 63")
        physical_device_ids = tuple(raw_physical_device_ids)
        if len(physical_device_ids) != len(set(physical_device_ids)):
            raise ApiValidationError("physicalDeviceIDs must be unique")
        if physical_device_ids and len(physical_device_ids) != devices_per_node:
            raise ApiValidationError("physicalDeviceIDs must match devicesPerNode")
        resource_name = text(accelerator["resourceName"], "resourceName")
        if physical_device_ids and resource_name != "huawei.com/Ascend910":
            raise ApiValidationError(
                "physicalDeviceIDs currently requires resourceName huawei.com/Ascend910"
            )
        pod_template = mapping(spec.get("podTemplate", {}), "spec.podTemplate")
        exact(pod_template, set(), "spec.podTemplate", optional={"head", "worker"})
        head = mapping(pod_template.get("head", {}), "spec.podTemplate.head")
        worker = mapping(pod_template.get("worker", {}), "spec.podTemplate.worker")
        pod_keys = {"resources", "tolerations", "priorityClassName", "rayCpus"}
        exact(head, set(), "spec.podTemplate.head", optional=pod_keys)
        exact(worker, set(), "spec.podTemplate.worker", optional=pod_keys)
        head_priority = head.get("priorityClassName")
        worker_priority = worker.get("priorityClassName")
        return cls(
            identity=identity,
            head_image=image(images["head"], "images.head"),
            worker_image=image(images["worker"], "images.worker"),
            ray_version=text(spec["rayVersion"], "rayVersion"),
            resource_name=resource_name,
            devices_per_node=devices_per_node,
            runtime_class_name=(
                dns(accelerator["runtimeClassName"], "runtimeClassName")
                if accelerator.get("runtimeClassName")
                else None
            ),
            workspace_claim=dns(workspace["claimName"], "workspace.claimName"),
            workspace_mount_path=mount_path,
            active_nodes=active,
            spare_nodes=spare,
            head_selector=string_map(scheduling["headSelector"], "headSelector"),
            worker_selector=string_map(scheduling["workerSelector"], "workerSelector"),
            ranktable_provider=rank,
            health_provider=health,
            artifact_provider=artifact,
            image_pull_secrets=_string_list(images.get("pullSecrets"), "images.pullSecrets"),
            head_resources=_resources(
                head.get("resources"),
                "podTemplate.head.resources",
                {"requests": {"cpu": "2", "memory": "4Gi"}, "limits": {"cpu": "8", "memory": "16Gi"}},
            ),
            worker_resources=_resources(
                worker.get("resources"),
                "podTemplate.worker.resources",
                {
                    "requests": {"cpu": "32", "memory": "128Gi"},
                    "limits": {"cpu": "128", "memory": "512Gi"},
                },
            ),
            head_tolerations=_tolerations(head.get("tolerations"), "podTemplate.head.tolerations"),
            worker_tolerations=_tolerations(worker.get("tolerations"), "podTemplate.worker.tolerations"),
            head_priority_class_name=(text(head_priority, "podTemplate.head.priorityClassName") if head_priority else None),
            worker_priority_class_name=(
                text(worker_priority, "podTemplate.worker.priorityClassName") if worker_priority else None
            ),
            head_ray_cpus=positive(head.get("rayCpus", 2), "podTemplate.head.rayCpus"),
            worker_ray_cpus=positive(worker.get("rayCpus", 32), "podTemplate.worker.rayCpus"),
            physical_device_ids=physical_device_ids,
        )


def _list(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list) or not value:
        raise ApiValidationError(f"{label} must be a non-empty list")
    return value


@dataclass(frozen=True)
class Recipe:
    identity: ObjectIdentity
    framework: str
    command: tuple[str, ...]
    working_directory: str
    environment: Mapping[str, str]
    source_uri: str
    model_uri: str
    data_uri: str
    output_subpath: str

    @classmethod
    def from_resource(cls, document: Mapping[str, Any]) -> "Recipe":
        identity = ObjectIdentity(*metadata(document, "TrainingRecipe"))
        spec = mapping(document.get("spec"), "spec")
        exact(spec, {"framework", "command", "workingDirectory", "environment", "artifacts"}, "spec")
        command = tuple(text(item, "command item") for item in _list(spec["command"], "command"))
        environment = string_map(spec["environment"], "environment")
        if any(_ENV.fullmatch(key) is None for key in environment):
            raise ApiValidationError("environment contains an invalid variable name")
        blocked = {
            "RANK_TABLE_FILE", "MASTER_ADDR", "MASTER_PORT", "NODE_RANK", "WORLD_SIZE", "KUBECONFIG",
            "KCC_SOURCE_DIR", "KCC_MODEL_DIR", "KCC_DATA_DIR", "KCC_OUTPUT_ROOT", "KCC_CHECKPOINT_ROOT",
        }
        if blocked & set(environment):
            raise ApiValidationError("environment overrides controller-owned variables")
        artifacts = mapping(spec["artifacts"], "spec.artifacts")
        exact(artifacts, {"source", "model", "data", "outputSubpath"}, "spec.artifacts")
        cwd = text(spec["workingDirectory"], "workingDirectory")
        output = text(artifacts["outputSubpath"], "outputSubpath")
        if output.startswith("/") or ".." in output.split("/"):
            raise ApiValidationError("outputSubpath is unsafe")
        if cwd.startswith("/") or ".." in cwd.split("/"):
            raise ApiValidationError("workingDirectory must be a safe relative path")
        return cls(
            identity=identity,
            framework=text(spec["framework"], "framework"),
            command=command,
            working_directory=cwd,
            environment=environment,
            source_uri=artifact_uri(artifacts["source"], "artifacts.source"),
            model_uri=artifact_uri(artifacts["model"], "artifacts.model"),
            data_uri=artifact_uri(artifacts["data"], "artifacts.data"),
            output_subpath=output,
        )


@dataclass(frozen=True)
class Run:
    identity: ObjectIdentity
    profile_name: str
    recipe_name: str
    workers: int
    same_topology_retries: int
    max_replacements: int
    no_progress_seconds: int
    suspended: bool
    suspend_mode: str

    @classmethod
    def from_resource(cls, document: Mapping[str, Any]) -> "Run":
        identity = ObjectIdentity(*metadata(document, "TrainingRun"))
        spec = mapping(document.get("spec"), "spec")
        exact(
            spec,
            {"runtimeProfile", "recipe", "workers", "recovery"},
            "spec",
            optional={"suspend", "suspendMode"},
        )
        recovery = mapping(spec["recovery"], "spec.recovery")
        exact(recovery, {"sameTopologyRetries", "maxReplacements", "noProgressSeconds"}, "spec.recovery")
        suspended = spec.get("suspend", False)
        if not isinstance(suspended, bool):
            raise ApiValidationError("suspend must be boolean")
        suspend_mode = spec.get("suspendMode", "Immediate")
        if suspend_mode not in {"Immediate", "AfterCheckpoint"}:
            raise ApiValidationError("suspendMode must be Immediate or AfterCheckpoint")
        same_topology_retries = positive(
            recovery["sameTopologyRetries"], "sameTopologyRetries", minimum=0, maximum=10
        )
        max_replacements = positive(
            recovery["maxReplacements"], "maxReplacements", minimum=0, maximum=100
        )
        try:
            maximum_attempt_count(max_replacements, same_topology_retries)
        except RecoveryPolicyError as error:
            raise ApiValidationError(str(error)) from error
        return cls(
            identity=identity,
            profile_name=dns(spec["runtimeProfile"], "runtimeProfile"),
            recipe_name=dns(spec["recipe"], "recipe"),
            workers=positive(spec["workers"], "workers", maximum=1024),
            same_topology_retries=same_topology_retries,
            max_replacements=max_replacements,
            no_progress_seconds=positive(recovery["noProgressSeconds"], "noProgressSeconds", minimum=0),
            suspended=suspended,
            suspend_mode=suspend_mode,
        )

