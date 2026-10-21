"""Pinned Databricks SDK Jobs routes backed by the local worker service."""

from __future__ import annotations

from datetime import datetime
from http import HTTPStatus
from urllib.parse import parse_qs, urlsplit

from brickhouse.api.catalog import ApiResponse
from brickhouse.jobs.execution_context import ExecutionContextResolutionError
from brickhouse.jobs.models import (
    InvalidJobDefinitionError,
    JobRecord,
    JobRunRecord,
    JobRunState,
    JobTaskAttemptOutput,
    JobTaskRunRecord,
    JobTaskState,
)
from brickhouse.jobs.repository import (
    JobNotFoundError,
    JobRepository,
    JobRepositoryError,
    JobRunNotFoundError,
    SubmissionTokenConflictError,
)
from brickhouse.jobs.service import (
    JobAdmissionError,
    JobExecutionService,
    UnsupportedJobRequestError,
)
from brickhouse.jobs.terraform import (
    TerraformJobNotFoundError,
    TerraformJobRegistry,
    TerraformJobRegistryError,
)
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.files import WorkspaceFileError, WorkspaceFileNotFoundError

_PREFIX = "/api/2.2/jobs/"
_TERRAFORM_PREFIX = "/api/2.0/jobs/"


class JobsHttpApi:
    """Expose the local Python DAG subset using pinned SDK wire shapes."""

    def __init__(self, context: WorkspaceContext) -> None:
        self._execution = JobExecutionService(context)
        self._repository: JobRepository = context.jobs
        self._terraform_jobs = TerraformJobRegistry(context)

    def close(self) -> None:
        """Cancel and reap active worker processes before the workspace is released."""
        self._execution.close()

    def dispatch(
        self,
        method: str,
        target: str,
        body: dict[str, object] | None = None,
    ) -> ApiResponse | None:
        parsed = urlsplit(target)
        if parsed.path.startswith(_TERRAFORM_PREFIX):
            return self._dispatch_terraform(
                method,
                parsed.path[len(_TERRAFORM_PREFIX) :],
                parsed.query,
                body,
            )
        if not parsed.path.startswith(_PREFIX):
            return None
        route = parsed.path[len(_PREFIX) :]
        query = parse_qs(parsed.query, keep_blank_values=True)
        try:
            if route == "create":
                if method != "POST":
                    return _method_not_allowed("POST")
                _validate_query(query, set())
                if not isinstance(body, dict):
                    raise _InputError("request body must be a JSON object")
                job = self._execution.create_job(body)
                return _ok({"job_id": job.job_id})
            if route == "get":
                if method != "GET":
                    return _method_not_allowed("GET")
                _validate_query(query, {"job_id"})
                job_id = _required_int(query, "job_id")
                resolved_job_id = self._terraform_jobs.resolve_execution_id(job_id)
                if resolved_job_id is not None:
                    self._repository.get_active_job(job_id)
                else:
                    resolved_job_id = job_id
                return _ok(_job_payload(self._repository.get_active_job(resolved_job_id)))
            if route == "run-now":
                if method != "POST":
                    return _method_not_allowed("POST")
                _validate_query(query, set())
                if not isinstance(body, dict):
                    raise _InputError("request body must be a JSON object")
                run_request = dict(body)
                job_id = _required_body_int(run_request, "job_id")
                resolved_job_id = self._terraform_jobs.resolve_execution_id(job_id)
                if resolved_job_id is not None:
                    self._repository.get_active_job(job_id)
                    run_request["job_id"] = resolved_job_id
                run, _ = self._execution.submit_run(run_request)
                number_in_job = len(self._repository.list_runs(run.job_id))
                return _ok({"run_id": run.run_id, "number_in_job": number_in_job})
            if route == "delete":
                if method != "POST":
                    return _method_not_allowed("POST")
                _validate_query(query, set())
                data = _require_object_body(body)
                if set(data) != {"job_id"}:
                    raise _InputError("job delete requires only job_id")
                self._delete_job(_required_body_int(data, "job_id"))
                return _ok({})
            if route == "runs/get":
                if method != "GET":
                    return _method_not_allowed("GET")
                _validate_query(
                    query,
                    {"run_id", "include_history", "include_resolved_values", "page_token"},
                )
                if _optional_bool(query, "include_history"):
                    raise UnsupportedJobRequestError("run attempt history is not implemented")
                if _optional_bool(query, "include_resolved_values"):
                    raise UnsupportedJobRequestError("resolved task values are not implemented")
                if _optional_single(query, "page_token") not in (None, ""):
                    raise UnsupportedJobRequestError("multi-task run pagination is not implemented")
                run_id = _required_int(query, "run_id")
                run, tasks = self._execution.get_run(run_id)
                return _ok(self._run_payload(run, tasks))
            if route == "runs/get-output":
                if method != "GET":
                    return _method_not_allowed("GET")
                _validate_query(query, {"run_id"})
                run_id = _required_int(query, "run_id")
                run, output = self._execution.get_run_output(run_id)
                if run.state in {JobRunState.QUEUED, JobRunState.RUNNING}:
                    return _error(
                        HTTPStatus.CONFLICT,
                        "INVALID_STATE",
                        "run output is available after the local worker has finished",
                    )
                return _ok(_output_payload(self._run_payload(run, ()), output))
            return _error(
                HTTPStatus.NOT_FOUND,
                "RESOURCE_DOES_NOT_EXIST",
                "Jobs API route not found",
            )
        except (
            JobNotFoundError,
            JobRunNotFoundError,
            TerraformJobNotFoundError,
            WorkspaceFileNotFoundError,
        ) as error:
            return _error(HTTPStatus.NOT_FOUND, "RESOURCE_DOES_NOT_EXIST", str(error))
        except SubmissionTokenConflictError as error:
            return _error(HTTPStatus.CONFLICT, "RESOURCE_CONFLICT", str(error))
        except JobAdmissionError as error:
            return _error(HTTPStatus.CONFLICT, "RESOURCE_CONFLICT", str(error))
        except (
            UnsupportedJobRequestError,
            InvalidJobDefinitionError,
            ExecutionContextResolutionError,
            WorkspaceFileError,
            _InputError,
        ) as error:
            return _error(HTTPStatus.BAD_REQUEST, "INVALID_PARAMETER_VALUE", str(error))
        except (JobRepositoryError, TerraformJobRegistryError):
            return _error(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "job metadata operation failed",
            )
        except Exception:
            return _error(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "local job request failed",
            )

    def _dispatch_terraform(
        self,
        method: str,
        route: str,
        query_string: str,
        body: dict[str, object] | None,
    ) -> ApiResponse:
        query = parse_qs(query_string, keep_blank_values=True)
        try:
            if route == "create":
                if method != "POST":
                    return _method_not_allowed("POST")
                _validate_query(query, set())
                settings = _required_settings_body(body)
                definition = self._execution.create_job(_terraform_job_payload(settings))
                resource = self._terraform_jobs.create(definition.job_id, settings)
                return _ok({"job_id": resource.job_id})
            if route == "get":
                if method != "GET":
                    return _method_not_allowed("GET")
                _validate_query(query, {"job_id"})
                job_id = _required_int(query, "job_id")
                resource = self._terraform_jobs.get(job_id)
                return _ok({"job_id": job_id, "settings": resource.settings})
            if route == "reset":
                if method != "POST":
                    return _method_not_allowed("POST")
                _validate_query(query, set())
                data = _require_object_body(body)
                if set(data) != {"job_id", "new_settings"}:
                    raise _InputError("job reset requires job_id and new_settings")
                job_id = _required_body_int(data, "job_id")
                self._terraform_jobs.get(job_id)
                settings = _required_settings(data.get("new_settings"))
                definition = self._execution.create_job(_terraform_job_payload(settings))
                self._terraform_jobs.advance(job_id, definition.job_id, settings)
                return _ok({})
            if route == "delete":
                if method != "POST":
                    return _method_not_allowed("POST")
                _validate_query(query, set())
                data = _require_object_body(body)
                if set(data) != {"job_id"}:
                    raise _InputError("job delete requires only job_id")
                job_id = _required_body_int(data, "job_id")
                definition_job_ids = self._terraform_jobs.definition_job_ids(job_id)
                if definition_job_ids is None:
                    raise TerraformJobNotFoundError(f"job {job_id} does not exist")
                self._execution.delete_job(
                    job_id,
                    related_job_ids=definition_job_ids,
                    on_delete=lambda: self._terraform_jobs.delete(job_id),
                )
                return _ok({})
            return _error(
                HTTPStatus.NOT_FOUND,
                "RESOURCE_DOES_NOT_EXIST",
                "Jobs API route not found",
            )
        except (JobNotFoundError, TerraformJobNotFoundError, JobRunNotFoundError) as error:
            return _error(HTTPStatus.NOT_FOUND, "RESOURCE_DOES_NOT_EXIST", str(error))
        except (
            UnsupportedJobRequestError,
            InvalidJobDefinitionError,
            ExecutionContextResolutionError,
            WorkspaceFileError,
            _InputError,
        ) as error:
            return _error(HTTPStatus.BAD_REQUEST, "INVALID_PARAMETER_VALUE", str(error))
        except (JobRepositoryError, TerraformJobRegistryError):
            return _error(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "job metadata operation failed",
            )
        except Exception:
            return _error(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "local job request failed",
            )

    def _delete_job(self, job_id: int) -> None:
        """Delete an SDK job or consistently tombstone a Terraform-backed identity."""
        definition_job_ids = self._terraform_jobs.definition_job_ids(job_id)
        if definition_job_ids is None:
            self._execution.delete_job(job_id)
            return
        self._execution.delete_job(
            job_id,
            related_job_ids=definition_job_ids,
            on_delete=lambda: self._terraform_jobs.delete(job_id),
        )

    def _run_payload(
        self,
        run: JobRunRecord,
        tasks: tuple[JobTaskRunRecord, ...],
    ) -> dict[str, object]:
        job = self._repository.get_job(run.job_id)
        number_in_job = next(
            index
            for index, item in enumerate(self._repository.list_runs(run.job_id), start=1)
            if item.run_id == run.run_id
        )
        life_cycle_state, result_state, state_message = _run_state(run)
        task_payloads = []
        for task in tasks:
            task_lifecycle, task_result = _task_state(task.state, task.error_code)
            state: dict[str, object] = {
                "life_cycle_state": task_lifecycle,
                "state_message": task.error_code or task.state.value,
            }
            if task_result is not None:
                state["result_state"] = task_result
            task_payload: dict[str, object] = {
                "task_key": task.task_key,
                "run_id": run.run_id,
                "state": state,
            }
            if task.attempt_count > 0:
                task_payload["attempt_number"] = task.attempt_count - 1
            description = task.settings.get("description")
            if isinstance(description, str):
                task_payload["description"] = description
            start_time = _epoch_millis(task.started_at)
            end_time = _epoch_millis(task.finished_at)
            if start_time is not None:
                task_payload["start_time"] = start_time
            if end_time is not None:
                task_payload["end_time"] = end_time
            task_payloads.append(task_payload)
        run_state: dict[str, object] = {
            "life_cycle_state": life_cycle_state,
            "state_message": state_message,
        }
        if result_state is not None:
            run_state["result_state"] = result_state
        payload: dict[str, object] = {
            "run_id": run.run_id,
            "job_id": run.job_id,
            "run_name": job.name,
            "number_in_job": number_in_job,
            "attempt_number": 0,
            "run_type": "JOB_RUN",
            "state": run_state,
            "tasks": task_payloads,
        }
        description = job.settings.get("description")
        if isinstance(description, str):
            payload["description"] = description
        start_time = _epoch_millis(run.started_at)
        end_time = _epoch_millis(run.finished_at)
        if start_time is not None:
            payload["start_time"] = start_time
        if end_time is not None:
            payload["end_time"] = end_time
        return payload


def _output_payload(
    metadata: dict[str, object], output: JobTaskAttemptOutput | None
) -> dict[str, object]:
    payload: dict[str, object] = {"metadata": metadata}
    if output is None:
        payload["logs"] = ""
        payload["logs_truncated"] = False
        return payload
    payload["logs"] = output.stdout
    payload["logs_truncated"] = output.stdout_truncated or output.stderr_truncated
    if output.stderr:
        payload["error"] = output.stderr
    return payload


def _job_payload(job: JobRecord) -> dict[str, object]:
    """Project the stored Python job subset back to the pinned Jobs API shape."""
    settings: dict[str, object] = {"name": job.name}
    for key, value in job.settings.items():
        settings[key] = value
    job_clusters: dict[str, dict[str, object]] = {}
    task_payloads: list[dict[str, object]] = []
    for task in job.tasks:
        execution = task.settings.get("execution_context")
        if not isinstance(execution, dict):
            raise JobRepositoryError("persisted task has no execution context")
        task_arguments = execution.get("task_arguments")
        if not isinstance(task_arguments, dict):
            raise JobRepositoryError("persisted task arguments are invalid")
        task_payload: dict[str, object] = {"task_key": task.task_key}
        if task.depends_on:
            task_payload["depends_on"] = [{"task_key": key} for key in task.depends_on]
        task_type = execution.get("task_type")
        if task_type == "python_script":
            task_payload["spark_python_task"] = dict(task_arguments)
        elif task_type == "notebook":
            task_payload["notebook_task"] = dict(task_arguments)
        else:
            raise UnsupportedJobRequestError("stored job task type is unsupported")

        compute_kind = execution.get("compute_resource_kind")
        if compute_kind == "all_purpose":
            compute_id = execution.get("compute_id")
            if not isinstance(compute_id, str):
                raise JobRepositoryError("persisted existing-cluster task has no identity")
            task_payload["existing_cluster_id"] = compute_id
        elif compute_kind in {"job_new", "job_shared"}:
            cluster_configuration = execution.get("cluster_configuration")
            compute_key = execution.get("compute_key")
            if not isinstance(cluster_configuration, dict) or not isinstance(compute_key, str):
                raise JobRepositoryError("persisted job-cluster selector is invalid")
            if compute_kind == "job_shared":
                task_payload["job_cluster_key"] = compute_key
                job_clusters[compute_key] = {
                    "job_cluster_key": compute_key,
                    "new_cluster": _cluster_payload(cluster_configuration),
                }
            else:
                task_payload["new_cluster"] = _cluster_payload(cluster_configuration)
        elif compute_kind is None and execution.get("compute_mode") == "serverless":
            environment_key = execution.get("environment_key")
            if not isinstance(environment_key, str):
                raise JobRepositoryError("persisted serverless task has no environment selector")
            task_payload["environment_key"] = environment_key
            task_payload["disable_auto_optimization"] = True
        else:
            raise JobRepositoryError("persisted task compute selector is invalid")

        retry_settings = execution.get("retry_settings")
        if isinstance(retry_settings, dict):
            for key in (
                "max_retries",
                "min_retry_interval_millis",
                "retry_on_timeout",
                "timeout_seconds",
            ):
                value = retry_settings.get(key)
                if value is not None:
                    task_payload[key] = value
        timeout_seconds = execution.get("timeout_seconds")
        if timeout_seconds is not None:
            task_payload["timeout_seconds"] = timeout_seconds
        description = task.settings.get("description")
        if isinstance(description, str):
            task_payload["description"] = description
        task_payloads.append(task_payload)
    settings["tasks"] = task_payloads
    if job_clusters:
        settings["job_clusters"] = list(job_clusters.values())
    return {"job_id": job.job_id, "settings": settings}


def _cluster_payload(configuration: dict[str, object]) -> dict[str, object]:
    """Flatten a stored simulated cluster configuration into a Jobs API request shape."""
    payload = {
        key: value
        for key, value in configuration.items()
        if value is not None and key != "settings"
    }
    cluster_settings = configuration.get("settings", {})
    if not isinstance(cluster_settings, dict):
        raise JobRepositoryError("persisted cluster settings are invalid")
    payload.update(cluster_settings)
    return payload


def _run_state(run: JobRunRecord) -> tuple[str, str | None, str]:
    if run.state is JobRunState.QUEUED:
        return "QUEUED", None, "Run is queued by the local worker service."
    if run.state is JobRunState.RUNNING:
        return "RUNNING", None, "Run is executing in a local worker process."
    if run.state is JobRunState.SUCCEEDED:
        return "TERMINATED", "SUCCESS", "Run completed successfully."
    if run.state is JobRunState.FAILED:
        return "TERMINATED", "FAILED", run.error_code or "Run failed in the local worker."
    if run.state is JobRunState.CANCELLED:
        return "TERMINATED", "CANCELED", "Run was cancelled during local shutdown."
    return "INTERNAL_ERROR", None, run.error_code or "Run was interrupted; outcome is unknown."


def _task_state(state: JobTaskState, error_code: str | None = None) -> tuple[str, str | None]:
    if state in {JobTaskState.PENDING, JobTaskState.QUEUED}:
        return "PENDING", None
    if state is JobTaskState.RUNNING:
        return "RUNNING", None
    if state is JobTaskState.SUCCEEDED:
        return "TERMINATED", "SUCCESS"
    if state is JobTaskState.CANCELLED:
        return "TERMINATED", "CANCELED"
    if state is JobTaskState.SKIPPED:
        if error_code == "DEPENDENCY_FAILED":
            return "SKIPPED", "UPSTREAM_FAILED"
        if error_code == "RUN_CANCELLED":
            return "SKIPPED", "UPSTREAM_CANCELED"
        return "SKIPPED", "EXCLUDED"
    if state is JobTaskState.INTERRUPTED:
        return "INTERNAL_ERROR", None
    return "TERMINATED", "FAILED"


def _validate_query(query: dict[str, list[str]], allowed: set[str]) -> None:
    unknown = set(query) - allowed
    if unknown:
        raise _InputError(f"unsupported query parameter(s): {', '.join(sorted(unknown))}")
    for key, values in query.items():
        if len(values) != 1:
            raise _InputError(f"query parameter {key} cannot be repeated")


def _optional_single(query: dict[str, list[str]], key: str) -> str | None:
    values = query.get(key)
    if values is None:
        return None
    if len(values) != 1:
        raise _InputError(f"query parameter {key} cannot be repeated")
    return values[0]


def _required_int(query: dict[str, list[str]], key: str) -> int:
    value = _optional_single(query, key)
    if value is None:
        raise _InputError(f"query parameter {key} is required")
    try:
        result = int(value)
    except ValueError as error:
        raise _InputError(f"query parameter {key} must be an integer") from error
    if result < 1:
        raise _InputError(f"query parameter {key} must be positive")
    return result


def _required_body_int(body: dict[str, object], key: str) -> int:
    value = body.get(key)
    if type(value) is not int or value < 1:
        raise _InputError(f"{key} must be a positive integer")
    return value


def _require_object_body(body: dict[str, object] | None) -> dict[str, object]:
    if body is None:
        raise _InputError("request body must be a JSON object")
    return body


def _required_settings_body(body: dict[str, object] | None) -> dict[str, object]:
    return _required_settings(_require_object_body(body))


def _required_settings(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise _InputError("job settings must be a JSON object")
    return value


def _terraform_job_payload(settings: dict[str, object]) -> dict[str, object]:
    allowed = {
        "description",
        "existing_cluster_id",
        "max_concurrent_runs",
        "max_retries",
        "min_retry_interval_millis",
        "name",
        "retry_on_timeout",
        "spark_python_task",
        "timeout_seconds",
    }
    unknown = set(settings) - allowed
    if unknown:
        raise UnsupportedJobRequestError(
            "the Terraform job profile does not implement setting(s): " + ", ".join(sorted(unknown))
        )
    task_fields = {
        "existing_cluster_id",
        "max_retries",
        "min_retry_interval_millis",
        "retry_on_timeout",
        "spark_python_task",
        "timeout_seconds",
    }
    task: dict[str, object] = {key: settings[key] for key in task_fields if key in settings}
    if "existing_cluster_id" not in task:
        raise UnsupportedJobRequestError(
            "the Terraform job profile requires an existing local cluster"
        )
    if "spark_python_task" not in task:
        raise UnsupportedJobRequestError(
            "the Terraform job profile supports Spark Python script tasks only"
        )
    payload: dict[str, object] = {
        "name": settings.get("name"),
        "tasks": [{"task_key": "terraform_task", **task}],
    }
    for key in ("description", "max_concurrent_runs"):
        if key in settings:
            payload[key] = settings[key]
    return payload


def _optional_bool(query: dict[str, list[str]], key: str) -> bool | None:
    value = _optional_single(query, key)
    if value is None:
        return None
    if value.casefold() not in {"true", "false"}:
        raise _InputError(f"query parameter {key} must be true or false")
    return value.casefold() == "true"


def _epoch_millis(value: str | None) -> int | None:
    if value is None:
        return None
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def _ok(payload: object) -> ApiResponse:
    return ApiResponse(HTTPStatus.OK, payload)


def _error(status: HTTPStatus, error_code: str, message: str) -> ApiResponse:
    return ApiResponse(status, {"error_code": error_code, "message": message})


def _method_not_allowed(allowed: str) -> ApiResponse:
    return ApiResponse(
        HTTPStatus.METHOD_NOT_ALLOWED,
        {
            "error_code": "METHOD_NOT_ALLOWED",
            "message": "method not allowed",
            "allowed_methods": allowed,
        },
    )


class _InputError(ValueError):
    """A malformed or unsupported SDK request field."""
