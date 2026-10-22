"""Local SDK job definition and bounded Python worker execution composition."""

from __future__ import annotations

import json
import tempfile
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import cast

from brickhouse.compute.models import ComputeRecord, ComputeResourceKind, ComputeState
from brickhouse.jobs.execution_context import (
    ExecutionContextResolutionError,
    ExecutionContextResolver,
    ResolvedTaskContext,
    validate_notebook_parameters,
    validate_python_parameters,
)
from brickhouse.jobs.models import (
    InvalidJobDefinitionError,
    JobAttemptState,
    JobRecord,
    JobRunRecord,
    JobRunState,
    JobTaskAttemptOutput,
    JobTaskDefinition,
    JobTaskRunRecord,
    JobTaskState,
)
from brickhouse.jobs.repository import (
    JobRepository,
    JobRepositoryError,
)
from brickhouse.jobs.terraform import TerraformJobRegistry
from brickhouse.notebooks.source_format import (
    NotebookSourceFormatError,
    notebook_environment_spec,
    parse_databricks_python_source,
)
from brickhouse.runtime import (
    WorkerCallMessage,
    WorkerConfigurationError,
    WorkerContext,
    WorkerLaunchError,
    WorkerProtocolError,
)
from brickhouse.runtime.outcomes import WorkerAttemptOutcome, worker_process_exists
from brickhouse.runtime.secrets import SecretProviderError
from brickhouse.runtime.worker import run_python_worker
from brickhouse.runtime.worker_auth import MAX_WORKER_MESSAGE_BYTES
from brickhouse.runtime.worker_services import WorkerCallHandler, WorkerServiceError
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.files import (
    WorkspaceFileError,
    WorkspaceFileService,
)

MAX_ACTIVE_JOB_RUNS = 2
MAX_QUEUED_JOB_RUNS = 8
MAX_ADMITTED_JOB_RUNS = MAX_ACTIVE_JOB_RUNS + MAX_QUEUED_JOB_RUNS


class UnsupportedJobRequestError(ValueError):
    """A valid SDK job setting asks for an effect outside this implementation slice."""


class JobAdmissionError(RuntimeError):
    """The local job runner cannot admit another active run."""


ParameterOverride = list[str] | dict[str, str] | None


class JobExecutionService:
    """Resolve Python task DAGs and dispatch each task through a fresh child process."""

    def __init__(self, context: WorkspaceContext) -> None:
        self._context = context
        self._repository: JobRepository = context.jobs
        self._files = WorkspaceFileService(context)
        self._lock = threading.RLock()
        self._active: dict[int, tuple[int, threading.Thread, threading.Event]] = {}
        self._closed = False
        self._recover_abandoned_runs()
        with self._lock:
            self._schedule_queued_runs()

    def _recover_abandoned_runs(self) -> None:
        """Reconcile runs left active by an earlier service process."""
        with self._lock:
            for run in self._repository.list_active_runs():
                self._reconcile_run_if_abandoned(run.run_id)

    def _reconcile_run_if_abandoned(self, run_id: int) -> None:
        """Preserve observed worker outcomes and interrupt work with no live owner."""
        with self._lock:
            run = self._repository.get_run(run_id)
            if run.state not in {JobRunState.QUEUED, JobRunState.RUNNING}:
                return
            current = self._active.get(run_id)
            if current is not None and current[1].is_alive():
                return

            attempts = self._repository.list_attempts(run_id)
            if run.state is JobRunState.QUEUED and not attempts:
                if run.dispatched_at is not None:
                    self._context.compute.release_run_consumers(run_id)
                    self._repository.reset_unstarted_dispatch(run_id)
                return

            active_attempts = [
                attempt
                for attempt in self._repository.list_attempts(run_id)
                if attempt.state in {JobAttemptState.QUEUED, JobAttemptState.RUNNING}
            ]
            outcomes: dict[str, WorkerAttemptOutcome | None] = {}
            for attempt in active_attempts:
                outcome = self._context.worker_outcomes.get(str(run_id), attempt.attempt_id)
                outcomes[attempt.attempt_id] = outcome
                if outcome is not None and outcome.state == "running":
                    if worker_process_exists(outcome.worker_pid):
                        return

            for attempt in active_attempts:
                outcome = outcomes[attempt.attempt_id]
                if outcome is None or outcome.state == "running":
                    if outcome is not None:
                        self._context.worker_outcomes.interrupt_attempt(
                            str(run_id), attempt.attempt_id
                        )
                    continue
                if outcome.termination_reason == "interrupted":
                    continue
                self._repository.finish_attempt(
                    run_id,
                    attempt.task_key,
                    attempt.attempt_number,
                    exit_code=outcome.exit_code,
                    termination_reason=outcome.termination_reason,
                )

            self._repository.interrupt_run(run_id, error_code="WORKSPACE_RESTARTED")
            self._context.compute.release_run_consumers(run_id)

    def _active_persisted_runs(self) -> list[JobRunRecord]:
        return self._repository.list_active_runs()

    def create_job(self, body: Mapping[str, object]) -> JobRecord:
        """Validate supported SDK Python script tasks and store their immutable profile."""
        allowed = {
            "name",
            "description",
            "tasks",
            "environments",
            "job_clusters",
            "max_concurrent_runs",
            "performance_target",
            "deployment",
            "edit_mode",
            "format",
            "queue",
            "tags",
        }
        unknown = set(body) - allowed
        if unknown:
            raise UnsupportedJobRequestError(
                f"unsupported job field(s): {', '.join(sorted(unknown))}"
            )
        name = body.get("name")
        if not isinstance(name, str) or not name.strip() or len(name) > 256:
            raise InvalidJobDefinitionError("name must contain 1 to 256 characters")
        description = body.get("description")
        if description is not None and (
            not isinstance(description, str) or len(description) > 4000
        ):
            raise InvalidJobDefinitionError(
                "description must be a string of at most 4000 characters"
            )
        max_concurrent_runs = body.get("max_concurrent_runs", 1)
        if (
            type(max_concurrent_runs) is not int
            or not 1 <= max_concurrent_runs <= MAX_ADMITTED_JOB_RUNS
        ):
            raise UnsupportedJobRequestError(
                "max_concurrent_runs must be between 1 and the local process limit "
                f"of {MAX_ADMITTED_JOB_RUNS}"
            )
        tasks = _object_sequence(body.get("tasks"), "tasks", minimum=1)
        job_clusters = _object_sequence(body.get("job_clusters", []), "job_clusters")
        environments = _object_sequence(body.get("environments", []), "environments")
        notebook_environments: dict[str, Mapping[str, object]] = {}
        for task in tasks:
            notebook_task = task.get("notebook_task")
            if not isinstance(notebook_task, Mapping):
                continue
            notebook_path = notebook_task.get("notebook_path")
            if not isinstance(notebook_path, str):
                continue
            notebook = self._files.get_workspace_object(notebook_path)
            if notebook.object_type != "FILE" or notebook.size_bytes is None:
                raise InvalidJobDefinitionError("notebook_path must name a local notebook file")
            if notebook.local_path.suffix.casefold() not in {".ipynb", ".py"}:
                raise UnsupportedJobRequestError(
                    "local notebook tasks support .ipynb files and marked Python .py exports"
                )
            try:
                specification = notebook_environment_spec(notebook.local_path)
                if notebook.local_path.suffix.casefold() == ".py":
                    parse_databricks_python_source(notebook.local_path)
            except NotebookSourceFormatError as error:
                raise InvalidJobDefinitionError(str(error)) from error
            selectors = ("new_cluster", "existing_cluster_id", "job_cluster_key")
            if (
                task.get("environment_key") is None
                and not any(task.get(selector) is not None for selector in selectors)
                and specification is not None
            ):
                notebook_environments[str(task.get("task_key"))] = specification
        performance_target = body.get("performance_target")
        if performance_target is not None and not isinstance(performance_target, str):
            raise InvalidJobDefinitionError("performance_target must be a string")
        bundle_settings = _validated_bundle_settings(body)
        resolved = ExecutionContextResolver(self._context).resolve_job(
            tasks,
            job_clusters=job_clusters,
            environments=environments,
            notebook_environments=notebook_environments,
            performance_target=performance_target,
        )
        task_definitions: list[JobTaskDefinition] = []
        for task, task_context in zip(tasks, resolved, strict=True):
            if task_context.task_type not in {"python_script", "notebook"}:
                raise UnsupportedJobRequestError(
                    "the SDK job route supports only Python script and notebook tasks"
                )
            arguments = task_context.task_arguments
            file_key = (
                "python_file" if task_context.task_type == "python_script" else "notebook_path"
            )
            workspace_path = arguments.get(file_key)
            if not isinstance(workspace_path, str):
                raise InvalidJobDefinitionError(f"resolved task is missing {file_key}")
            if task_context.task_type == "python_script":
                self._files.get_python(workspace_path)
            else:
                notebook = self._files.get_workspace_object(workspace_path)
                if notebook.object_type != "FILE" or notebook.local_path.suffix.casefold() not in {
                    ".ipynb",
                    ".py",
                }:
                    raise InvalidJobDefinitionError("notebook_path must name a supported file")
            settings = _resolved_context_settings(task_context)
            task_description = task.get("description")
            if task_description is not None and (
                not isinstance(task_description, str) or len(task_description) > 4000
            ):
                raise InvalidJobDefinitionError(
                    "task description must be a string of at most 4000 characters"
                )
            if task_description is not None:
                settings["description"] = task_description
            _build_worker_context(settings, run_id=1, attempt_id="0" * 36)
            task_definitions.append(
                JobTaskDefinition(
                    task_context.task_key,
                    task_type=task_context.task_type,
                    depends_on=_dependencies(task.get("depends_on", [])),
                    settings=settings,
                )
            )
        job_settings: dict[str, object] = {"max_concurrent_runs": max_concurrent_runs}
        if description is not None:
            job_settings["description"] = description
        job_settings.update(bundle_settings)
        return self._repository.create_job(
            name,
            task_definitions,
            settings=job_settings,
        )

    def delete_job(
        self,
        job_id: int,
        *,
        related_job_ids: Sequence[int] = (),
        on_delete: Callable[[], None] | None = None,
    ) -> None:
        """Remove a job from new work while retaining immutable definitions and run history."""
        with self._lock:
            if self._closed:
                raise JobAdmissionError("the local job runner is shutting down")
            self._recover_abandoned_runs()
            self._repository.get_active_job(job_id)
            tracked_job_ids = {job_id, *related_job_ids}
            active_run_exists = any(
                run.state in {JobRunState.QUEUED, JobRunState.RUNNING}
                for tracked_job_id in tracked_job_ids
                for run in self._repository.list_runs(tracked_job_id)
            )
            if active_run_exists:
                raise JobAdmissionError("a job cannot be deleted while it has active runs")
            if on_delete is not None:
                on_delete()
            self._repository.delete_job(job_id)

    def promote_admission_identity(self, job_id: int, public_job_id: int) -> None:
        """Rekey all prior run history when reset creates a stable public job ID."""
        with self._lock:
            self._repository.rekey_admission_identity(
                f"job:{job_id}",
                f"terraform:{public_job_id}",
            )
            self._schedule_queued_runs()

    def submit_run(
        self,
        body: Mapping[str, object],
        *,
        admission_key: str | None = None,
    ) -> tuple[JobRunRecord, bool]:
        """Create one idempotent run and start its background worker if it is new."""
        allowed = {"job_id", "idempotency_token", "python_params", "notebook_params"}
        unknown = set(body) - allowed
        if unknown:
            raise UnsupportedJobRequestError(
                f"unsupported run-now field(s): {', '.join(sorted(unknown))}"
            )
        job_id = body.get("job_id")
        if type(job_id) is not int or job_id < 1:
            raise InvalidJobDefinitionError("job_id must be a positive integer")
        token = body.get("idempotency_token")
        if token is not None and not isinstance(token, str):
            raise InvalidJobDefinitionError("idempotency_token must be a string")
        python_parameters_value = body.get("python_params")
        notebook_parameters_value = body.get("notebook_params")
        if python_parameters_value is not None and notebook_parameters_value is not None:
            raise UnsupportedJobRequestError(
                "run-now cannot include both python_params and notebook_params"
            )
        parameters: ParameterOverride
        if python_parameters_value is not None:
            parameters = _python_parameters(python_parameters_value)
        elif notebook_parameters_value is not None:
            try:
                parameters = validate_notebook_parameters(notebook_parameters_value)
            except ExecutionContextResolutionError as error:
                raise InvalidJobDefinitionError(str(error)) from error
        else:
            parameters = None

        with self._lock:
            if self._closed:
                raise JobAdmissionError("the local job runner is shutting down")
            self._recover_abandoned_runs()
            job = self._repository.get_active_job(job_id)
            if parameters is not None and len(job.tasks) > 1:
                raise UnsupportedJobRequestError(
                    "run-now parameter overrides are supported only for single-task jobs; "
                    "set parameters on each task"
                )
            if isinstance(parameters, list) and job.tasks[0].task_type != "python_script":
                raise UnsupportedJobRequestError(
                    "python_params run-now overrides require a Python script task"
                )
            if isinstance(parameters, dict) and job.tasks[0].task_type != "notebook":
                raise UnsupportedJobRequestError(
                    "notebook_params run-now overrides require a notebook task"
                )
            submitted_settings: dict[str, object] = {}
            if parameters is not None:
                parameter_name = (
                    "python_params" if isinstance(parameters, list) else "notebook_params"
                )
                submitted_settings[parameter_name] = parameters
            if token is not None:
                replay = self._repository.find_submission_by_token(
                    job_id,
                    submitted_settings=submitted_settings,
                    submission_token=token,
                )
                if replay is not None:
                    return replay.run, True
            self._schedule_queued_runs()
            active_runs = self._active_persisted_runs()
            stable_job_id = (
                TerraformJobRegistry(self._context).resolve_resource_id(job_id)
                if admission_key is None
                else None
            )
            selected_admission_key = (
                admission_key
                if admission_key is not None
                else f"terraform:{stable_job_id}"
                if stable_job_id is not None
                else f"job:{job_id}"
            )
            max_concurrent_runs = job.settings.get("max_concurrent_runs", 1)
            if (
                type(max_concurrent_runs) is not int
                or not 1 <= max_concurrent_runs <= MAX_ADMITTED_JOB_RUNS
            ):
                raise JobRepositoryError("persisted job concurrency setting is invalid")
            active_for_key = sum(
                active_run.admission_key == selected_admission_key for active_run in active_runs
            )
            waiting_runs = self._repository.list_queued_runs()
            will_queue = (
                len(active_runs) >= MAX_ACTIVE_JOB_RUNS
                or active_for_key >= max_concurrent_runs
                or bool(waiting_runs)
            )
            if will_queue and len(waiting_runs) >= MAX_QUEUED_JOB_RUNS:
                raise JobAdmissionError(
                    f"the local queue limit of {MAX_QUEUED_JOB_RUNS} waiting runs is reached"
                )
            submission = self._repository.submit_run(
                job_id,
                submitted_settings=submitted_settings,
                submission_token=token,
                admission_key=selected_admission_key,
            )
            if not submission.replayed:
                self._schedule_queued_runs()
            return submission.run, submission.replayed

    def _schedule_queued_runs(self) -> None:
        """Dispatch eligible FIFO runs while both global and public-job slots remain."""
        if self._closed:
            return
        while True:
            active = self._active_persisted_runs()
            if len(active) >= MAX_ACTIVE_JOB_RUNS:
                return
            queued = self._repository.list_queued_runs()
            if not queued:
                return
            selected: tuple[JobRunRecord, JobRecord] | None = None
            for run in queued:
                job = self._repository.get_job(run.job_id)
                max_concurrent_runs = job.settings.get("max_concurrent_runs", 1)
                if type(max_concurrent_runs) is not int or not 1 <= max_concurrent_runs <= (
                    MAX_ADMITTED_JOB_RUNS
                ):
                    raise JobRepositoryError("persisted job concurrency setting is invalid")
                active_for_key = sum(
                    active_run.admission_key == run.admission_key for active_run in active
                )
                if active_for_key < max_concurrent_runs:
                    selected = (run, job)
                    break
            if selected is None:
                return
            run, job = selected
            try:
                self._repository.mark_run_dispatched(run.run_id)
                self._reserve_run_compute(run.run_id, job)
                cancel_event = threading.Event()
                thread = threading.Thread(
                    target=self._execute_run,
                    args=(run.run_id, job, _parameters_from_settings(run), cancel_event),
                    name=f"brickhouse-job-{run.run_id}",
                    daemon=False,
                )
                self._active[run.run_id] = (job.job_id, thread, cancel_event)
                thread.start()
            except Exception:
                self._active.pop(run.run_id, None)
                try:
                    self._context.compute.release_run_consumers(run.run_id)
                finally:
                    current = self._repository.get_run(run.run_id)
                    if current.state in {JobRunState.QUEUED, JobRunState.RUNNING}:
                        self._repository.interrupt_run(
                            run.run_id, error_code="WORKER_DISPATCH_FAILED"
                        )
                continue

    def get_run(self, run_id: int) -> tuple[JobRunRecord, tuple[JobTaskRunRecord, ...]]:
        """Return durable run and task states for SDK polling."""
        self._reconcile_run_if_abandoned(run_id)
        run = self._repository.get_run(run_id)
        return run, tuple(self._repository.list_task_runs(run_id))

    def get_run_output(self, run_id: int) -> tuple[JobRunRecord, JobTaskAttemptOutput | None]:
        """Return the latest attempt's bounded stdout/stderr for SDK output retrieval."""
        self._reconcile_run_if_abandoned(run_id)
        run = self._repository.get_run(run_id)
        tasks = self._repository.get_job(run.job_id).tasks
        if len(tasks) != 1:
            raise UnsupportedJobRequestError(
                "the pinned SDK output route supports one task per run"
            )
        task = tasks[0]
        attempts = self._repository.list_attempts(run_id, task.task_key)
        for attempt in reversed(attempts):
            output = self._repository.get_attempt_output(
                run_id, task.task_key, attempt.attempt_number
            )
            if output is not None:
                return run, output
        return run, None

    def close(self) -> None:
        """Cancel and reap active child processes before releasing workspace ownership."""
        with self._lock:
            self._closed = True
            active = tuple(self._active.values())
            for _, _, event in active:
                event.set()
        deadline = time.monotonic() + 10
        for _, thread, _ in active:
            thread.join(max(0.0, deadline - time.monotonic()))

    def _reserve_run_compute(self, run_id: int, job: JobRecord) -> None:
        """Persist each selected compute identity and all its planned task consumers."""
        grouped: dict[str, tuple[ComputeResourceKind, dict[str, object], list[str]]] = {}
        all_purpose: list[tuple[str, str]] = []
        for task in job.tasks:
            execution_context = task.settings.get("execution_context")
            if not isinstance(execution_context, Mapping):
                raise JobRepositoryError("submitted task has no resolved execution context")
            resource_kind_value = execution_context.get("compute_resource_kind")
            if resource_kind_value is None:
                if execution_context.get("compute_mode") != "serverless":
                    raise JobRepositoryError("classic task has no resolved compute resource")
                continue
            if resource_kind_value == ComputeResourceKind.ALL_PURPOSE.value:
                compute_id = execution_context.get("compute_id")
                if not isinstance(compute_id, str):
                    raise JobRepositoryError("existing-cluster task has no compute identity")
                all_purpose.append((compute_id, task.task_key))
                continue
            try:
                resource_kind = ComputeResourceKind(resource_kind_value)
            except ValueError as error:
                raise JobRepositoryError(
                    "submitted task has an invalid compute resource kind"
                ) from error
            if resource_kind not in {
                ComputeResourceKind.JOB_NEW,
                ComputeResourceKind.JOB_SHARED,
            }:
                raise JobRepositoryError("submitted task has an unsupported compute resource kind")
            compute_key = execution_context.get("compute_key")
            configuration = execution_context.get("cluster_configuration")
            if not isinstance(compute_key, str) or not isinstance(configuration, Mapping):
                raise JobRepositoryError("job-compute task has an incomplete resolved selector")
            body = _cluster_provisioning_body(configuration)
            existing = grouped.get(compute_key)
            if existing is None:
                grouped[compute_key] = (resource_kind, body, [task.task_key])
            elif existing[0] is resource_kind and existing[1] == body:
                existing[2].append(task.task_key)
            else:
                raise JobRepositoryError("job-compute consumers disagree on their resource")

        for compute_id, task_key in all_purpose:
            self._context.compute.register_all_purpose_consumer(compute_id, run_id, task_key)
        for compute_key, (resource_kind, body, task_keys) in grouped.items():
            self._context.compute.provision_job_compute(
                run_id,
                compute_key,
                resource_kind,
                body,
                task_keys,
            )

    def _execute_run(
        self,
        run_id: int,
        job: JobRecord,
        parameter_override: ParameterOverride,
        cancel_event: threading.Event,
    ) -> None:
        try:
            for task in _topological_tasks(job.tasks):
                if cancel_event.is_set():
                    current = self._repository.get_run(run_id)
                    if current.state in {JobRunState.QUEUED, JobRunState.RUNNING}:
                        self._context.compute.release_run_consumers(run_id)
                        self._repository.interrupt_run(
                            run_id, error_code="SHUTDOWN_BEFORE_TASK_DISPATCH"
                        )
                    return
                task_runs = {
                    task_run.task_key: task_run
                    for task_run in self._repository.list_task_runs(run_id)
                }
                dependency_states = [task_runs[dependency].state for dependency in task.depends_on]
                if any(state is not JobTaskState.SUCCEEDED for state in dependency_states):
                    unsuccessful = any(
                        state
                        in {
                            JobTaskState.FAILED,
                            JobTaskState.CANCELLED,
                            JobTaskState.SKIPPED,
                            JobTaskState.INTERRUPTED,
                        }
                        for state in dependency_states
                    )
                    if unsuccessful:
                        self._repository.mark_task_skipped(
                            run_id, task.task_key, error_code="DEPENDENCY_FAILED"
                        )
                        continue
                    raise JobRepositoryError(
                        f"task {task.task_key!r} is not ready after topological ordering"
                    )
                task_state = self._execute_task_with_retries(
                    run_id,
                    task,
                    parameter_override=parameter_override if len(job.tasks) == 1 else None,
                    cancel_event=cancel_event,
                )
                if task_state is JobTaskState.CANCELLED:
                    for pending in self._repository.list_task_runs(run_id):
                        if pending.state is JobTaskState.PENDING:
                            self._repository.mark_task_skipped(
                                run_id, pending.task_key, error_code="RUN_CANCELLED"
                            )
                    self._context.compute.release_run_consumers(run_id)
                    self._repository.finish_run(run_id, JobRunState.CANCELLED)
                    return
                if cancel_event.is_set():
                    for pending in self._repository.list_task_runs(run_id):
                        if pending.state is JobTaskState.PENDING:
                            self._repository.mark_task_skipped(
                                run_id, pending.task_key, error_code="RUN_CANCELLED"
                            )
                    self._context.compute.release_run_consumers(run_id)
                    self._repository.finish_run(run_id, JobRunState.CANCELLED)
                    return

            final_task_runs = self._repository.list_task_runs(run_id)
            self._context.compute.release_run_consumers(run_id)
            if any(task_run.state is JobTaskState.FAILED for task_run in final_task_runs):
                failed = next(
                    task_run
                    for task_run in final_task_runs
                    if task_run.state is JobTaskState.FAILED
                )
                self._repository.finish_run(
                    run_id, JobRunState.FAILED, error_code=failed.error_code or "TASK_FAILED"
                )
            elif all(task_run.state is JobTaskState.SUCCEEDED for task_run in final_task_runs):
                self._repository.finish_run(run_id, JobRunState.SUCCEEDED)
            else:
                raise JobRepositoryError("scheduler left tasks without terminal outcomes")
        except Exception:
            try:
                self._context.compute.release_run_consumers(run_id)
            except Exception:
                pass
            current = self._repository.get_run(run_id)
            if current.state in {JobRunState.QUEUED, JobRunState.RUNNING}:
                self._repository.interrupt_run(run_id, error_code="LOCAL_EXECUTION_ERROR")
        finally:
            with self._lock:
                self._active.pop(run_id, None)
                if not self._closed:
                    self._schedule_queued_runs()

    def _execute_task_with_retries(
        self,
        run_id: int,
        task: JobTaskDefinition,
        *,
        parameter_override: ParameterOverride,
        cancel_event: threading.Event,
    ) -> JobTaskState:
        """Retry observed worker failures within the task's immutable retry policy."""
        context_settings = task.settings.get("execution_context")
        if not isinstance(context_settings, Mapping):
            raise JobRepositoryError("submitted task has no resolved execution context")
        retry_settings = context_settings.get("retry_settings")
        if not isinstance(retry_settings, Mapping):
            raise JobRepositoryError("submitted task retry settings are malformed")
        max_retries = retry_settings.get("max_retries") or 0
        interval_millis = retry_settings.get("min_retry_interval_millis") or 0
        retry_on_timeout = retry_settings.get("retry_on_timeout") is True
        if type(max_retries) is not int or type(interval_millis) is not int:
            raise JobRepositoryError("submitted task retry settings are malformed")

        consumer = self._context.compute.get_run_consumer(run_id, task.task_key)
        compute_record: ComputeRecord | None = None
        watcher_stop: threading.Event | None = None
        watcher: threading.Thread | None = None
        if consumer is not None:
            execution_context = task.settings.get("execution_context")
            autostart = (
                isinstance(execution_context, Mapping)
                and execution_context.get("autostart") is True
            )
            compute_record = self._context.compute.start_for_consumer(
                consumer.compute_id,
                run_id,
                task.task_key,
                autostart=autostart,
            )
            watcher_stop = threading.Event()
            watcher = threading.Thread(
                target=self._watch_compute_generation,
                args=(compute_record, watcher_stop, cancel_event),
                name=f"brickhouse-compute-watch-{run_id}-{task.task_key}",
                daemon=True,
            )
            watcher.start()

        try:
            retry_number = 0
            while True:
                state = self._execute_task(
                    run_id,
                    task,
                    parameter_override=parameter_override,
                    cancel_event=cancel_event,
                    compute_record=compute_record,
                )
                if state is not JobTaskState.FAILED or retry_number >= max_retries:
                    return state
                attempts = self._repository.list_attempts(run_id, task.task_key)
                if not attempts:
                    raise JobRepositoryError("failed task has no persisted attempt")
                last_attempt = attempts[-1]
                retryable = last_attempt.termination_reason == "completed" or (
                    last_attempt.termination_reason == "timeout" and retry_on_timeout
                )
                if not retryable:
                    return state
                if cancel_event.wait(interval_millis / 1000):
                    current = self._repository.get_run(run_id)
                    if current.state in {JobRunState.QUEUED, JobRunState.RUNNING}:
                        self._repository.interrupt_run(
                            run_id, error_code="SHUTDOWN_DURING_RETRY_WAIT"
                        )
                    return state
                retry_number += 1
        finally:
            if watcher_stop is not None:
                watcher_stop.set()
            if watcher is not None:
                watcher.join(timeout=2)
            if consumer is not None:
                self._context.compute.release_consumer(consumer.compute_id, run_id, task.task_key)

    def _watch_compute_generation(
        self,
        selected: ComputeRecord,
        stop_event: threading.Event,
        cancel_event: threading.Event,
    ) -> None:
        """Cancel the run when simulated compute state invalidates a live worker."""
        while not stop_event.wait(0.025):
            if cancel_event.is_set():
                return
            try:
                current = self._context.compute.get_resource(selected.compute_id)
            except Exception:
                cancel_event.set()
                return
            if (
                current.generation != selected.generation
                or current.state is not ComputeState.RUNNING
            ):
                cancel_event.set()
                return

    def _execute_task(
        self,
        run_id: int,
        task: JobTaskDefinition,
        *,
        parameter_override: ParameterOverride,
        cancel_event: threading.Event,
        compute_record: ComputeRecord | None,
    ) -> JobTaskState:
        """Run one already-eligible task and persist its actual attempt outcome."""
        context_settings = task.settings.get("execution_context")
        if not isinstance(context_settings, Mapping):
            raise JobRepositoryError("submitted task has no resolved execution context")
        task_arguments = context_settings.get("task_arguments")
        if not isinstance(task_arguments, Mapping):
            raise JobRepositoryError("submitted task arguments are malformed")
        task_type = context_settings.get("task_type")
        if task_type == "python_script":
            workspace_path = task_arguments.get("python_file")
            if not isinstance(workspace_path, str):
                raise JobRepositoryError("submitted Python task has no workspace path")
            parameters_value = (
                parameter_override
                if parameter_override is not None
                else task_arguments.get("parameters", [])
            )
            if isinstance(parameters_value, dict):
                raise JobRepositoryError("notebook parameters were submitted for a Python task")
            python_parameters = _python_parameters(parameters_value)
            notebook_parameters: dict[str, str] | None = None
        elif task_type == "notebook":
            workspace_path = task_arguments.get("notebook_path")
            if not isinstance(workspace_path, str):
                raise JobRepositoryError("submitted notebook task has no workspace path")
            base_parameters = task_arguments.get("base_parameters", {})
            try:
                notebook_parameters = validate_notebook_parameters(base_parameters)
            except ExecutionContextResolutionError as error:
                raise JobRepositoryError("submitted notebook parameters are malformed") from error
            if parameter_override is not None:
                if not isinstance(parameter_override, dict):
                    raise JobRepositoryError("Python parameters were submitted for a notebook task")
                notebook_parameters.update(parameter_override)
            python_parameters = None
        else:
            raise JobRepositoryError("submitted task type is unsupported")
        attempt = self._repository.queue_attempt(run_id, task.task_key)
        attempt_number = attempt.attempt_number
        attempt = self._repository.start_attempt(run_id, task.task_key, attempt_number)
        try:
            source_path: Path
            if task_type == "python_script":
                source_path = self._files.get_python(workspace_path).local_path
            else:
                notebook = self._files.get_workspace_object(workspace_path)
                if notebook.object_type != "FILE" or notebook.local_path.suffix.casefold() not in {
                    ".ipynb",
                    ".py",
                }:
                    raise WorkspaceFileError("notebook task path is not a supported notebook file")
                source_path = notebook.local_path
            run = self._repository.get_run(run_id)
            secret_snapshot = self._context.secret_provider.snapshot_for_job(run.job_id)
            worker_context = _build_worker_context(
                cast(dict[str, object], context_settings),
                run_id=run_id,
                attempt_id=attempt.attempt_id,
                compute_record=compute_record,
                dbutils_configuration=(
                    {
                        "dbfs_root": str(self._context.workspace.path / "files" / "dbfs"),
                        "widget_parameters": _widget_parameters(
                            task_arguments.get("parameters", []),
                            parameter_override if isinstance(parameter_override, list) else None,
                        ),
                        "task_values_enabled": True,
                    }
                    if task_type == "python_script"
                    else None
                ),
            )
            if task_type == "notebook":
                if notebook_parameters is None:
                    raise JobRepositoryError("submitted notebook parameters are unavailable")
                notebook_context = worker_context.to_mapping()
                notebook_context["notebook_parameters"] = notebook_parameters
                worker_context = WorkerContext.from_mapping(notebook_context)
            timeout_value = context_settings.get("timeout_seconds")
            timeout_seconds = cast(int | None, timeout_value)
            worker_call_handler = self._worker_call_handler(
                run_id,
                task.task_key,
                attempt_number,
            )
            if task_type == "notebook":
                with tempfile.TemporaryDirectory(prefix="brickhouse-job-notebook-") as temporary:
                    artifact_path = Path(temporary) / "executed.ipynb"
                    result = run_python_worker(
                        _notebook_worker_entry(),
                        cwd=self._context.workspace.path / "files",
                        context=worker_context,
                        arguments=[str(artifact_path)],
                        timeout_seconds=timeout_seconds,
                        cancel_event=cancel_event,
                        outcome_journal=self._context.worker_outcomes,
                        secret_payload=secret_snapshot.payload,
                        secret_redactions=secret_snapshot.redaction_values,
                        worker_call_handler=worker_call_handler,
                    )
            else:
                if python_parameters is None:
                    raise JobRepositoryError("submitted Python task parameters are unavailable")
                result = run_python_worker(
                    source_path,
                    cwd=self._context.workspace.path / "files",
                    context=worker_context,
                    arguments=python_parameters,
                    timeout_seconds=timeout_seconds,
                    cancel_event=cancel_event,
                    outcome_journal=self._context.worker_outcomes,
                    secret_payload=secret_snapshot.payload,
                    secret_redactions=secret_snapshot.redaction_values,
                    worker_call_handler=worker_call_handler,
                )
            output = JobTaskAttemptOutput(
                run_id,
                task.task_key,
                attempt_number,
                result.stdout,
                result.stderr,
                result.stdout_truncated,
                result.stderr_truncated,
            )
            self._repository.finish_attempt(
                run_id,
                task.task_key,
                attempt_number,
                exit_code=result.exit_code,
                termination_reason=result.termination_reason,
                output=output,
            )
            return self._repository.get_task_run(run_id, task.task_key).state
        except (
            SecretProviderError,
            WorkerConfigurationError,
            WorkerLaunchError,
            WorkerServiceError,
            WorkspaceFileError,
        ):
            self._repository.finish_attempt(
                run_id,
                task.task_key,
                attempt_number,
                exit_code=None,
                termination_reason="launch_failed",
            )
            return self._repository.get_task_run(run_id, task.task_key).state

    def _worker_call_handler(
        self,
        run_id: int,
        reader_task_key: str,
        reader_attempt_number: int,
    ) -> WorkerCallHandler:
        """Bind private worker operations to one run/task/attempt identity."""

        def handle(message: WorkerCallMessage) -> object:
            if message.service == "tables":
                from brickhouse.catalog import (
                    NamespaceNotFoundError,
                    TableNotFoundError,
                    parse_identifier,
                )
                from brickhouse.tables import DeltaRsTableService, TableHandle, WriteMode

                if message.operation == "write":
                    import base64
                    import binascii
                    import hashlib

                    import pyarrow.ipc as ipc  # type: ignore[import-untyped]

                    expected_fields = {
                        "identifier",
                        "table_id",
                        "canonical_location",
                        "expected_version",
                        "format",
                        "mode",
                        "data_base64",
                    }
                    if set(message.payload) != expected_fields:
                        raise WorkerProtocolError("worker table write payload has invalid fields")
                    identifier = message.payload["identifier"]
                    table_id = message.payload["table_id"]
                    canonical_location = message.payload["canonical_location"]
                    expected_version = message.payload["expected_version"]
                    selected_format = message.payload["format"]
                    selected_mode = message.payload["mode"]
                    encoded_data = message.payload["data_base64"]
                    if (
                        not isinstance(identifier, str)
                        or not identifier
                        or not isinstance(table_id, str)
                        or not table_id
                        or not isinstance(canonical_location, str)
                        or not canonical_location
                        or type(expected_version) is not int
                        or expected_version < 0
                        or selected_format != "delta"
                        or selected_mode != "append"
                        or not isinstance(encoded_data, str)
                    ):
                        raise WorkerProtocolError("worker table write fields are invalid")
                    from brickhouse.runtime.worker_auth import (
                        MAX_WORKER_ARROW_BYTES,
                        MAX_WORKER_ARROW_ROWS,
                    )

                    maximum_encoded_size = ((MAX_WORKER_ARROW_BYTES + 2) // 3) * 4
                    if len(encoded_data) > maximum_encoded_size:
                        raise WorkerProtocolError("worker Arrow write exceeds the byte limit")
                    try:
                        raw_data = base64.b64decode(encoded_data, validate=True)
                    except (binascii.Error, ValueError) as error:
                        raise WorkerProtocolError(
                            "worker Arrow write encoding is invalid"
                        ) from error
                    if not raw_data or len(raw_data) > MAX_WORKER_ARROW_BYTES:
                        raise WorkerProtocolError("worker Arrow write exceeds the byte limit")

                    table_service = DeltaRsTableService(self._context)
                    try:
                        table = table_service.resolve(
                            parse_identifier(identifier),
                            self._context.new_namespace_state(),
                        )
                    except (NamespaceNotFoundError, TableNotFoundError) as error:
                        raise KeyError(
                            f"registered worker table does not exist: {identifier}"
                        ) from error
                    if table.table_id != table_id or str(table.location) != canonical_location:
                        raise WorkerProtocolError(
                            "worker write identity does not match the registered table"
                        )
                    current = table_service.read_snapshot(table)
                    if current.version != expected_version:
                        raise WorkerProtocolError(
                            "registered table changed after the worker resolved its snapshot"
                        )
                    try:
                        arrow_table = ipc.open_stream(raw_data).read_all()
                    except Exception as error:
                        raise WorkerProtocolError("worker Arrow write stream is invalid") from error
                    if arrow_table.num_rows > MAX_WORKER_ARROW_ROWS:
                        raise WorkerProtocolError("worker Arrow write exceeds the row limit")
                    operation_scope = (
                        f"{run_id}:{reader_task_key}:{reader_attempt_number}:"
                        f"{message.attempt_id}:{message.request_id}"
                    )
                    operation_id = (
                        "worker-" + hashlib.sha256(operation_scope.encode("utf-8")).hexdigest()
                    )
                    commit = table_service.write_arrow(
                        table,
                        arrow_table,
                        WriteMode.APPEND,
                        operation_id=operation_id,
                        expected_version=expected_version,
                    )
                    snapshot = commit.snapshot
                    receipt = {
                        "identifier": table.full_name,
                        "table_id": table.table_id,
                        "canonical_location": str(table.location),
                        "schema_json": snapshot.schema.json_text,
                        "provider": "brickhouse-local-delta",
                        "version": commit.version,
                        "committed_version": commit.version,
                    }
                    encoded_receipt = json.dumps(
                        {"ok": True, "result": receipt},
                        ensure_ascii=False,
                        allow_nan=False,
                        separators=(",", ":"),
                    ).encode("utf-8")
                    if len(encoded_receipt) > MAX_WORKER_MESSAGE_BYTES:
                        raise ValueError("worker table write receipt exceeds the service limit")
                    return receipt

                if message.operation == "is_registered_path":
                    if set(message.payload) != {"path"}:
                        raise WorkerProtocolError("worker table path payload has invalid fields")
                    path = message.payload["path"]
                    if not isinstance(path, str) or not path:
                        raise WorkerProtocolError("worker table path must be a non-empty string")
                    try:
                        self._context.tables.get_by_location(path)
                    except TableNotFoundError:
                        return False
                    return True

                if message.operation == "resolve":
                    if set(message.payload) != {"identifier"}:
                        raise WorkerProtocolError("worker table resolve payload has invalid fields")
                    identifier = message.payload["identifier"]
                    if not isinstance(identifier, str):
                        raise WorkerProtocolError("worker table identifier must be a string")
                    parsed = parse_identifier(identifier)
                    table_service = DeltaRsTableService(self._context)
                    try:
                        table = table_service.resolve(
                            parsed,
                            self._context.new_namespace_state(),
                        )
                    except (NamespaceNotFoundError, TableNotFoundError):
                        return None
                elif message.operation == "resolve_path":
                    if set(message.payload) != {"path"}:
                        raise WorkerProtocolError("worker table path payload has invalid fields")
                    path = message.payload["path"]
                    if not isinstance(path, str) or not path:
                        raise WorkerProtocolError("worker table path must be a non-empty string")
                    try:
                        registration = self._context.tables.get_by_location(path)
                    except TableNotFoundError:
                        return None
                    table_service = DeltaRsTableService(self._context)
                    table = TableHandle.from_registration(registration)
                else:
                    raise WorkerProtocolError("worker table operation is not supported")

                snapshot = table_service.read_snapshot(table)
                descriptor = {
                    "identifier": table.full_name,
                    "table_id": table.table_id,
                    "canonical_location": str(table.location),
                    "schema_json": snapshot.schema.json_text,
                    "provider": "brickhouse-local-delta",
                    "version": snapshot.version,
                }
                encoded = json.dumps(
                    {"ok": True, "result": descriptor},
                    ensure_ascii=False,
                    allow_nan=False,
                    separators=(",", ":"),
                ).encode("utf-8")
                if len(encoded) > MAX_WORKER_MESSAGE_BYTES:
                    raise ValueError("registered table descriptor exceeds the worker service limit")
                return descriptor

            if message.service != "task_values":
                raise WorkerProtocolError("worker service is not supported")
            if message.operation == "set":
                if set(message.payload) != {"key", "value"}:
                    raise WorkerProtocolError("task value set payload has invalid fields")
                key = message.payload["key"]
                if not isinstance(key, str):
                    raise WorkerProtocolError("task value key must be a string")
                return self._repository.set_task_value(
                    run_id,
                    reader_task_key,
                    reader_attempt_number,
                    key,
                    message.payload["value"],
                )
            if message.operation == "get":
                if set(message.payload) != {"key", "task_key"}:
                    raise WorkerProtocolError("task value get payload has invalid fields")
                key = message.payload["key"]
                upstream_task_key = message.payload["task_key"]
                if not isinstance(key, str) or not isinstance(upstream_task_key, str):
                    raise WorkerProtocolError("task value task and key names must be strings")
                found, value = self._repository.get_task_value(
                    run_id,
                    reader_task_key,
                    reader_attempt_number,
                    upstream_task_key,
                    key,
                )
                if not found:
                    raise KeyError(f"task value {key!r} was not set by {upstream_task_key!r}")
                return value
            raise WorkerProtocolError("task value operation is not supported")

        return handle


def _object_sequence(
    value: object,
    label: str,
    *,
    minimum: int = 0,
    maximum: int = 10,
) -> list[dict[str, object]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise InvalidJobDefinitionError(f"{label} must be a sequence of objects")
    if not minimum <= len(value) <= maximum:
        raise UnsupportedJobRequestError(
            f"{label} must contain between {minimum} and {maximum} supported entries"
        )
    if any(not isinstance(item, Mapping) for item in value):
        raise InvalidJobDefinitionError(f"{label} entries must be JSON objects")
    return [dict(item) for item in value]


def _validated_bundle_settings(body: Mapping[str, object]) -> dict[str, object]:
    """Retain only the selected CLI bundle's explicit deployment metadata subset."""
    retained: dict[str, object] = {}
    if "deployment" in body:
        deployment = body["deployment"]
        if not isinstance(deployment, Mapping) or set(deployment) != {
            "kind",
            "metadata_file_path",
        }:
            raise UnsupportedJobRequestError(
                "bundle deployment metadata requires kind and metadata_file_path"
            )
        metadata_path = deployment.get("metadata_file_path")
        if (
            deployment.get("kind") != "BUNDLE"
            or not isinstance(metadata_path, str)
            or not metadata_path.startswith(("/Workspace/Users/", "/Workspace/Shared/"))
            or PurePosixPath(metadata_path).as_posix() != metadata_path
            or ".." in PurePosixPath(metadata_path).parts
            or not metadata_path.endswith("/state/metadata.json")
        ):
            raise UnsupportedJobRequestError(
                "only normalized local bundle deployment metadata paths are supported"
            )
        retained["deployment"] = dict(deployment)

    if "edit_mode" in body:
        if body["edit_mode"] != "UI_LOCKED":
            raise UnsupportedJobRequestError("only UI_LOCKED bundle job edit mode is supported")
        retained["edit_mode"] = "UI_LOCKED"

    if "format" in body:
        if body["format"] != "MULTI_TASK":
            raise UnsupportedJobRequestError("only MULTI_TASK bundle jobs are supported")
        retained["format"] = "MULTI_TASK"

    if "queue" in body:
        queue = body["queue"]
        if not isinstance(queue, Mapping) or set(queue) != {"enabled"}:
            raise UnsupportedJobRequestError("bundle queue metadata requires only enabled")
        if type(queue.get("enabled")) is not bool:
            raise InvalidJobDefinitionError("bundle queue enabled must be a boolean")
        retained["queue"] = {"enabled": queue["enabled"]}

    if "tags" in body:
        tags = body["tags"]
        if (
            not isinstance(tags, Mapping)
            or len(tags) > 25
            or any(
                not isinstance(key, str)
                or not key
                or len(key) > 75
                or not isinstance(value, str)
                or len(value) > 500
                for key, value in tags.items()
            )
        ):
            raise InvalidJobDefinitionError("bundle tags must be bounded string pairs")
        retained["tags"] = dict(tags)
    return retained


def _dependencies(value: object) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise InvalidJobDefinitionError("depends_on must be a sequence")
    result: list[str] = []
    for dependency in value:
        if not isinstance(dependency, Mapping) or set(dependency) - {"task_key", "outcome"}:
            raise UnsupportedJobRequestError("unsupported task dependency shape")
        task_key = dependency.get("task_key")
        if not isinstance(task_key, str):
            raise InvalidJobDefinitionError("dependency task_key must be a string")
        if dependency.get("outcome") is not None:
            raise UnsupportedJobRequestError("conditional task dependencies are unsupported")
        result.append(task_key)
    return tuple(result)


def _python_parameters(value: object) -> list[str]:
    try:
        return validate_python_parameters(value)
    except ExecutionContextResolutionError as error:
        raise InvalidJobDefinitionError(str(error)) from error


def _parameters_from_settings(run: JobRunRecord) -> ParameterOverride:
    """Rebuild the already-validated parameter override of a persisted queued run."""
    settings = run.submitted_settings
    if "python_params" in settings:
        return _python_parameters(settings["python_params"])
    if "notebook_params" in settings:
        try:
            return validate_notebook_parameters(settings["notebook_params"])
        except ExecutionContextResolutionError as error:
            raise JobRepositoryError("queued notebook parameters are invalid") from error
    return None


def _resolved_context_settings(context: ResolvedTaskContext) -> dict[str, object]:
    cluster_configuration: object = None
    if context.cluster_configuration_json is not None:
        cluster_configuration = json.loads(context.cluster_configuration_json)
        if isinstance(cluster_configuration, dict):
            settings = cluster_configuration.get("settings")
            if isinstance(settings, dict):
                settings.pop("custom_tags", None)
    return {
        "execution_context": {
            "task_key": context.task_key,
            "task_type": context.task_type,
            "compute_mode": context.compute_mode.value,
            "profile": context.profile,
            "runtime_profile": context.runtime_profile,
            "access_mode": context.access_mode,
            "compute_resource_kind": (
                context.compute_resource_kind.value
                if context.compute_resource_kind is not None
                else None
            ),
            "compute_id": context.compute_id,
            "compute_key": context.compute_key,
            "compute_generation": context.compute_generation,
            "autostart": context.autostart,
            "cluster_configuration": cluster_configuration,
            "environment_key": context.environment_key,
            "environment_source": context.environment_source,
            "environment_profile": context.environment_profile,
            "environment_version": context.environment_version,
            "dependencies": list(context.dependencies),
            "performance_target": context.performance_target,
            "disable_auto_optimization": context.disable_auto_optimization,
            "timeout_seconds": context.timeout_seconds,
            "retry_settings": context.retry_settings,
            "effective_settings": context.effective_settings,
            "task_arguments": context.task_arguments,
        }
    }


def _notebook_worker_entry() -> Path:
    """Return the installed worker entry file used for a supervised notebook task."""
    return Path(__file__).resolve().parents[1] / "runtime" / "notebook_worker.py"


def _build_worker_context(
    settings: dict[str, object],
    *,
    run_id: int,
    attempt_id: str,
    compute_record: ComputeRecord | None = None,
    dbutils_configuration: Mapping[str, object] | None = None,
) -> WorkerContext:
    execution_context = settings.get("execution_context")
    if not isinstance(execution_context, Mapping):
        execution_context = settings
    profile_context = dict(execution_context)
    profile_context["run_id"] = str(run_id)
    profile_context["attempt_id"] = attempt_id
    if compute_record is not None:
        profile_context.update(
            compute_id=compute_record.compute_id,
            compute_generation=compute_record.generation,
            compute_state=compute_record.state.value,
            compute_simulated=compute_record.is_simulated,
        )
    if dbutils_configuration is not None:
        profile_context["dbutils"] = dict(dbutils_configuration)
    return WorkerContext.from_mapping(profile_context)


def _widget_parameters(
    task_parameters: object, run_now_parameters: list[str] | None
) -> dict[str, str]:
    """Resolve ``name=value`` task args, with run-now values taking precedence."""
    result: dict[str, str] = {}
    for values in (task_parameters, run_now_parameters):
        if values is None:
            continue
        if not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray)):
            raise JobRepositoryError("submitted Python parameters are malformed")
        for item in values:
            if not isinstance(item, str):
                raise JobRepositoryError("submitted Python parameters must be strings")
            name, separator, value = item.partition("=")
            if separator and name:
                result[name] = value
    return result


def _cluster_provisioning_body(configuration: Mapping[str, object]) -> dict[str, object]:
    """Rebuild the validated cluster request shape from its immutable context snapshot."""
    body = {key: value for key, value in configuration.items() if key != "settings"}
    settings = configuration.get("settings", {})
    if not isinstance(settings, Mapping):
        raise JobRepositoryError("resolved cluster settings are malformed")
    body.update(settings)
    return body


def _topological_tasks(tasks: Sequence[JobTaskDefinition]) -> tuple[JobTaskDefinition, ...]:
    """Return a stable topological order, preserving input order for eligible tasks."""
    pending = list(tasks)
    completed: set[str] = set()
    ordered: list[JobTaskDefinition] = []
    while pending:
        next_index = next(
            (index for index, task in enumerate(pending) if set(task.depends_on) <= completed),
            None,
        )
        if next_index is None:
            raise JobRepositoryError(
                "validated job dependencies could not be topologically ordered"
            )
        task = pending.pop(next_index)
        ordered.append(task)
        completed.add(task.task_key)
    return tuple(ordered)
