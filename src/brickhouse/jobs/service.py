"""Local SDK job definition and bounded Python worker execution composition."""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from pathlib import PurePosixPath
from typing import cast

from brickhouse.compute.models import ComputeRecord, ComputeResourceKind, ComputeState
from brickhouse.jobs.execution_context import (
    ExecutionContextResolutionError,
    ExecutionContextResolver,
    ResolvedTaskContext,
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
from brickhouse.runtime.worker_services import WorkerCallHandler, WorkerServiceError
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.files import WorkspaceFileNotFoundError, WorkspaceFileService

MAX_ACTIVE_JOB_RUNS = 4


class UnsupportedJobRequestError(ValueError):
    """A valid SDK job setting asks for an effect outside this implementation slice."""


class JobAdmissionError(RuntimeError):
    """The local job runner cannot admit another active run."""


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

    def _recover_abandoned_runs(self) -> None:
        """Reconcile runs left active by an earlier service process."""
        with self._lock:
            for job in self._repository.list_jobs():
                for run in self._repository.list_runs(job.job_id):
                    if run.state in {JobRunState.QUEUED, JobRunState.RUNNING}:
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
        active: list[JobRunRecord] = []
        for job in self._repository.list_jobs():
            active.extend(
                run
                for run in self._repository.list_runs(job.job_id)
                if run.state in {JobRunState.QUEUED, JobRunState.RUNNING}
            )
        return active

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
            or not 1 <= max_concurrent_runs <= MAX_ACTIVE_JOB_RUNS
        ):
            raise UnsupportedJobRequestError(
                "max_concurrent_runs must be between 1 and the local process limit "
                f"of {MAX_ACTIVE_JOB_RUNS}"
            )
        tasks = _object_sequence(body.get("tasks"), "tasks", minimum=1)
        job_clusters = _object_sequence(body.get("job_clusters", []), "job_clusters")
        environments = _object_sequence(body.get("environments", []), "environments")
        performance_target = body.get("performance_target")
        if performance_target is not None and not isinstance(performance_target, str):
            raise InvalidJobDefinitionError("performance_target must be a string")
        bundle_settings = _validated_bundle_settings(body)
        resolved = ExecutionContextResolver(self._context).resolve_job(
            tasks,
            job_clusters=job_clusters,
            environments=environments,
            performance_target=performance_target,
        )
        task_definitions: list[JobTaskDefinition] = []
        for task, task_context in zip(tasks, resolved, strict=True):
            if task_context.task_type != "python_script":
                raise UnsupportedJobRequestError(
                    "the SDK job route currently executes Python script tasks; notebook "
                    "execution belongs to the notebook kernel profile"
                )
            arguments = task_context.task_arguments
            script_path = arguments.get("python_file")
            if not isinstance(script_path, str):
                raise InvalidJobDefinitionError("resolved task is missing python_file")
            self._files.get_python(script_path)
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

    def submit_run(self, body: Mapping[str, object]) -> tuple[JobRunRecord, bool]:
        """Create one idempotent run and start its background worker if it is new."""
        allowed = {"job_id", "idempotency_token", "python_params"}
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
        parameters_value = body.get("python_params")
        parameters = _python_parameters(parameters_value) if parameters_value is not None else None

        with self._lock:
            if self._closed:
                raise JobAdmissionError("the local job runner is shutting down")
            self._recover_abandoned_runs()
            job = self._repository.get_active_job(job_id)
            if parameters is not None and len(job.tasks) > 1:
                raise UnsupportedJobRequestError(
                    "python_params run-now overrides are supported only for single-task jobs; "
                    "set parameters on each task"
                )
            submitted_settings: dict[str, object] = {}
            if parameters is not None:
                submitted_settings["python_params"] = parameters
            if token is not None:
                replay = self._repository.find_submission_by_token(
                    job_id,
                    submitted_settings=submitted_settings,
                    submission_token=token,
                )
                if replay is not None:
                    return replay.run, True
            active_runs = self._active_persisted_runs()
            job_active_count = sum(active_run.job_id == job_id for active_run in active_runs)
            max_concurrent_runs = job.settings.get("max_concurrent_runs", 1)
            if (
                type(max_concurrent_runs) is not int
                or not 1 <= max_concurrent_runs <= MAX_ACTIVE_JOB_RUNS
            ):
                raise JobRepositoryError("persisted job concurrency setting is invalid")
            if len(active_runs) >= MAX_ACTIVE_JOB_RUNS:
                raise JobAdmissionError(
                    f"the local process limit of {MAX_ACTIVE_JOB_RUNS} active runs is reached"
                )
            if job_active_count >= max_concurrent_runs:
                raise JobAdmissionError(
                    f"job {job_id} reached max_concurrent_runs={max_concurrent_runs}"
                )
            submission = self._repository.submit_run(
                job_id,
                submitted_settings=submitted_settings,
                submission_token=token,
            )
            if not submission.replayed:
                try:
                    self._reserve_run_compute(submission.run.run_id, job)
                except Exception as error:
                    try:
                        self._context.compute.release_run_consumers(submission.run.run_id)
                    finally:
                        self._repository.interrupt_run(
                            submission.run.run_id, error_code="COMPUTE_ADMISSION_FAILED"
                        )
                    raise JobAdmissionError(
                        "could not reserve the selected local compute resources"
                    ) from error
                cancel_event = threading.Event()
                thread = threading.Thread(
                    target=self._execute_run,
                    args=(submission.run.run_id, job, parameters, cancel_event),
                    name=f"brickhouse-job-{submission.run.run_id}",
                    daemon=False,
                )
                self._active[submission.run.run_id] = (job_id, thread, cancel_event)
                try:
                    thread.start()
                except RuntimeError as error:
                    self._active.pop(submission.run.run_id, None)
                    self._context.compute.release_run_consumers(submission.run.run_id)
                    self._repository.interrupt_run(
                        submission.run.run_id, error_code="WORKER_START_FAILED"
                    )
                    raise JobAdmissionError("could not start the local job worker") from error
            return submission.run, submission.replayed

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
        parameter_override: list[str] | None,
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

    def _execute_task_with_retries(
        self,
        run_id: int,
        task: JobTaskDefinition,
        *,
        parameter_override: list[str] | None,
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
        parameter_override: list[str] | None,
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
        script_path = task_arguments.get("python_file")
        if not isinstance(script_path, str):
            raise JobRepositoryError("submitted Python task has no workspace path")
        parameters_value = (
            parameter_override
            if parameter_override is not None
            else task_arguments.get("parameters", [])
        )
        parameters = _python_parameters(parameters_value)
        attempt = self._repository.queue_attempt(run_id, task.task_key)
        attempt_number = attempt.attempt_number
        attempt = self._repository.start_attempt(run_id, task.task_key, attempt_number)
        try:
            source = self._files.get_python(script_path)
            run = self._repository.get_run(run_id)
            secret_snapshot = self._context.secret_provider.snapshot_for_job(run.job_id)
            worker_context = _build_worker_context(
                cast(dict[str, object], context_settings),
                run_id=run_id,
                attempt_id=attempt.attempt_id,
                compute_record=compute_record,
                dbutils_configuration={
                    "dbfs_root": str(self._context.workspace.path / "files" / "dbfs"),
                    "widget_parameters": _widget_parameters(
                        task_arguments.get("parameters", []),
                        parameter_override,
                    ),
                    "task_values_enabled": True,
                },
            )
            timeout_value = context_settings.get("timeout_seconds")
            timeout_seconds = cast(int | None, timeout_value)
            result = run_python_worker(
                source.local_path,
                cwd=self._context.workspace.path / "files",
                context=worker_context,
                arguments=parameters,
                timeout_seconds=timeout_seconds,
                cancel_event=cancel_event,
                outcome_journal=self._context.worker_outcomes,
                secret_payload=secret_snapshot.payload,
                secret_redactions=secret_snapshot.redaction_values,
                worker_call_handler=self._task_value_handler(
                    run_id,
                    task.task_key,
                    attempt_number,
                ),
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
            WorkspaceFileNotFoundError,
        ):
            self._repository.finish_attempt(
                run_id,
                task.task_key,
                attempt_number,
                exit_code=None,
                termination_reason="launch_failed",
            )
            return self._repository.get_task_run(run_id, task.task_key).state

    def _task_value_handler(
        self,
        run_id: int,
        reader_task_key: str,
        reader_attempt_number: int,
    ) -> WorkerCallHandler:
        """Bind private worker operations to one run/task/attempt identity."""

        def handle(message: WorkerCallMessage) -> object:
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
