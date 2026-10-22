from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest
from databricks.sdk.service.compute import ClusterSpec, DataSecurityMode, Environment
from databricks.sdk.service.jobs import JobEnvironment, SparkPythonTask, Task
from sparkless.sql import SparklessError

from brickhouse.catalog import parse_identifier
from brickhouse.engines.sparkless import (
    SparklessAdapterError,
    SparklessSessionFactory,
    UnregisteredTableWriteError,
    UnsupportedDeltaSchemaError,
    UnsupportedSparklessConfigurationError,
    UnsupportedSparklessSessionOperationError,
    UnsupportedSparklessWriteError,
    _BrickHouseExternalCatalogProvider,
    _sparkless_schema,
)
from brickhouse.jobs import ExecutionContextResolver
from brickhouse.tables import (
    DeltaRsTableService,
    OperationKeyReuseError,
    StaleTableSnapshotError,
    TableHandle,
    WriteMode,
)
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.lifecycle import initialize_workspace


def _data(base: int) -> pa.Table:
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("amount", pa.decimal128(12, 2)),
            ("items", pa.list_(pa.int64())),
            ("details", pa.struct([("label", pa.string())])),
        ]
    )
    return pa.Table.from_pylist(
        [
            {
                "id": base,
                "amount": Decimal("12.34"),
                "items": [base, base + 1],
                "details": {"label": "present"},
            },
            {"id": base + 1, "amount": None, "items": None, "details": None},
            {
                "id": base + 2,
                "amount": Decimal("0.00"),
                "items": [],
                "details": {"label": None},
            },
        ],
        schema=schema,
    )


def _register_table(
    context: WorkspaceContext,
    *,
    schema_name: str = "default",
    table_name: str = "orders",
    base: int = 1,
) -> None:
    context.tables.create_managed("main", schema_name, table_name)
    service = DeltaRsTableService(context)
    handle = service.resolve(
        parse_identifier(f"main.{schema_name}.{table_name}"),
        context.new_namespace_state(),
    )
    service.write_arrow(handle, _data(base), WriteMode.CREATE, operation_id=f"seed-{base}")


def test_published_sparkless_reads_real_typed_delta_snapshots(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        _register_table(context)
        with SparklessSessionFactory(context).open() as session:
            rows = session.table("main.default.orders").sort("id").collect()

        assert rows == [
            (1, Decimal("12.34"), [1, 2], {"label": "present"}),
            (2, None, None, None),
            (3, Decimal("0.00"), [], {"label": None}),
        ]


def test_provider_binds_exact_latest_delta_version_and_rejects_unsupported_types(
    tmp_path: Path,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        _register_table(context)
        provider = _BrickHouseExternalCatalogProvider(context)
        descriptor = provider.resolve_table("main.default.orders")
        assert descriptor is not None
        assert descriptor.version == 0
        assert descriptor.table_id
        assert descriptor.provider == "brickhouse-local-delta"

        table = context.tables.list("main", "default")[0]
        handle = TableHandle.from_registration(table)
        DeltaRsTableService(context).write_arrow(
            handle,
            _data(10),
            WriteMode.APPEND,
            operation_id="append-after-binding",
        )
        latest = provider.resolve_table("main.default.orders")
        assert latest is not None
        assert latest.version == 1

    with pytest.raises(UnsupportedDeltaSchemaError, match="unsupported Delta type: float"):
        _sparkless_schema(
            '{"type":"struct","fields":[{"name":"value","type":"float",'
            '"nullable":true,"metadata":{}}]}'
        )
    with pytest.raises(UnsupportedDeltaSchemaError, match="unsupported Delta nested type: 'map'"):
        _sparkless_schema(
            '{"type":"struct","fields":[{"name":"value","type":{"type":"map",'
            '"keyType":"string","valueType":"long","valueContainsNull":true},'
            '"nullable":true,"metadata":{}}]}'
        )


def test_dataframe_named_and_registered_path_writes_use_the_table_service(
    tmp_path: Path,
) -> None:
    from sparkless.sql import types as T

    workspace = initialize_workspace(tmp_path / "workspace")
    schema = T.StructType(
        [
            T.StructField("id", T.LongType(), False),
            T.StructField("amount", T.DecimalType(12, 2), True),
            T.StructField("items", T.ArrayType(T.LongType(), True), True),
            T.StructField(
                "details",
                T.StructType([T.StructField("label", T.StringType(), True)]),
                True,
            ),
        ]
    )
    with WorkspaceContext.local(workspace.path) as context:
        with SparklessSessionFactory(context).open() as session:
            created = session.create_managed_table(
                "main.default.orders",
                session.spark.createDataFrame(_data(1), schema),
                operation_id="create-orders",
            )
            assert created.version == 0
            replayed = session.create_managed_table(
                "main.default.orders",
                session.spark.createDataFrame(_data(1), schema),
                operation_id="create-orders",
            )
            assert replayed.version == 0
            with pytest.raises(OperationKeyReuseError, match="different payload"):
                session.create_managed_table(
                    "main.default.orders",
                    session.spark.createDataFrame(_data(2), schema),
                    operation_id="create-orders",
                )

            appended = session.spark.createDataFrame(_data(10), schema)
            appended.write.format("delta").mode("append").saveAsTable("main.default.orders")

            registration = context.tables.get("main", "default", "orders")
            handle = TableHandle.from_registration(registration)
            path_commit = session.write_path(
                session.spark.createDataFrame(_data(20), schema),
                handle.location,
                WriteMode.APPEND,
                operation_id="path-append",
            )
            assert path_commit.version == 2
            replaced = session.spark.createDataFrame(_data(30), schema)
            replaced.write.format("DELTA").mode("overwrite").saveAsTable("main.default.orders")

            with pytest.raises(UnsupportedSparklessWriteError, match="path/insert writers"):
                appended.write.format("delta").mode("append").save(str(handle.location))
            with pytest.raises(UnregisteredTableWriteError, match="not registered"):
                appended.write.mode("append").saveAsTable("main.default.unregistered")
            with pytest.raises(UnsupportedSparklessWriteError, match="writer options"):
                appended.write.option("mergeSchema", "true").mode("append").saveAsTable(
                    "main.default.orders"
                )
            with pytest.raises(UnsupportedSparklessWriteError, match="multiple or terminated"):
                session.spark.sql("SELECT 1; INSERT INTO main.default.orders SELECT 2")
            quoted_semicolon = session.spark.sql("SELECT 'literal; value' AS value").toArrow()
            assert quoted_semicolon.to_pylist() == [{"value": "literal; value"}]
            quoted_injection = session.spark.sql(
                "SELECT 'x''); DROP TABLE orders; --' AS value"
            ).toArrow()
            assert quoted_injection.to_pylist() == [{"value": "x'); DROP TABLE orders; --"}]
            with pytest.raises(UnsupportedSparklessWriteError, match="path/insert writers"):
                appended.groupBy("id").count().write.mode("append").save(str(handle.location))
            with pytest.raises(UnsupportedSparklessWriteError, match="path/insert writers"):
                appended.na.fill(0).write.mode("append").save(str(handle.location))

            readback = DeltaRsTableService(context).read_table(handle).data.sort_by("id")
            assert readback.to_pylist() == _data(30).to_pylist()

        with SparklessSessionFactory(context).open() as fresh:
            assert fresh.table("main.default.orders").sort("id").select("id").collect() == [
                (30,),
                (31,),
                (32,),
            ]


def test_concurrent_sql_and_dataframe_appends_serialize_location_aliases_and_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import deltalake
    from deltalake import DeltaTable
    from sparkless.sql import types as T

    from brickhouse.sql import SqlStatementService
    from brickhouse.tables import StaleTableSnapshotError

    workspace_dataframe = initialize_workspace(tmp_path / "workspace-dataframe")
    workspace_sql = initialize_workspace(tmp_path / "workspace-sql")
    shared_location = tmp_path / "shared-delta"
    shared_location.mkdir()
    dataframe_location_alias = tmp_path / "shared-delta-alias"
    dataframe_location_alias.symlink_to(shared_location, target_is_directory=True)
    dataframe_operation_id = "concurrent-dataframe-append"
    sql_operation_id = "concurrent-sql-merge-append"
    merge_sql = (
        "MERGE INTO main.default.shared_orders AS t "
        "USING main.default.incoming AS s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET id = s.id "
        "WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)"
    )

    with WorkspaceContext.local(workspace_dataframe.path) as context:
        dataframe_registration = context.tables.register_external(
            "main", "default", "shared_orders", dataframe_location_alias
        )
        dataframe_handle = TableHandle.from_registration(dataframe_registration)
        seed = DeltaRsTableService(context).write_arrow(
            dataframe_handle,
            pa.table({"id": pa.array([0], type=pa.int64())}),
            WriteMode.CREATE,
            operation_id="seed-concurrent-aliases",
        )
        assert seed.version == 0

    with WorkspaceContext.local(workspace_sql.path) as context:
        sql_registration = context.tables.register_external(
            "main", "default", "shared_orders", shared_location
        )
        sql_handle = TableHandle.from_registration(sql_registration)
        source_registration = context.tables.create_managed("main", "default", "incoming")
        source_handle = TableHandle.from_registration(source_registration)
        source_seed = DeltaRsTableService(context).write_arrow(
            source_handle,
            pa.table({"id": pa.array([10], type=pa.int64())}),
            WriteMode.CREATE,
            operation_id="seed-concurrent-incoming",
        )
        assert source_seed.version == 0

    assert dataframe_handle.table_id != sql_handle.table_id
    assert dataframe_handle.location == sql_handle.location == shared_location.resolve()

    active_guard = threading.Lock()
    active_dispatches = 0
    maximum_active_dispatches = 0
    dispatch_count = 0
    dataframe_dispatch_started = threading.Event()
    release_dataframe_dispatch = threading.Event()
    sql_dispatch_started = threading.Event()
    release_sql_dispatch = threading.Event()
    sql_mutation_ready = threading.Event()
    sql_expected_versions: list[int | None] = []

    def observe_commit(
        action,
        *,
        hold_for_dataframe: bool = False,
        hold_for_sql: bool = False,
    ):
        nonlocal active_dispatches, maximum_active_dispatches, dispatch_count
        with active_guard:
            active_dispatches += 1
            dispatch_count += 1
            maximum_active_dispatches = max(maximum_active_dispatches, active_dispatches)
        try:
            if hold_for_dataframe:
                dataframe_dispatch_started.set()
                if not release_dataframe_dispatch.wait(timeout=15):
                    raise TimeoutError("timed out waiting to release the DataFrame Delta commit")
            elif hold_for_sql:
                sql_dispatch_started.set()
                if not release_sql_dispatch.wait(timeout=15):
                    raise TimeoutError("timed out waiting to release the SQL Delta commit")
            else:
                time.sleep(0.1)
            return action()
        finally:
            with active_guard:
                active_dispatches -= 1

    original_write_deltalake = deltalake.write_deltalake

    def observed_write_deltalake(*args, **kwargs):
        return observe_commit(
            lambda: original_write_deltalake(*args, **kwargs), hold_for_dataframe=True
        )

    original_merge = deltalake.DeltaTable.merge

    class ObservedMerge:
        def __init__(self, merger):
            self._merger = merger

        def when_matched_update(self, *args, **kwargs):
            self._merger = self._merger.when_matched_update(*args, **kwargs)
            return self

        def when_not_matched_insert(self, *args, **kwargs):
            self._merger = self._merger.when_not_matched_insert(*args, **kwargs)
            return self

        def execute(self, *args, **kwargs):
            return observe_commit(lambda: self._merger.execute(*args, **kwargs), hold_for_sql=True)

    def observed_merge(table, *args, **kwargs):
        return ObservedMerge(original_merge(table, *args, **kwargs))

    original_merge_rows = DeltaRsTableService.merge_rows

    def observed_merge_rows(service, table, *args, **kwargs):
        if table.table_id == sql_handle.table_id and kwargs.get("operation_id") == sql_operation_id:
            sql_expected_versions.append(kwargs.get("expected_version"))
            sql_mutation_ready.set()
        return original_merge_rows(service, table, *args, **kwargs)

    monkeypatch.setattr(deltalake, "write_deltalake", observed_write_deltalake)
    monkeypatch.setattr(deltalake.DeltaTable, "merge", observed_merge)
    monkeypatch.setattr(DeltaRsTableService, "merge_rows", observed_merge_rows)

    def dataframe_writer() -> tuple[int, int]:
        with WorkspaceContext.local(workspace_dataframe.path) as context:
            with SparklessSessionFactory(context).open() as session:
                frame = session.spark.createDataFrame(
                    [(20,)], T.StructType([T.StructField("id", T.LongType(), False)])
                )
                committed = session.write_path(
                    frame,
                    dataframe_location_alias,
                    WriteMode.APPEND,
                    operation_id=dataframe_operation_id,
                )
                replayed = session.write_path(
                    frame,
                    dataframe_location_alias,
                    WriteMode.APPEND,
                    operation_id=dataframe_operation_id,
                )
                return committed.version, replayed.version

    def sql_writer() -> tuple[int, int, int]:
        with WorkspaceContext.local(workspace_sql.path) as context:
            sql = SqlStatementService(context)
            stale_retries = 0
            for attempt in range(2):
                try:
                    committed = sql.execute(merge_sql, operation_id=sql_operation_id)
                except StaleTableSnapshotError:
                    assert (
                        context.table_operations.get(sql_handle.table_id, sql_operation_id) is None
                    )
                    if attempt == 1:
                        raise
                    stale_retries += 1
                else:
                    break
            replayed = sql.execute(merge_sql, operation_id=sql_operation_id)
            assert committed.commit is not None
            assert replayed.commit is not None
            return committed.commit.commit_version, replayed.commit.commit_version, stale_retries

    with ThreadPoolExecutor(max_workers=2) as executor:
        dataframe_future = executor.submit(dataframe_writer)
        assert dataframe_dispatch_started.wait(timeout=15)
        sql_future = executor.submit(sql_writer)
        try:
            # SQL captures version 0 while the DataFrame commit holds the location lock.
            assert sql_mutation_ready.wait(timeout=15)
            sql_dispatched_before_dataframe_release = sql_dispatch_started.wait(timeout=0.1)
        finally:
            release_dataframe_dispatch.set()
            release_sql_dispatch.set()
        dataframe_versions = dataframe_future.result(timeout=45)
        sql_versions = sql_future.result(timeout=45)

    assert not sql_dispatched_before_dataframe_release
    assert dataframe_versions[0] == dataframe_versions[1]
    assert sql_versions[0] == sql_versions[1]
    assert sql_versions[2] == 1
    assert sql_expected_versions[:2] == [0, 1]
    assert sorted([dataframe_versions[0], sql_versions[0]]) == [1, 2]
    assert dispatch_count == 2
    assert maximum_active_dispatches == 1

    with WorkspaceContext.local(workspace_dataframe.path) as context:
        current_registration = context.tables.get_by_id(dataframe_handle.table_id)
        current_handle = TableHandle.from_registration(current_registration)
        result = DeltaRsTableService(context).read_table(current_handle)
        assert sorted(row["id"] for row in result.data.to_pylist()) == [0, 10, 20]
        history = DeltaTable(str(shared_location)).history()
        assert len(history) == 3
        history_by_operation = {
            item["brickhouse.operation_id"]: item["version"] for item in history
        }
        assert history_by_operation == {
            "seed-concurrent-aliases": 0,
            dataframe_operation_id: dataframe_versions[0],
            sql_operation_id: sql_versions[0],
        }
        dataframe_record = context.table_operations.get(
            dataframe_handle.table_id, dataframe_operation_id
        )
        assert dataframe_record is not None
        assert dataframe_record.state.value == "committed"
        assert dataframe_record.commit_version == dataframe_versions[0]

    with WorkspaceContext.local(workspace_sql.path) as context:
        sql_record = context.table_operations.get(sql_handle.table_id, sql_operation_id)
        assert sql_record is not None
        assert sql_record.state.value == "committed"
        assert sql_record.commit_version == sql_versions[0]


def test_sparkless_creates_and_reads_a_typed_empty_managed_table(tmp_path: Path) -> None:
    from sparkless.sql import types as T

    workspace = initialize_workspace(tmp_path / "workspace")
    empty = pa.Table.from_batches(
        [],
        schema=pa.schema(
            [pa.field("id", pa.int64(), nullable=False), pa.field("label", pa.string())]
        ),
    )
    schema = T.StructType(
        [
            T.StructField("id", T.LongType(), False),
            T.StructField("label", T.StringType(), True),
        ]
    )

    with WorkspaceContext.local(workspace.path) as context:
        with SparklessSessionFactory(context).open() as session:
            commit = session.create_managed_table(
                "main.default.empty_orders",
                session.spark.createDataFrame(empty, schema),
                operation_id="sparkless-empty-create",
            )
            assert commit.version == 0
            readback = session.table("main.default.empty_orders").toArrow()
            assert readback.num_rows == 0
            assert readback.schema.names == ["id", "label"]
            assert [field.type for field in readback.schema] == [pa.int64(), pa.string()]

        registration = context.tables.get("main", "default", "empty_orders")
        stored = DeltaRsTableService(context).read_table(
            TableHandle.from_registration(registration)
        )
        assert stored.snapshot.version == 0
        assert stored.data.num_rows == 0
        assert stored.data.schema.names == ["id", "label"]


def test_sparkless_rejects_schema_mismatched_append_before_delta_commit(
    tmp_path: Path,
) -> None:
    from sparkless.sql import types as T

    workspace = initialize_workspace(tmp_path / "workspace")
    valid_schema = T.StructType(
        [
            T.StructField("id", T.LongType(), False),
            T.StructField("label", T.StringType(), True),
        ]
    )
    wrong_schema = T.StructType(
        [
            T.StructField("id", T.StringType(), False),
            T.StructField("label", T.StringType(), True),
        ]
    )
    valid_data = pa.table({"id": pa.array([1], type=pa.int64()), "label": pa.array(["kept"])})
    wrong_data = pa.table(
        {"id": pa.array(["two"], type=pa.string()), "label": pa.array(["rejected"])}
    )

    with WorkspaceContext.local(workspace.path) as context:
        with SparklessSessionFactory(context).open() as session:
            created = session.create_managed_table(
                "main.default.typed_orders",
                session.spark.createDataFrame(valid_data, valid_schema),
                operation_id="sparkless-schema-create",
            )
            with pytest.raises(SparklessError, match="incompatible_table_schema"):
                session.spark.createDataFrame(wrong_data, wrong_schema).write.format("delta").mode(
                    "append"
                ).saveAsTable("main.default.typed_orders")

        registration = context.tables.get("main", "default", "typed_orders")
        readback = DeltaRsTableService(context).read_table(
            TableHandle.from_registration(registration)
        )
        assert created.version == readback.snapshot.version == 0
        assert readback.data.to_pylist() == [{"id": 1, "label": "kept"}]


def test_existing_sparkless_session_rejects_table_after_registration_drop(
    tmp_path: Path,
) -> None:
    from sparkless.sql import types as T

    workspace = initialize_workspace(tmp_path / "workspace")
    schema = T.StructType(
        [T.StructField("id", T.LongType(), False), T.StructField("label", T.StringType())]
    )
    data = pa.table({"id": pa.array([1], type=pa.int64()), "label": pa.array(["before-drop"])})

    with WorkspaceContext.local(workspace.path) as context:
        with SparklessSessionFactory(context).open() as session:
            session.create_managed_table(
                "main.default.stale_orders",
                session.spark.createDataFrame(data, schema),
                operation_id="stale-session-create",
            )
            assert session.table("main.default.stale_orders").collect() == [(1, "before-drop")]
            registration = context.tables.get("main", "default", "stale_orders")
            handle = TableHandle.from_registration(registration)
            DeltaRsTableService(context).drop_table(handle)

            with pytest.raises(SparklessError):
                session.table("main.default.stale_orders").collect()
            with pytest.raises(UnregisteredTableWriteError, match="not registered"):
                session.spark.createDataFrame(data, schema).write.format("delta").mode(
                    "append"
                ).saveAsTable("main.default.stale_orders")

            assert not registration.canonical_location.exists()


def test_delta_service_rejects_a_stale_snapshot_write(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        _register_table(context)
        registration = context.tables.get("main", "default", "orders")
        handle = TableHandle.from_registration(registration)
        service = DeltaRsTableService(context)
        service.write_arrow(
            handle,
            _data(10),
            WriteMode.APPEND,
            operation_id="fresh-append",
            expected_version=0,
        )
        with pytest.raises(ValueError, match="integer zero or greater"):
            service.write_arrow(
                handle,
                _data(15),
                WriteMode.APPEND,
                operation_id="boolean-snapshot-version",
                expected_version=True,
            )
        with pytest.raises(StaleTableSnapshotError, match="expected version 0"):
            service.write_arrow(
                handle,
                _data(20),
                WriteMode.APPEND,
                operation_id="stale-append",
                expected_version=0,
            )
        assert service.read_snapshot(handle).version == 1


def test_factory_sessions_isolate_namespace_configuration_and_temp_views(
    tmp_path: Path,
) -> None:
    from sparkless.sql import functions as F
    from sparkless.sql import types as T

    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        context.schemas.create("main", "red")
        context.schemas.create("main", "blue")
        _register_table(context, schema_name="red", table_name="shared", base=100)
        _register_table(context, schema_name="blue", table_name="shared", base=200)
        resolver = ExecutionContextResolver(context)
        red_task = Task(
            task_key="serverless",
            spark_python_task=SparkPythonTask(python_file="/Workspace/Shared/task.py"),
            environment_key="python311",
            disable_auto_optimization=True,
        )
        red_environment = JobEnvironment(
            environment_key="python311",
            spec=Environment(environment_version="1"),
        )
        red_context = resolver.resolve_job(
            [red_task.as_dict()],
            environments=[red_environment.as_dict()],
        )[0]
        blue_task = Task(
            task_key="classic",
            spark_python_task=SparkPythonTask(python_file="/Workspace/Shared/task.py"),
            new_cluster=ClusterSpec(
                cluster_name="classic-explicit-settings",
                spark_version="spark35-local-v1",
                data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_STANDARD,
                num_workers=0,
                spark_conf={
                    "spark.sql.ansi.enabled": "false",
                    "spark.sql.session.timeZone": "America/Los_Angeles",
                },
            ),
        )
        blue_context = resolver.resolve_job([blue_task.as_dict()])[0]
        factory = SparklessSessionFactory(context)
        with (
            factory.open_for_execution(red_context) as red,
            factory.open_for_execution(blue_context) as blue,
        ):
            red.use_schema("main.red")
            blue.use_schema("main.blue")
            assert red.compute_profile == "serverless-local-v1"
            assert red.runtime_profile is None
            assert blue.compute_profile == "classic-standard-local-v1"
            assert blue.runtime_profile == "spark35-local-v1"
            assert red.table("shared").select("id").collect() == [(100,), (101,), (102,)]
            assert blue.table("shared").select("id").collect() == [(200,), (201,), (202,)]
            assert red.spark.conf.get("spark.sql.session.timeZone") == "UTC"
            assert blue.spark.conf.get("spark.sql.session.timeZone") == "America/Los_Angeles"
            assert red.spark.conf.get("spark.sql.ansi.enabled") == "true"
            assert blue.spark.conf.get("spark.sql.ansi.enabled") == "false"
            assert red.configuration["spark.sql.session.timeZone"] == "UTC"

            red_udf = F.udf(lambda value: value + 1000, T.LongType())
            blue_udf = F.udf(lambda value: value + 2000, T.LongType())
            red_dataframe = red.spark.createDataFrame(
                [(7,)], T.StructType([T.StructField("value", T.LongType(), False)])
            ).withColumn("adjusted", red_udf(F.col("value")))
            blue_dataframe = blue.spark.createDataFrame(
                [(7,)], T.StructType([T.StructField("value", T.LongType(), False)])
            ).withColumn("adjusted", blue_udf(F.col("value")))
            assert red_dataframe.schema["adjusted"].dataType == T.LongType()
            assert blue_dataframe.schema["adjusted"].dataType == T.LongType()
            red_result = red_dataframe.collect()
            blue_result = blue_dataframe.collect()
            assert red_result == [(7, 1007)]
            assert blue_result == [(7, 2007)]

            red.spark.sql("CREATE TEMP VIEW local_only AS SELECT 7 AS value")
            assert red.spark.table("local_only").collect() == [(7,)]
            with pytest.raises(SparklessError):
                blue.spark.table("local_only").collect()


def test_factory_rejects_unreviewed_session_settings_and_engine_profiles(
    tmp_path: Path,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        with pytest.raises(UnsupportedSparklessConfigurationError, match="spark.databricks"):
            SparklessSessionFactory(context).open(
                configuration={"spark.databricks.token": "not-a-secret"}
            )
        with pytest.raises(UnsupportedSparklessConfigurationError, match="profile"):
            SparklessSessionFactory(context, engine_profile="spark4-local-v1")


def test_factory_applies_resolved_serverless_and_classic_profiles(
    tmp_path: Path,
) -> None:
    from dataclasses import replace

    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        resolver = ExecutionContextResolver(context)
        serverless_task = Task(
            task_key="serverless",
            spark_python_task=SparkPythonTask(python_file="/Workspace/Shared/task.py"),
            environment_key="python311",
            disable_auto_optimization=True,
        )
        serverless_environment = JobEnvironment(
            environment_key="python311",
            spec=Environment(environment_version="1"),
        )
        serverless_context = resolver.resolve_job(
            [serverless_task.as_dict()],
            environments=[serverless_environment.as_dict()],
        )[0]

        classic_task = Task(
            task_key="classic",
            spark_python_task=SparkPythonTask(python_file="/Workspace/Shared/task.py"),
            new_cluster=ClusterSpec(
                cluster_name="classic-explicit-settings",
                spark_version="spark35-local-v1",
                data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_STANDARD,
                num_workers=0,
                spark_conf={
                    "spark.sql.ansi.enabled": "false",
                    "spark.sql.session.timeZone": "America/Los_Angeles",
                },
            ),
        )
        classic_context = resolver.resolve_job([classic_task.as_dict()])[0]
        factory = SparklessSessionFactory(context)
        with (
            factory.open_for_execution(serverless_context) as serverless,
            factory.open_for_execution(classic_context) as classic,
        ):
            assert serverless.compute_profile == "serverless-local-v1"
            assert serverless.runtime_profile is None
            assert serverless.configuration["spark.sql.ansi.enabled"] == "true"
            assert serverless.configuration["spark.sql.session.timeZone"] == "UTC"
            assert classic.compute_profile == "classic-standard-local-v1"
            assert classic.runtime_profile == "spark35-local-v1"
            assert classic.configuration["spark.sql.ansi.enabled"] == "false"
            assert classic.configuration["spark.sql.session.timeZone"] == "America/Los_Angeles"
            assert (
                serverless.configuration["spark.sql.session.timeZone"]
                != (classic.configuration["spark.sql.session.timeZone"])
            )

        unconfigured_classic_task = Task(
            task_key="classic-defaults-unmapped",
            spark_python_task=SparkPythonTask(python_file="/Workspace/Shared/task.py"),
            new_cluster=ClusterSpec(
                cluster_name="classic-defaults-unmapped",
                spark_version="spark35-local-v1",
                data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_STANDARD,
                num_workers=0,
            ),
        )
        unconfigured_context = resolver.resolve_job([unconfigured_classic_task.as_dict()])[0]
        with pytest.raises(
            UnsupportedSparklessConfigurationError, match="defaults are not inferred"
        ):
            factory.open_for_execution(unconfigured_context)

        unmapped_runtime = replace(classic_context, runtime_profile="spark42-local-v1")
        with pytest.raises(UnsupportedSparklessConfigurationError, match="runtime is not mapped"):
            factory.open_for_execution(unmapped_runtime)

        unsupported_settings = replace(
            classic_context,
            effective_settings_json=(
                '{"spark.databricks.secret":"hidden",'
                '"spark.sql.ansi.enabled":"false",'
                '"spark.sql.session.timeZone":"America/Los_Angeles"}'
            ),
        )
        with pytest.raises(
            UnsupportedSparklessConfigurationError, match="unsupported Sparkless session setting"
        ):
            factory.open_for_execution(unsupported_settings)


def test_b019_1_7_serverless_and_classic_profile_sql_semantics(tmp_path: Path) -> None:
    fixture_path = (
        Path(__file__).resolve().parents[1] / "fixtures" / "sql" / "B019.1.7-profile-cases.json"
    )
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        resolver = ExecutionContextResolver(context)
        python_file = "/Workspace/Shared/profile.py"
        serverless_task = Task(
            task_key="serverless",
            spark_python_task=SparkPythonTask(python_file=python_file),
            environment_key="python311",
            disable_auto_optimization=True,
        )
        environment = JobEnvironment(
            environment_key="python311",
            spec=Environment(environment_version="1"),
        )
        serverless_context = resolver.resolve_job(
            [serverless_task.as_dict()], environments=[environment.as_dict()]
        )[0]

        standard_task = Task(
            task_key="classic-standard",
            spark_python_task=SparkPythonTask(python_file=python_file),
            new_cluster=ClusterSpec(
                cluster_name="classic-standard-profile",
                spark_version="spark35-local-v1",
                data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_STANDARD,
                num_workers=0,
                spark_conf=fixture["engine_profiles"]["classic_standard"]["settings"],
            ),
        )
        standard_context = resolver.resolve_job([standard_task.as_dict()])[0]

        dedicated_task = Task(
            task_key="classic-dedicated",
            spark_python_task=SparkPythonTask(python_file=python_file),
            new_cluster=ClusterSpec(
                cluster_name="classic-dedicated-profile",
                spark_version="spark35-local-v1",
                data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_DEDICATED,
                single_user_name="local-user",
                num_workers=0,
                spark_conf=fixture["engine_profiles"]["classic_dedicated"]["settings"],
            ),
        )
        dedicated_context = resolver.resolve_job([dedicated_task.as_dict()])[0]

        factory = SparklessSessionFactory(context)
        assert serverless_context.profile == fixture["engine_profiles"]["serverless"]["profile"]
        assert (
            serverless_context.effective_settings
            == fixture["engine_profiles"]["serverless"]["settings"]
        )
        assert standard_context.profile == fixture["engine_profiles"]["classic_standard"]["profile"]
        assert (
            standard_context.runtime_profile
            == fixture["engine_profiles"]["classic_standard"]["runtime_profile"]
        )
        assert (
            standard_context.effective_settings
            == fixture["engine_profiles"]["classic_standard"]["settings"]
        )
        assert (
            dedicated_context.profile == fixture["engine_profiles"]["classic_dedicated"]["profile"]
        )
        assert (
            dedicated_context.runtime_profile
            == fixture["engine_profiles"]["classic_dedicated"]["runtime_profile"]
        )
        assert (
            dedicated_context.effective_settings
            == fixture["engine_profiles"]["classic_dedicated"]["settings"]
        )

        with (
            factory.open_for_execution(serverless_context) as serverless,
            factory.open_for_execution(standard_context) as classic_standard,
            factory.open_for_execution(dedicated_context) as classic_dedicated,
        ):
            sessions = {
                "serverless": serverless,
                "classic_standard": classic_standard,
                "classic_dedicated": classic_dedicated,
            }
            for name, session in sessions.items():
                profile = fixture["engine_profiles"][name]
                assert session.compute_profile == profile["profile"]
                assert session.runtime_profile == profile["runtime_profile"]
                assert dict(session.configuration) == {
                    **profile["settings"],
                    "spark.sql.caseSensitive": "false",
                }
                assert (
                    session.spark.conf.get("spark.sql.ansi.enabled")
                    == profile["settings"]["spark.sql.ansi.enabled"]
                )
                assert (
                    session.spark.conf.get("spark.sql.session.timeZone")
                    == profile["settings"]["spark.sql.session.timeZone"]
                )

                value = fixture["temporary_view_scope"]["values"][name]
                session.spark.sql(f"CREATE TEMP VIEW profile_value AS SELECT '{value}' AS value")
                assert session.spark.sql("SELECT value FROM profile_value").collect() == [(value,)]

            for name in sessions:
                for other_name, other_session in sessions.items():
                    if name == other_name:
                        continue
                    other_value = fixture["temporary_view_scope"]["values"][other_name]
                    assert other_value != fixture["temporary_view_scope"]["values"][name]
                    assert other_session.spark.sql("SELECT value FROM profile_value").collect() == [
                        (other_value,)
                    ]

            with pytest.raises(
                UnsupportedSparklessSessionOperationError,
                match="global temporary views are unsupported",
            ):
                serverless.spark.sql("CREATE GLOBAL TEMP VIEW global_value AS SELECT 1 AS value")
            with pytest.raises(
                UnsupportedSparklessSessionOperationError,
                match=r"DataFrame.cache\(\) is unsupported",
            ):
                serverless.spark.table("profile_value").cache()
            with pytest.raises(
                UnsupportedSparklessSessionOperationError,
                match="SQL table caching is unsupported",
            ):
                classic_standard.spark.sql("CACHE TABLE profile_value")
            with pytest.raises(
                UnsupportedSparklessSessionOperationError,
                match=r"DataFrame.cache\(\) is unsupported",
            ):
                classic_dedicated.spark.table("profile_value").cache()

            deferred = fixture["deferred_analysis"]
            with pytest.raises(SparklessError) as eager_error:
                serverless.spark.sql(deferred["sql"])
            assert "missing_column" in str(eager_error.value)
            with pytest.raises(SparklessError):
                classic_standard.spark.sql(deferred["sql"])

        bad_serverless = replace(serverless_context, runtime_profile="spark35-local-v1")
        with pytest.raises(
            UnsupportedSparklessConfigurationError,
            match=fixture["profile_errors"]["serverless_with_classic_runtime"]["message_fragment"],
        ):
            factory.open_for_execution(bad_serverless)

        unconfigured_classic = replace(standard_context, effective_settings_json="{}")
        with pytest.raises(
            UnsupportedSparklessConfigurationError,
            match=fixture["profile_errors"]["classic_without_explicit_settings"][
                "message_fragment"
            ],
        ):
            factory.open_for_execution(unconfigured_classic)

        with pytest.raises(
            UnsupportedSparklessConfigurationError,
            match=fixture["profile_errors"]["unsupported_setting"]["message_fragment"],
        ):
            factory.open_for_execution(
                replace(
                    standard_context,
                    effective_settings_json=(
                        '{"spark.sql.ansi.enabled":"false",'
                        '"spark.sql.session.timeZone":"America/Los_Angeles",'
                        '"spark.sql.shuffle.partitions":"8"}'
                    ),
                )
            )
        with pytest.raises(
            UnsupportedSparklessConfigurationError,
            match=fixture["profile_errors"]["unsupported_setting"]["message_fragment"],
        ):
            factory.open_for_execution(
                replace(
                    serverless_context,
                    effective_settings_json=(
                        '{"spark.sql.ansi.enabled":"true",'
                        '"spark.sql.session.timeZone":"UTC",'
                        '"spark.sql.shuffle.partitions":"8"}'
                    ),
                )
            )

        connect = fixture["connect"]
        with pytest.raises(
            UnsupportedSparklessConfigurationError, match="unsupported Sparkless engine profile"
        ):
            SparklessSessionFactory(context, engine_profile=connect["engine_profile_request"])

    assert fixture["deferred_analysis"]["connect_parity_claim"] is False
    assert fixture["connect"]["remote_execution"] is False
    assert fixture["databricks_reference"]["remote_compatibility_claim"] is False


def test_session_use_and_close_are_owner_thread_bound(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        session = SparklessSessionFactory(context).open()
        try:
            errors: list[BaseException] = []

            def use_from_other_thread() -> None:
                try:
                    session.table("main.default.orders")
                except BaseException as error:
                    errors.append(error)

            worker = threading.Thread(target=use_from_other_thread)
            worker.start()
            worker.join()
            assert len(errors) == 1
            assert isinstance(errors[0], SparklessAdapterError)
        finally:
            session.close()
            assert session._native_session is None
        session.close()
