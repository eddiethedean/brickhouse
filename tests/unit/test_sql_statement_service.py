from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

from brickhouse.catalog.repository import (
    NamespaceAlreadyExistsError,
    TableAlreadyExistsError,
    TableNotFoundError,
)
from brickhouse.sql import (
    SqlOperation,
    SqlParseError,
    SqlStatementParameter,
    SqlStatementParameterError,
    SqlStatementPlanner,
    SqlStatementService,
    UnsupportedSqlStatementError,
)
from brickhouse.tables import (
    DeltaRsTableService,
    DeltaTableProviderError,
    PendingTableOperationError,
    TableHandle,
    TableMissingError,
    WriteMode,
)
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.lifecycle import initialize_workspace


def _seed_table(context: WorkspaceContext, name: str, data: pa.Table) -> TableHandle:
    registration = context.tables.create_managed("main", "default", name)
    handle = TableHandle.from_registration(registration)
    DeltaRsTableService(context).write_arrow(
        handle,
        data,
        WriteMode.CREATE,
        operation_id=f"seed-{name}",
    )
    return handle


def _orders() -> pa.Table:
    return pa.table(
        {
            "order_id": pa.array([1, 2, 3, 4], type=pa.int64()),
            "customer": pa.array(["alice", "alice", "bob", "bob"]),
            "amount": pa.array(
                [Decimal("10.50"), Decimal("5.25"), None, Decimal("20.00")],
                type=pa.decimal128(12, 2),
            ),
            "active": pa.array([True, True, True, False]),
        }
    )


def _b019_1_1_orders_fixture() -> tuple[dict[str, object], pa.Table]:
    fixture_path = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "sql"
        / "B019.1.1-orders-scalar-query.json"
    )
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    records = fixture["table"]["rows"]
    schema = pa.schema(
        [
            pa.field("order_id", pa.int64(), nullable=False),
            pa.field("customer", pa.string(), nullable=False),
            pa.field("amount", pa.decimal128(12, 2), nullable=True),
            pa.field("active", pa.bool_(), nullable=False),
            pa.field("event_date", pa.date32(), nullable=True),
            pa.field("event_ts", pa.timestamp("us", tz="UTC"), nullable=True),
            pa.field("item_ids", pa.list_(pa.int64()), nullable=True),
            pa.field(
                "details",
                pa.struct(
                    [
                        pa.field("coupon", pa.string(), nullable=True),
                        pa.field("tags", pa.list_(pa.string()), nullable=True),
                    ]
                ),
                nullable=True,
            ),
        ]
    )
    rows = [
        {
            **record,
            "amount": Decimal(record["amount"]) if record["amount"] is not None else None,
            "event_date": (
                date.fromisoformat(record["event_date"])
                if record["event_date"] is not None
                else None
            ),
            "event_ts": (
                datetime.fromisoformat(record["event_ts"]).astimezone(UTC)
                if record["event_ts"] is not None
                else None
            ),
        }
        for record in records
    ]
    return fixture, pa.Table.from_pylist(rows, schema=schema)


def test_select_projection_filter_aggregate_and_inner_equi_join_use_sparkless(
    tmp_path: Path,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        _seed_table(context, "orders", _orders())
        _seed_table(
            context,
            "segments",
            pa.table(
                {
                    "customer": pa.array(["alice", "bob"]),
                    "segment": pa.array(["silver", "gold"]),
                }
            ),
        )
        service = SqlStatementService(context)

        projected = service.execute(
            "SELECT order_id, amount FROM orders "
            "WHERE active = true AND amount IS NOT NULL ORDER BY order_id"
        ).data
        assert projected.to_pylist() == [
            {"order_id": 1, "amount": Decimal("10.50")},
            {"order_id": 2, "amount": Decimal("5.25")},
        ]

        aggregate = service.execute(
            "SELECT customer, COUNT(*) AS order_count, SUM(amount) AS total_amount "
            "FROM orders WHERE active = true AND amount IS NOT NULL GROUP BY customer"
        ).data
        assert aggregate.to_pylist() == [
            {"customer": "alice", "order_count": 2, "total_amount": Decimal("15.75")}
        ]

        joined = service.execute(
            "SELECT o.order_id, s.segment FROM orders o "
            "JOIN segments s ON o.customer = s.customer "
            "WHERE o.active = true ORDER BY o.order_id"
        ).data
        assert joined.to_pylist() == [
            {"order_id": 1, "segment": "silver"},
            {"order_id": 2, "segment": "silver"},
            {"order_id": 3, "segment": "gold"},
        ]


def test_join_aliases_bind_to_distinct_same_named_tables_across_schemas(
    tmp_path: Path,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        service = SqlStatementService(context)
        service.execute("CREATE SCHEMA other")
        sources = (
            ("default", pa.table({"id": pa.array([1], type=pa.int64()), "label": ["left"]})),
            ("other", pa.table({"id": pa.array([1], type=pa.int64()), "label": ["right"]})),
        )
        for schema_name, data in sources:
            registration = context.tables.create_managed("main", schema_name, "orders")
            DeltaRsTableService(context).write_arrow(
                TableHandle.from_registration(registration),
                data,
                WriteMode.CREATE,
                operation_id=f"alias-orders-{schema_name}",
            )

        result = service.execute(
            "SELECT l.label AS left_label, r.label AS right_label "
            "FROM main.default.orders AS l "
            "JOIN main.other.orders AS r ON l.id = r.id"
        ).data

        assert result.to_pylist() == [{"left_label": "left", "right_label": "right"}]


def test_select_limits_stream_join_results_before_arrow_materialization(
    tmp_path: Path,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    input_rows = pa.table({"id": pa.array([0] * 300, type=pa.int64())})
    with WorkspaceContext.local(workspace.path) as context:
        _seed_table(context, "left_rows", input_rows)
        _seed_table(context, "right_rows", input_rows)

        execution = SqlStatementService(context).execute(
            "SELECT l.id FROM left_rows AS l JOIN right_rows AS r ON l.id = r.id",
            row_limit=1,
            byte_limit=100,
        )

        assert execution.data.to_pylist() == [{"id": 0}]
        assert execution.total_row_count == 90_000
        assert execution.result_truncated is True


def test_insert_values_and_select_append_through_the_shared_delta_service(
    tmp_path: Path,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    target_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("label", pa.string(), nullable=True),
        ]
    )
    with WorkspaceContext.local(workspace.path) as context:
        target_registration = context.tables.create_managed("main", "default", "target")
        target = TableHandle.from_registration(target_registration)
        tables = DeltaRsTableService(context)
        tables.write_arrow(
            target,
            pa.Table.from_batches([], schema=target_schema),
            WriteMode.CREATE,
            operation_id="insert-target-create",
        )
        _seed_table(
            context,
            "source",
            pa.table(
                {
                    "id": pa.array([4], type=pa.int64()),
                    "label": pa.array(["four"], type=pa.string()),
                }
            ),
        )
        service = SqlStatementService(context)

        values_result = service.execute(
            "INSERT INTO target VALUES (1, 'one'), (2, 'two')",
            operation_id="insert-values",
        )
        parameter_result = service.execute(
            "INSERT INTO target (id, label) VALUES (:id, :label)",
            operation_id="insert-parameters",
            parameters=(
                SqlStatementParameter("id", "3", "BIGINT"),
                SqlStatementParameter("label", "three", "STRING"),
            ),
        )
        select_result = service.execute(
            "INSERT INTO target (label, id) SELECT label, id FROM source",
            operation_id="insert-select",
        )

        assert values_result.rows_affected == 2
        assert parameter_result.rows_affected == 1
        assert select_result.rows_affected == 1
        inserted = sorted(tables.read_table(target).data.to_pylist(), key=lambda row: row["id"])
        assert inserted == [
            {"id": 1, "label": "one"},
            {"id": 2, "label": "two"},
            {"id": 3, "label": "three"},
            {"id": 4, "label": "four"},
        ]

        version = tables.read_snapshot(target).version
        with pytest.raises(UnsupportedSqlStatementError, match="equal widths"):
            service.execute("INSERT INTO target VALUES (5, 'five'), (6)")
        assert tables.read_snapshot(target).version == version


def test_b019_1_1_versioned_scalar_projection_filter_null_and_coercion(
    tmp_path: Path,
) -> None:
    fixture, orders = _b019_1_1_orders_fixture()
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        registration = context.tables.create_managed("main", "default", "orders")
        DeltaRsTableService(context).write_arrow(
            TableHandle.from_registration(registration),
            orders,
            WriteMode.CREATE,
            operation_id="b019-1-1-seed-orders",
        )

        result = SqlStatementService(context).execute(fixture["scalar_query"]["sql"]).data

        expected_schema = pa.schema(
            [
                pa.field("order_id", pa.int64()),
                pa.field("amount", pa.decimal128(12, 2)),
                pa.field("active", pa.bool_()),
                pa.field("amount_missing", pa.bool_()),
                pa.field("order_as_double", pa.float64()),
            ]
        )
        expected_rows = [
            {
                **row,
                "amount": Decimal(row["amount"]) if row["amount"] is not None else None,
            }
            for row in fixture["scalar_query"]["expected_rows"]
        ]

        assert result.schema == expected_schema
        assert result.to_pylist() == expected_rows
        assert fixture["databricks_reference"]["outcome"] == "not_accepted"
        assert fixture["databricks_reference"]["warehouse_restored_to_stopped"] is True


def test_b019_1_2_versioned_grouped_empty_and_invalid_cast_aggregates(
    tmp_path: Path,
) -> None:
    _, orders = _b019_1_1_orders_fixture()
    aggregate_fixture_path = (
        Path(__file__).resolve().parents[1] / "fixtures" / "sql" / "B019.1.2-aggregate-cases.json"
    )
    aggregate_fixture = json.loads(aggregate_fixture_path.read_text(encoding="utf-8"))
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        registration = context.tables.create_managed("main", "default", "orders")
        DeltaRsTableService(context).write_arrow(
            TableHandle.from_registration(registration),
            orders,
            WriteMode.CREATE,
            operation_id="b019-1-2-seed-orders",
        )
        service = SqlStatementService(context)

        grouped_case = aggregate_fixture["aggregate_queries"]["grouped"]
        grouped = service.execute(grouped_case["sql"]).data
        assert grouped.schema == pa.schema(
            [
                pa.field("customer", pa.string()),
                pa.field("order_count", pa.int32()),
                pa.field("amount_count", pa.int32()),
                pa.field("total_amount", pa.decimal128(22, 2)),
            ]
        )
        assert grouped.to_pylist() == [
            {
                **row,
                "total_amount": (
                    Decimal(row["total_amount"]) if row["total_amount"] is not None else None
                ),
            }
            for row in grouped_case["expected_rows"]
        ]

        empty_case = aggregate_fixture["aggregate_queries"]["empty_input"]
        empty = service.execute(empty_case["sql"]).data
        assert empty.schema == pa.schema(
            [
                pa.field("order_count", pa.int32()),
                pa.field("total_amount", pa.decimal128(22, 2)),
            ]
        )
        assert empty.to_pylist() == empty_case["expected_rows"]

        invalid_cast = aggregate_fixture["invalid_cast"]
        with pytest.raises(ValueError, match=invalid_cast["expected_message_fragment"]):
            service.execute(invalid_cast["sql"])

    assert aggregate_fixture["databricks_reference"]["outcome"] == "not_accepted"
    assert aggregate_fixture["databricks_reference"]["warehouse_restored_to_stopped"] is True
    assert aggregate_fixture["databricks_reference"]["objects_created"] is False


def test_b019_1_3_versioned_date_timestamp_timezone_and_error_cases(
    tmp_path: Path,
) -> None:
    _, orders = _b019_1_1_orders_fixture()
    fixture_path = (
        Path(__file__).resolve().parents[1] / "fixtures" / "sql" / "B019.1.3-date-time-cases.json"
    )
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        registration = context.tables.create_managed("main", "default", "orders")
        DeltaRsTableService(context).write_arrow(
            TableHandle.from_registration(registration),
            orders,
            WriteMode.CREATE,
            operation_id="b019-1-3-seed-orders",
        )
        service = SqlStatementService(context)

        projected = service.execute(fixture["projection_and_date_predicate"]["sql"]).data
        assert projected.schema == pa.schema(
            [
                pa.field("order_id", pa.int64()),
                pa.field("event_date", pa.date32()),
                pa.field("event_ts", pa.timestamp("us")),
                pa.field("on_oct_17", pa.bool_()),
            ]
        )
        assert projected.to_pylist() == [
            {
                **row,
                "event_date": (
                    date.fromisoformat(row["event_date"]) if row["event_date"] is not None else None
                ),
                "event_ts": (
                    datetime.fromisoformat(row["event_ts"]) if row["event_ts"] is not None else None
                ),
            }
            for row in fixture["projection_and_date_predicate"]["expected_rows"]
        ]

        literals = service.execute(fixture["date_timestamp_literals"]["sql"]).data
        assert literals.schema == pa.schema(
            [
                pa.field("day", pa.date32()),
                pa.field("instant", pa.timestamp("us")),
            ]
        )
        assert literals.to_pylist() == [
            {
                "day": date.fromisoformat("2026-10-17"),
                "instant": datetime.fromisoformat("2026-10-18T03:30:00"),
            }
        ]

        boundary = service.execute(fixture["timestamp_boundary"]["sql"]).data
        assert boundary.schema == pa.schema(
            [
                pa.field("order_id", pa.int64()),
                pa.field("at_or_after", pa.bool_()),
                pa.field("before_boundary", pa.bool_()),
            ]
        )
        assert boundary.to_pylist() == fixture["timestamp_boundary"]["expected_rows"]

        direct_case = fixture["direct_timestamp_predicate"]
        direct = service.execute(direct_case["sql"]).data
        assert direct.schema == pa.schema([pa.field("order_id", pa.int64())])
        assert direct.to_pylist() == direct_case["expected_rows"]

        for invalid_case in fixture["invalid_conversions"]:
            with pytest.raises(
                ValueError,
                match=invalid_case["expected_message_fragment"],
            ):
                service.execute(invalid_case["sql"])

    assert fixture["databricks_reference"]["remote_compatibility_claim"] is False


def test_b019_1_4_versioned_nested_array_struct_projection_and_rejections(
    tmp_path: Path,
) -> None:
    _, orders = _b019_1_1_orders_fixture()
    fixture_path = (
        Path(__file__).resolve().parents[1] / "fixtures" / "sql" / "B019.1.4-collection-cases.json"
    )
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        registration = context.tables.create_managed("main", "default", "orders")
        DeltaRsTableService(context).write_arrow(
            TableHandle.from_registration(registration),
            orders,
            WriteMode.CREATE,
            operation_id="b019-1-4-seed-orders",
        )
        service = SqlStatementService(context)

        projection_case = fixture["nested_projection"]
        projected = service.execute(projection_case["sql"]).data
        assert projected.schema == pa.schema(
            [
                pa.field("order_id", pa.int64()),
                pa.field("item_ids", pa.list_(pa.int64())),
                pa.field(
                    "details",
                    pa.struct(
                        [
                            pa.field("coupon", pa.string()),
                            pa.field("tags", pa.list_(pa.string())),
                        ]
                    ),
                ),
            ]
        )
        assert projected.to_pylist() == projection_case["expected_rows"]

        nested_field_case = fixture["nested_field_projection"]
        nested_field = service.execute(nested_field_case["sql"]).data
        assert nested_field.schema == pa.schema([pa.field("coupon", pa.string(), nullable=True)])
        assert nested_field.to_pylist() == nested_field_case["expected_rows"]

        from sparkless.sql import SparklessError

        for unsupported_case in fixture["unsupported_sql_cases"]:
            with pytest.raises(SparklessError) as error:
                service.execute(unsupported_case["sql"])
            fragments = unsupported_case.get("expected_message_fragments")
            if fragments is None:
                fragments = [unsupported_case["expected_message_fragment"]]
            for fragment in fragments:
                assert fragment in str(error.value)

    assert fixture["databricks_reference"]["remote_compatibility_claim"] is False


def test_b019_1_5_versioned_inner_join_null_keys_aliases_and_rejections(
    tmp_path: Path,
) -> None:
    fixture_path = (
        Path(__file__).resolve().parents[1] / "fixtures" / "sql" / "B019.1.5-join-cases.json"
    )
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    tables = fixture["tables"]
    orders = pa.Table.from_pylist(
        tables["orders"]["rows"],
        schema=pa.schema(
            [
                pa.field("order_id", pa.int64(), nullable=False),
                pa.field("customer", pa.string(), nullable=True),
            ]
        ),
    )
    segments = pa.Table.from_pylist(
        tables["segments"]["rows"],
        schema=pa.schema(
            [
                pa.field("customer", pa.string(), nullable=True),
                pa.field("segment", pa.string(), nullable=False),
            ]
        ),
    )
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        for name, data in (("orders", orders), ("segments", segments)):
            registration = context.tables.create_managed("main", "default", name)
            DeltaRsTableService(context).write_arrow(
                TableHandle.from_registration(registration),
                data,
                WriteMode.CREATE,
                operation_id=f"b019-1-5-seed-{name}",
            )
        service = SqlStatementService(context)

        joined = service.execute(fixture["inner_equi_join"]["sql"]).data
        assert joined.schema == pa.schema(
            [
                pa.field("order_id", pa.int64()),
                pa.field("segment", pa.string()),
                pa.field("customer", pa.string()),
            ]
        )
        assert joined.to_pylist() == fixture["inner_equi_join"]["expected_rows"]

        for unsupported_case in fixture["unsupported_joins"]:
            with pytest.raises(UnsupportedSqlStatementError) as error:
                service.execute(unsupported_case["sql"])
            assert unsupported_case["expected_message_fragment"] in str(error.value)

    assert fixture["databricks_reference"]["remote_compatibility_claim"] is False


def test_b019_1_6_versioned_ddl_errors_and_planning_side_effects(tmp_path: Path) -> None:
    fixture_path = (
        Path(__file__).resolve().parents[1] / "fixtures" / "sql" / "B019.1.6-ddl-error-cases.json"
    )
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    _, orders = _b019_1_1_orders_fixture()
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        _seed_table(context, "orders", orders)
        service = SqlStatementService(context)

        create_schema = fixture["ddl"]["create_schema"]
        assert service.execute(create_schema["sql"]).object_name == create_schema["object_name"]
        assert sorted(schema.name for schema in context.schemas.list("main")) == sorted(
            create_schema["expected_schemas"]
        )

        create_table = fixture["ddl"]["create_empty_table"]
        created = service.execute(create_table["sql"])
        assert created.object_name == create_table["object_name"]
        assert created.commit is not None
        assert created.commit.version == create_table["expected_delta_version"]
        empty_registration = context.tables.get("main", "analytics", "metrics")
        empty = (
            DeltaRsTableService(context)
            .read_table(TableHandle.from_registration(empty_registration))
            .data
        )
        assert empty.schema == pa.schema(
            [pa.field("id", pa.int64()), pa.field("amount", pa.decimal128(12, 2))]
        )
        assert empty.to_pylist() == create_table["expected_rows"]

        ctas_case = fixture["ddl"]["ctas"]
        ctas = service.execute(ctas_case["sql"], operation_id="b019-1-6-ctas")
        assert ctas.object_name == ctas_case["object_name"]
        assert ctas.commit is not None
        assert ctas.commit.version == ctas_case["expected_delta_version"]
        ctas_registration = context.tables.get("main", "default", "active_orders")
        ctas_data = (
            DeltaRsTableService(context)
            .read_table(TableHandle.from_registration(ctas_registration))
            .data
        )
        assert ctas_data.schema == pa.schema(
            [
                pa.field("order_id", pa.int64()),
                pa.field("customer", pa.string()),
                pa.field("amount", pa.decimal128(12, 2)),
            ]
        )
        assert ctas_data.to_pylist() == [
            {
                **row,
                "amount": Decimal(row["amount"]) if row["amount"] is not None else None,
            }
            for row in ctas_case["expected_rows"]
        ]

        drop_if_exists = fixture["ddl"]["drop_if_exists"]
        noop = service.execute(drop_if_exists["sql"])
        assert noop.object_name == drop_if_exists["object_name"]
        assert noop.no_op is drop_if_exists["no_op"]

        source_registration = context.tables.get("main", "default", "orders")
        source_handle = TableHandle.from_registration(source_registration)
        delta_service = DeltaRsTableService(context)
        source_before = delta_service.read_table(source_handle)
        schemas_before = sorted(schema.name for schema in context.schemas.list("main"))
        tables_before = sorted(
            (table.schema_name, table.name, table.table_id)
            for schema in schemas_before
            for table in context.tables.list("main", schema)
        )
        error_types = {
            "TableAlreadyExistsError": TableAlreadyExistsError,
            "NamespaceAlreadyExistsError": NamespaceAlreadyExistsError,
            "TableNotFoundError": TableNotFoundError,
            "UnsupportedSqlStatementError": UnsupportedSqlStatementError,
            "SqlParseError": SqlParseError,
        }
        for error_case in fixture["typed_errors"]:
            expected_type = error_types[error_case["expected_error_type"]]
            with pytest.raises(expected_type) as error:
                service.execute(error_case["sql"])
            fragment = error_case.get("expected_message_fragment")
            if fragment is not None:
                assert fragment in str(error.value)
            assert sorted(schema.name for schema in context.schemas.list("main")) == schemas_before
            assert (
                sorted(
                    (table.schema_name, table.name, table.table_id)
                    for schema in schemas_before
                    for table in context.tables.list("main", schema)
                )
                == tables_before
            )
            source_after = delta_service.read_table(source_handle)
            assert source_after.snapshot.version == source_before.snapshot.version
            assert source_after.data.schema == source_before.data.schema
            assert source_after.data.to_pylist() == source_before.data.to_pylist()

        drop_table = fixture["ddl"]["drop_table"]
        assert service.execute(drop_table["sql"]).object_name == drop_table["object_name"]
        with pytest.raises(TableNotFoundError):
            context.tables.get("main", "analytics", "metrics")

    assert fixture["databricks_reference"]["remote_compatibility_claim"] is False


def test_schema_and_managed_delta_table_ddl_share_catalog_and_table_services(
    tmp_path: Path,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        service = SqlStatementService(context)
        created_schema = service.execute("CREATE SCHEMA analytics")
        assert created_schema.object_name == "main.analytics"

        created_table = service.execute(
            "CREATE TABLE analytics.metrics (id BIGINT, amount DECIMAL(12, 2)) USING DELTA",
            operation_id="create-metrics",
        )
        assert created_table.commit is not None
        assert created_table.commit.version == 0
        registration = context.tables.get("main", "analytics", "metrics")
        readback = DeltaRsTableService(context).read_table(
            TableHandle.from_registration(registration)
        )
        assert readback.data.num_rows == 0
        assert readback.data.schema == pa.schema(
            [pa.field("id", pa.int64()), pa.field("amount", pa.decimal128(12, 2))]
        )

        assert (
            service.execute("DROP TABLE analytics.metrics").object_name == "main.analytics.metrics"
        )
        with pytest.raises(TableNotFoundError):
            context.tables.get("main", "analytics", "metrics")
        assert service.execute("DROP SCHEMA analytics").object_name == "main.analytics"


def test_ctas_commits_delta_and_is_visible_to_a_fresh_sparkless_session(tmp_path: Path) -> None:
    from brickhouse.engines.sparkless import SparklessSessionFactory

    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        _seed_table(context, "orders", _orders())
        result = SqlStatementService(context).execute(
            "CREATE TABLE active_orders USING DELTA AS "
            "SELECT order_id, customer, amount FROM orders WHERE active = true",
            operation_id="create-active-orders",
        )
        assert result.commit is not None
        assert result.commit.version == 0

        with SparklessSessionFactory(context).open() as session:
            rows = session.table("main.default.active_orders").sort("order_id").collect()
        assert rows == [
            (1, "alice", Decimal("10.50")),
            (2, "alice", Decimal("5.25")),
            (3, "bob", None),
        ]


def test_planner_binds_registered_table_names_without_changing_catalog_state(
    tmp_path: Path,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        _seed_table(context, "orders", _orders())
        planner = SqlStatementPlanner(context)
        plan = planner.plan("SELECT order_id FROM main.default.ORDERS")
        assert plan.operation is SqlOperation.SELECT
        assert len(plan.source_tables) == 1
        assert plan.source_tables[0].table.full_name == "main.default.orders"
        assert "`main`.`default`.`orders`" in (plan.rendered_sql or "")
        assert context.new_namespace_state().current_schema == "default"


def test_self_join_is_rejected_after_both_sources_are_bound(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        _seed_table(context, "orders", _orders())
        with pytest.raises(UnsupportedSqlStatementError, match="self-joins"):
            SqlStatementService(context).execute(
                "SELECT left_order.order_id FROM orders left_order "
                "JOIN orders right_order "
                "ON left_order.order_id = right_order.order_id"
            )


@pytest.mark.parametrize(
    "statement",
    [
        "DELETE FROM main.default.orders",
        "SELECT read_files('/tmp/secret')",
        "SELECT * FROM range(10)",
        "SELECT * FROM missing_table",
        "SELECT ? FROM main.default.orders",
        "SELECT * FROM main.default.orders TABLESAMPLE (10 PERCENT)",
        "WITH selected AS (SELECT * FROM main.default.orders) SELECT * FROM selected",
    ],
)
def test_unsupported_or_unbound_queries_are_rejected_before_execution(
    tmp_path: Path,
    statement: str,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        service = SqlStatementService(context)
        with pytest.raises(
            (SqlStatementParameterError, UnsupportedSqlStatementError, TableNotFoundError)
        ):
            service.execute(statement)
        assert context.tables.list("main", "default") == []


def test_unsupported_ddl_and_multiple_statements_have_no_side_effects(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        service = SqlStatementService(context)
        with pytest.raises(UnsupportedSqlStatementError, match="USING DELTA"):
            service.execute("CREATE TABLE bad USING PARQUET AS SELECT 1 AS id")
        with pytest.raises(SqlParseError, match="exactly one"):
            service.execute("CREATE SCHEMA one; CREATE SCHEMA two")
        assert {schema.name for schema in context.schemas.list("main")} == {
            "default",
            "information_schema",
        }
        assert context.tables.list("main", "default") == []


@pytest.mark.parametrize(
    "statement",
    [
        "VACUUM orders",
        "OPTIMIZE orders",
        "ANALYZE TABLE orders COMPUTE STATISTICS",
        "SHOW TABLES",
        "EXPLAIN SELECT * FROM orders",
    ],
)
def test_unselected_sql_utilities_reject_without_table_side_effects(
    tmp_path: Path,
    statement: str,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        table = _seed_table(context, "orders", _orders())
        service = SqlStatementService(context)
        provider = DeltaRsTableService(context)
        version_before = provider.read_snapshot(table).version
        registrations_before = context.tables.list("main", "default")

        with pytest.raises(UnsupportedSqlStatementError):
            service.execute(statement)

        assert context.tables.list("main", "default") == registrations_before
        assert provider.read_snapshot(table).version == version_before


def test_if_not_exists_preserves_existing_table_and_duplicate_ddl_is_typed(
    tmp_path: Path,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        table = _seed_table(context, "orders", _orders())
        service = SqlStatementService(context)
        provider = DeltaRsTableService(context)
        before = provider.read_table(table)
        noop = service.execute("CREATE TABLE IF NOT EXISTS ORDERS USING DELTA AS SELECT 1 AS id")
        assert noop.no_op
        after = provider.read_table(table)
        assert after.snapshot.version == before.snapshot.version
        assert after.data.equals(before.data)
        with pytest.raises(TableAlreadyExistsError):
            service.execute("CREATE TABLE orders USING DELTA AS SELECT 1 AS id")
        with pytest.raises(NamespaceAlreadyExistsError):
            service.execute("CREATE SCHEMA default")
        assert len(context.tables.list("main", "default")) == 1


def test_failed_initial_create_removes_registration_when_provider_rejects_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:

        def reject_write(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("provider rejected first write")

        monkeypatch.setattr(DeltaRsTableService, "write_arrow", reject_write)
        service = SqlStatementService(context)

        with pytest.raises(RuntimeError, match="provider rejected first write"):
            service.execute("CREATE TABLE rejected (id BIGINT) USING DELTA")

        assert context.tables.list("main", "default") == []


def test_create_retries_after_interruption_left_managed_registration_without_delta_log(
    tmp_path: Path,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        interrupted = context.tables.create_managed("main", "default", "interrupted")
        interrupted_location = interrupted.canonical_location
        assert not (interrupted_location / "_delta_log").exists()

        result = SqlStatementService(context).execute(
            "CREATE TABLE interrupted (id BIGINT) USING DELTA"
        )

        assert result.commit is not None
        assert result.commit.version == 0
        registration = context.tables.get("main", "default", "interrupted")
        recovered = TableHandle.from_registration(registration)
        snapshot = DeltaRsTableService(context).read_table(recovered)
        assert snapshot.data.schema.field("id").type == pa.int64()
        assert snapshot.data.num_rows == 0


def test_if_not_exists_rejects_external_table_without_removing_its_data(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    external_location = tmp_path / "external"
    external_location.mkdir()
    with WorkspaceContext.local(workspace.path) as context:
        registration = context.tables.register_external(
            "main", "default", "external_orders", external_location
        )
        table = TableHandle.from_registration(registration)
        provider = DeltaRsTableService(context)
        provider.write_arrow(
            table,
            pa.table({"id": pa.array([7], type=pa.int64())}),
            WriteMode.CREATE,
            operation_id="external-create",
        )
        before = provider.read_table(table)

        with pytest.raises(TableAlreadyExistsError, match="externally owned"):
            SqlStatementService(context).execute(
                "CREATE TABLE IF NOT EXISTS external_orders USING DELTA AS SELECT 1 AS id"
            )

        retained = context.tables.get("main", "default", "external_orders")
        assert retained.canonical_location == external_location.resolve()
        after = provider.read_table(TableHandle.from_registration(retained))
        assert after.snapshot.version == before.snapshot.version
        assert after.data.to_pylist() == [{"id": 7}]


def test_if_not_exists_preserves_malformed_managed_delta_log(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        registration = context.tables.create_managed("main", "default", "malformed")
        table = TableHandle.from_registration(registration)
        delta_log = registration.canonical_location / "_delta_log"
        delta_log.mkdir(parents=True)
        malformed_json = delta_log / "00000000000000000000.json"
        malformed_json.write_text("{not valid json", encoding="utf-8")

        with pytest.raises((TableMissingError, DeltaTableProviderError)):
            SqlStatementService(context).execute(
                "CREATE TABLE IF NOT EXISTS malformed USING DELTA AS SELECT 1 AS id"
            )

        assert context.tables.get("main", "default", "malformed").table_id == table.table_id
        assert malformed_json.read_text(encoding="utf-8") == "{not valid json"


def test_if_not_exists_preserves_unresolved_managed_create_operation(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        registration = context.tables.create_managed("main", "default", "unresolved")
        table = TableHandle.from_registration(registration)
        context.table_operations.prepare(
            table_id=table.table_id,
            operation_id="unresolved-create",
            fingerprint="f" * 64,
            mode=WriteMode.CREATE.value,
            schema_fingerprint="s" * 64,
            input_sha256="i" * 64,
            base_version=0,
            row_count=1,
        )
        context.table_operations.mark_committing(table.table_id, "unresolved-create")
        context.table_operations.mark_uncertain(table.table_id, "unresolved-create")

        with pytest.raises(PendingTableOperationError, match="unresolved-create"):
            SqlStatementService(context).execute(
                "CREATE TABLE IF NOT EXISTS unresolved USING DELTA AS SELECT 1 AS id"
            )

        retained = context.tables.get("main", "default", "unresolved")
        assert retained.table_id == table.table_id
        assert not (retained.canonical_location / "_delta_log").exists()
        assert context.table_operations.latest_unfinished(table.table_id) is not None


def test_ctas_query_execution_errors_do_not_register_target(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        service = SqlStatementService(context)

        def fail_query(_query: str, **_kwargs: object) -> pa.Table:
            raise RuntimeError("Sparkless execution failed")

        service._execute_query = fail_query  # type: ignore[method-assign]
        with pytest.raises(RuntimeError, match="Sparkless execution failed"):
            service.execute("CREATE TABLE failed_ctas USING DELTA AS SELECT 1 AS id")
        assert context.tables.list("main", "default") == []


def test_drop_if_exists_missing_namespace_is_a_noop(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        result = SqlStatementService(context).execute(
            "DROP TABLE IF EXISTS nonexistent.schema.table"
        )
        assert result.no_op
        assert result.object_name == "nonexistent.schema.table"
