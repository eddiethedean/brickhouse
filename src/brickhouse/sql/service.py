"""Execution routing for the bounded local SQL profile."""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass, replace
from typing import Any

from sqlglot import exp

from brickhouse.catalog import TableOwnership
from brickhouse.catalog.identifiers import NamespaceState
from brickhouse.catalog.repository import TableAlreadyExistsError, TableNotFoundError
from brickhouse.catalog.service import CatalogService
from brickhouse.sql.cancellation import SqlStatementCancelledError, SqlStatementControl
from brickhouse.sql.errors import UnsupportedSqlStatementError
from brickhouse.sql.parameters import SqlStatementParameter
from brickhouse.sql.planner import (
    SqlObjectName,
    SqlOperation,
    SqlStatementPlan,
    SqlStatementPlanner,
)
from brickhouse.sql.render import render_sparkless_sql
from brickhouse.tables.contracts import (
    SnapshotSelector,
    TableHandle,
    TableHistoryEntry,
    TableMissingError,
    WriteMode,
)
from brickhouse.workspace.context import WorkspaceContext


@dataclass(frozen=True, slots=True)
class SqlExecutionResult:
    """Actual query data or DDL effect produced by one local statement."""

    operation: SqlOperation
    data: Any | None = None
    object_name: str | None = None
    commit: Any | None = None
    rows_affected: int | None = None
    no_op: bool = False
    total_row_count: int | None = None
    result_truncated: bool = False


class SqlStatementService:
    """Route approved SQL ASTs to Sparkless, catalog, and the Delta table service."""

    def __init__(
        self,
        context: WorkspaceContext,
        *,
        namespace: NamespaceState | None = None,
    ) -> None:
        self._context = context
        self._namespace = namespace or context.new_namespace_state()
        self._planner = SqlStatementPlanner(context, self._namespace)
        self._catalog = CatalogService(context)

    def execute(
        self,
        statement: str,
        *,
        operation_id: str | None = None,
        parameters: tuple[SqlStatementParameter, ...] = (),
        cancellation: SqlStatementControl | None = None,
        row_limit: int | None = None,
        byte_limit: int | None = None,
    ) -> SqlExecutionResult:
        """Plan before effects, then route only to the service selected by its AST."""
        if cancellation is not None:
            cancellation.mark_preparing()
        plan = self._planner.plan(statement, parameters=parameters)
        if plan.operation is SqlOperation.CREATE_TABLE and plan.target_exists:
            existing_result = self._handle_existing_create_table(plan)
            if existing_result is not None:
                return existing_result
            # A managed registration without a Delta table is a recoverable
            # remnant of an interrupted CREATE. The old registration has been
            # removed, so continue with the validated create plan.
            plan = replace(plan, target_exists=False, no_op=False)
        if plan.no_op:
            return SqlExecutionResult(
                operation=plan.operation,
                object_name=_target_name(plan),
                no_op=True,
            )
        if plan.operation in {SqlOperation.DESCRIBE_TABLE, SqlOperation.DESCRIBE_HISTORY}:
            return self._describe_table(plan)
        if plan.operation is SqlOperation.SELECT:
            from brickhouse.sql.results import BoundedArrowResult

            bounded = self._execute_planned_query(
                plan,
                _required_sql(plan),
                cancellation=cancellation,
                commit_follows=False,
                bounded_result=True,
                row_limit=row_limit,
                byte_limit=byte_limit,
            )
            if not isinstance(bounded, BoundedArrowResult):
                raise RuntimeError("bounded SQL result collection returned an invalid result")
            return SqlExecutionResult(
                operation=plan.operation,
                data=bounded.table,
                total_row_count=bounded.total_row_count,
                result_truncated=bounded.truncated,
            )
        if plan.operation is SqlOperation.CREATE_SCHEMA:
            if cancellation is not None:
                cancellation.begin_commit()
            target = _required_schema_target(plan)
            self._catalog.create_schema(target.catalog_name, target.schema_name)
            return SqlExecutionResult(plan.operation, object_name=target.full_name)
        if plan.operation is SqlOperation.DROP_SCHEMA:
            if cancellation is not None:
                cancellation.begin_commit()
            target = _required_schema_target(plan)
            self._catalog.delete_schema(target.catalog_name, target.schema_name)
            return SqlExecutionResult(plan.operation, object_name=target.full_name)
        if plan.operation is SqlOperation.CREATE_TABLE:
            return self._create_table(
                plan,
                operation_id=operation_id,
                cancellation=cancellation,
            )
        if plan.operation is SqlOperation.INSERT:
            return self._insert_table(
                plan,
                operation_id=operation_id,
                cancellation=cancellation,
            )
        if plan.operation is SqlOperation.DROP_TABLE:
            if cancellation is not None:
                cancellation.begin_commit()
            return self._drop_table(plan)
        if plan.operation is SqlOperation.MERGE:
            return self._merge_table(
                plan,
                operation_id=operation_id,
                cancellation=cancellation,
            )
        if plan.operation in {SqlOperation.UPDATE, SqlOperation.DELETE}:
            return self._mutate_table(
                plan,
                operation_id=operation_id,
                cancellation=cancellation,
            )
        raise UnsupportedSqlStatementError(
            f"SQL operation is not routed by the local profile: {plan.operation}"
        )

    def _describe_table(self, plan: SqlStatementPlan) -> SqlExecutionResult:
        target = _required_table_target(plan)
        registration = self._context.tables.get(
            target.catalog_name,
            target.schema_name,
            target.object_name,
        )
        table = TableHandle.from_registration(registration)
        from brickhouse.tables.delta_rs import DeltaRsTableService

        service = DeltaRsTableService(self._context)
        if plan.operation is SqlOperation.DESCRIBE_TABLE:
            snapshot = service.read_snapshot(table)
            data = _describe_schema(snapshot.schema.json_text)
        elif plan.operation is SqlOperation.DESCRIBE_HISTORY:
            data = _describe_history(service.history(table))
        else:
            raise UnsupportedSqlStatementError("DESCRIBE plan has an unsupported operation")
        return SqlExecutionResult(
            operation=plan.operation,
            data=data,
            object_name=target.full_name,
        )

    def _create_table(
        self,
        plan: SqlStatementPlan,
        *,
        operation_id: str | None,
        cancellation: SqlStatementControl | None,
    ) -> SqlExecutionResult:
        target = _required_table_target(plan)
        expression = plan.expression
        if not isinstance(expression, exp.Create):
            raise UnsupportedSqlStatementError("CREATE TABLE plan lost its validated AST")
        query = expression.args.get("expression")
        if query is not None:
            if not isinstance(query, exp.Select):
                raise UnsupportedSqlStatementError("CREATE TABLE AS requires a SELECT query")
            data = self._execute_planned_query(
                plan,
                render_sparkless_sql(query),
                cancellation=cancellation,
                commit_follows=True,
            )
        else:
            data = _empty_table_data(plan.create_columns)

        if cancellation is not None:
            cancellation.begin_commit()

        from brickhouse.tables.delta_rs import DeltaRsTableService

        table_service = DeltaRsTableService(self._context)
        registration = self._context.tables.create_managed(
            target.catalog_name,
            target.schema_name,
            target.object_name,
        )
        table = TableHandle.from_registration(registration)
        try:
            commit = table_service.write_arrow(
                table,
                data,
                WriteMode.CREATE,
                operation_id=operation_id or f"sql-{uuid.uuid4().hex}",
            )
        except BaseException:
            # Remove a newly registered managed table only when the provider
            # confirms there is no Delta snapshot. drop_table also refuses to
            # remove registrations with unresolved journal operations.
            try:
                table_service.read_snapshot(table)
            except TableMissingError:
                if not (table.location / "_delta_log").exists():
                    try:
                        table_service.drop_table(table)
                    except Exception:
                        pass
            except Exception:
                pass
            raise
        return SqlExecutionResult(
            operation=plan.operation,
            object_name=target.full_name,
            commit=commit,
        )

    def _handle_existing_create_table(
        self,
        plan: SqlStatementPlan,
    ) -> SqlExecutionResult | None:
        """Validate an existing registration or recover an unmaterialized managed CREATE."""
        target = _required_table_target(plan)
        try:
            registration = self._context.tables.get(
                target.catalog_name,
                target.schema_name,
                target.object_name,
            )
        except TableNotFoundError:
            # The target was removed after planning; proceed with normal create.
            return None
        table = TableHandle.from_registration(registration)
        from brickhouse.tables.delta_rs import DeltaRsTableService

        table_service = DeltaRsTableService(self._context)
        try:
            table_service.read_snapshot(table)
        except TableMissingError:
            if registration.ownership is not TableOwnership.MANAGED:
                raise
            if (table.location / "_delta_log").exists():
                # Preserve a malformed or otherwise unopenable Delta log for
                # explicit diagnosis; it is not a safe interrupted-create remnant.
                raise
            # An unresolved provider commit is retained by drop_table as an
            # explicit blocker. Otherwise managed data at this location is
            # owned by BrickHouse and can be removed with its stale registration.
            table_service.drop_table(table)
            return None

        if registration.ownership is not TableOwnership.MANAGED:
            raise TableAlreadyExistsError(
                f"table already exists and is externally owned: {target.full_name}"
            )
        if plan.create_if_missing:
            return SqlExecutionResult(
                operation=plan.operation,
                object_name=target.full_name,
                no_op=True,
            )
        raise TableAlreadyExistsError(f"table already exists: {target.full_name}")

    def _insert_table(
        self,
        plan: SqlStatementPlan,
        *,
        operation_id: str | None,
        cancellation: SqlStatementControl | None,
    ) -> SqlExecutionResult:
        target = _required_table_target(plan)
        registration = self._context.tables.get(
            target.catalog_name,
            target.schema_name,
            target.object_name,
        )
        table = TableHandle.from_registration(registration)
        from brickhouse.sql.results import BoundedArrowResult
        from brickhouse.tables.delta_rs import DeltaRsTableService

        table_service = DeltaRsTableService(self._context)
        snapshot, target_schema = table_service.read_arrow_schema(table)
        source_query = plan.rendered_sql
        if source_query is None:
            raise UnsupportedSqlStatementError("INSERT plan has no validated source query")
        source = plan.expression.args.get("expression")
        if not isinstance(source, (exp.Select, exp.Values)):
            raise UnsupportedSqlStatementError("INSERT plan lost its validated source")
        if isinstance(source, exp.Select):
            input_result = self._execute_planned_query(
                plan,
                source_query,
                cancellation=cancellation,
                commit_follows=True,
                bounded_result=True,
            )
        else:
            input_result = self._execute_query(
                source_query,
                cancellation=cancellation,
                commit_follows=True,
                bounded_result=True,
            )
        if not isinstance(input_result, BoundedArrowResult):
            raise RuntimeError("bounded INSERT source collection returned an invalid result")
        input_data = _align_insert_columns(input_result.table, target_schema, plan.insert_columns)
        if cancellation is not None:
            cancellation.begin_commit()
        commit = table_service.write_arrow(
            table,
            input_data,
            WriteMode.APPEND,
            operation_id=operation_id or f"sql-{uuid.uuid4().hex}",
            expected_version=snapshot.version,
        )
        return SqlExecutionResult(
            operation=plan.operation,
            data=_empty_result_table(),
            object_name=target.full_name,
            commit=commit,
            rows_affected=commit.rows_written,
        )

    def _drop_table(self, plan: SqlStatementPlan) -> SqlExecutionResult:
        target = _required_table_target(plan)
        from brickhouse.tables.delta_rs import DeltaRsTableService

        registration = self._context.tables.get(
            target.catalog_name,
            target.schema_name,
            target.object_name,
        )
        service = DeltaRsTableService(self._context)
        table = TableHandle.from_registration(registration)
        service.drop_table(table)
        return SqlExecutionResult(plan.operation, object_name=target.full_name)

    def _mutate_table(
        self,
        plan: SqlStatementPlan,
        *,
        operation_id: str | None,
        cancellation: SqlStatementControl | None,
    ) -> SqlExecutionResult:
        target = _required_table_target(plan)
        registration = self._context.tables.get(
            target.catalog_name,
            target.schema_name,
            target.object_name,
        )
        table = TableHandle.from_registration(registration)
        from brickhouse.tables.delta_rs import DeltaRsTableService

        service = DeltaRsTableService(self._context)
        snapshot = service.read_snapshot(table)
        field_names = snapshot.schema.field_names
        expression = plan.expression
        where = expression.args.get("where")
        if not isinstance(where, exp.Where):
            raise UnsupportedSqlStatementError("SQL mutation plan has no validated predicate")
        predicate = _render_provider_expression(where.this, field_names)
        selected_operation_id = operation_id or f"sql-{uuid.uuid4().hex}"

        if plan.operation is SqlOperation.UPDATE:
            if not isinstance(expression, exp.Update):
                raise UnsupportedSqlStatementError("UPDATE plan lost its validated AST")
            assignments: dict[str, str] = {}
            for assignment in expression.expressions:
                if not isinstance(assignment, exp.EQ) or not isinstance(
                    assignment.this, exp.Column
                ):
                    raise UnsupportedSqlStatementError("UPDATE plan has an invalid assignment")
                target_column = _canonical_provider_column(assignment.this, field_names)
                if target_column in assignments:
                    raise UnsupportedSqlStatementError(
                        f"UPDATE assigns column more than once: {target_column}"
                    )
                assignments[target_column] = render_sparkless_sql(
                    _canonicalize_provider_columns(assignment.expression, field_names)
                )
            if cancellation is not None:
                cancellation.begin_commit()
            mutation = service.update_rows(
                table,
                assignments,
                predicate,
                operation_id=selected_operation_id,
                expected_version=snapshot.version,
            )
        else:
            if not isinstance(expression, exp.Delete):
                raise UnsupportedSqlStatementError("DELETE plan lost its validated AST")
            if cancellation is not None:
                cancellation.begin_commit()
            mutation = service.delete_rows(
                table,
                predicate,
                operation_id=selected_operation_id,
                expected_version=snapshot.version,
            )

        return SqlExecutionResult(
            operation=plan.operation,
            data=_empty_result_table(),
            object_name=target.full_name,
            commit=mutation,
            rows_affected=mutation.rows_affected,
            no_op=mutation.no_op,
        )

    def _merge_table(
        self,
        plan: SqlStatementPlan,
        *,
        operation_id: str | None,
        cancellation: SqlStatementControl | None,
    ) -> SqlExecutionResult:
        target = _required_table_target(plan)
        if len(plan.source_tables) != 1 or plan.merge_actions is None:
            raise UnsupportedSqlStatementError("MERGE plan lost its registered source or actions")
        source = plan.source_tables[0].table
        actions = plan.merge_actions
        registration = self._context.tables.get(
            target.catalog_name,
            target.schema_name,
            target.object_name,
        )
        target_table = TableHandle.from_registration(registration)
        from brickhouse.tables.delta_rs import DeltaRsTableService

        table_service = DeltaRsTableService(self._context)
        target_snapshot = table_service.read_snapshot(target_table)
        source_read = table_service.read_table(source)
        source_data = source_read.data
        target_field_names = target_snapshot.schema.field_names
        source_field_names = source_read.snapshot.schema.field_names
        if target_snapshot.schema != source_read.snapshot.schema:
            raise UnsupportedSqlStatementError(
                "selected MERGE requires identical source and target Delta schemas"
            )

        target_key = _canonical_merge_field(actions.target_key, target_field_names, "target key")
        source_key = _canonical_merge_field(actions.source_key, source_field_names, "source key")
        updates: dict[str, str] = {}
        for target_name, source_name in actions.matched_updates:
            target_column = _canonical_merge_field(target_name, target_field_names, "UPDATE target")
            source_column = _canonical_merge_field(source_name, source_field_names, "UPDATE source")
            if (
                source_data.schema.field(target_column).type
                != source_data.schema.field(source_column).type
            ):
                raise UnsupportedSqlStatementError(
                    f"MERGE UPDATE column types do not match: {target_column} and {source_column}"
                )
            updates[_quote_delta_identifier(target_column)] = _delta_column_reference(
                actions.source_alias,
                source_column,
            )

        inserts: dict[str, str] = {}
        insert_target_names: set[str] = set()
        for target_name, source_name in actions.not_matched_inserts:
            target_column = _canonical_merge_field(target_name, target_field_names, "INSERT target")
            source_column = _canonical_merge_field(source_name, source_field_names, "INSERT source")
            if (
                source_data.schema.field(target_column).type
                != source_data.schema.field(source_column).type
            ):
                raise UnsupportedSqlStatementError(
                    f"MERGE INSERT column types do not match: {target_column} and {source_column}"
                )
            insert_target_names.add(target_column.casefold())
            inserts[_quote_delta_identifier(target_column)] = _delta_column_reference(
                actions.source_alias,
                source_column,
            )
        if insert_target_names != set(field.casefold() for field in target_field_names):
            raise UnsupportedSqlStatementError(
                "selected MERGE INSERT must provide exactly one source value for every "
                "target column"
            )

        predicate = (
            f"{actions.target_alias}.{_quote_delta_identifier(target_key)} = "
            f"{actions.source_alias}.{_quote_delta_identifier(source_key)}"
        )
        if cancellation is not None:
            cancellation.begin_commit()
        mutation = table_service.merge_rows(
            target_table,
            source_data,
            predicate,
            source_alias=actions.source_alias,
            target_alias=actions.target_alias,
            source_key_column=source_key,
            matched_updates=updates,
            not_matched_inserts=inserts,
            source_identity=f"{source.table_id}@{source_read.snapshot.version}",
            operation_id=operation_id or f"sql-{uuid.uuid4().hex}",
            expected_version=target_snapshot.version,
        )
        return SqlExecutionResult(
            operation=plan.operation,
            data=_empty_result_table(),
            object_name=target.full_name,
            commit=mutation,
            rows_affected=mutation.rows_affected,
            no_op=mutation.no_op,
        )

    def _execute_query(
        self,
        query: str,
        *,
        cancellation: SqlStatementControl | None = None,
        commit_follows: bool,
        snapshot_versions: dict[str, int] | None = None,
        bounded_result: bool = False,
        row_limit: int | None = None,
        byte_limit: int | None = None,
    ) -> Any:
        from brickhouse.engines.sparkless import (
            SparklessActionCancelledError,
            SparklessSessionFactory,
        )

        with SparklessSessionFactory(self._context).open(
            snapshot_versions=snapshot_versions
        ) as session:
            dataframe = session.spark.sql(query)

            def materialize() -> Any:
                if bounded_result:
                    from brickhouse.sql.results import collect_bounded_arrow_result

                    return collect_bounded_arrow_result(
                        dataframe,
                        row_limit=row_limit,
                        byte_limit=byte_limit,
                    )
                return dataframe.toArrow()

            if cancellation is None:
                return materialize()
            cancellation.install_engine_canceller(
                session.spark.cancellation_handle().cancel_active_actions
            )
            cancellation.begin_engine_action()
            try:
                result = materialize()
            except SparklessActionCancelledError as error:
                cancellation.fail_engine_action()
                raise SqlStatementCancelledError(
                    "Sparkless reported cancellation of the active SQL statement"
                ) from error
            except BaseException:
                cancellation.fail_engine_action()
                raise
            cancellation.finish_engine_action(commit_follows=commit_follows)
            return result

    def _execute_planned_query(
        self,
        plan: SqlStatementPlan,
        query: str,
        *,
        cancellation: SqlStatementControl | None,
        commit_follows: bool,
        bounded_result: bool = False,
        row_limit: int | None = None,
        byte_limit: int | None = None,
    ) -> Any:
        snapshot_versions = {
            reference.table.table_id: reference.snapshot_version
            for reference in plan.source_tables
            if reference.snapshot_version is not None
        }
        if snapshot_versions:
            from brickhouse.tables.delta_rs import DeltaRsTableService

            tables = DeltaRsTableService(self._context)
            for reference in plan.source_tables:
                version = reference.snapshot_version
                if version is not None:
                    tables.read_table(reference.table, SnapshotSelector(version=version))
        return self._execute_query(
            query,
            cancellation=cancellation,
            commit_follows=commit_follows,
            snapshot_versions=snapshot_versions or None,
            bounded_result=bounded_result,
            row_limit=row_limit,
            byte_limit=byte_limit,
        )


def _align_insert_columns(data: Any, target_schema: Any, columns: tuple[str, ...] | None) -> Any:
    import pyarrow as pa  # type: ignore[import-untyped]

    target_names = tuple(field.name for field in target_schema)
    selected_names = target_names if columns is None else columns
    if data.num_columns != len(selected_names):
        raise UnsupportedSqlStatementError(
            "INSERT source column count does not match its target column list"
        )
    target_by_name = {
        name.casefold(): field for name, field in zip(target_names, target_schema, strict=True)
    }
    selected_by_name: dict[str, int] = {}
    for index, name in enumerate(selected_names):
        key = name.casefold()
        if key not in target_by_name:
            raise UnsupportedSqlStatementError(f"INSERT target column does not exist: {name}")
        if key in selected_by_name:
            raise UnsupportedSqlStatementError(f"INSERT target column is repeated: {name}")
        selected_by_name[key] = index

    arrays: list[Any] = []
    for field in target_schema:
        source_index = selected_by_name.get(field.name.casefold())
        if source_index is None:
            if not field.nullable:
                raise UnsupportedSqlStatementError(
                    f"INSERT omits non-nullable target column: {field.name}"
                )
            arrays.append(pa.nulls(data.num_rows, type=field.type))
            continue
        column = data.column(source_index)
        if column.type != field.type:
            try:
                column = column.cast(field.type, safe=True)
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError) as error:
                raise UnsupportedSqlStatementError(
                    f"INSERT source type is incompatible with target column: {field.name}"
                ) from error
        if not field.nullable and column.null_count:
            raise UnsupportedSqlStatementError(
                f"INSERT supplies NULL for non-nullable target column: {field.name}"
            )
        arrays.append(column)
    try:
        return pa.Table.from_arrays(arrays, schema=target_schema)
    except (pa.ArrowInvalid, pa.ArrowTypeError, ValueError) as error:
        raise UnsupportedSqlStatementError(
            "INSERT source does not match the target schema"
        ) from error


def _empty_table_data(columns: tuple[exp.ColumnDef, ...]) -> Any:
    import pyarrow as pa

    fields = [
        pa.field(str(column.this.this), _arrow_type(column.kind))
        for column in columns
        if isinstance(column.this, exp.Identifier) and isinstance(column.kind, exp.DataType)
    ]
    if len(fields) != len(columns):
        raise UnsupportedSqlStatementError("table column definitions are incomplete")
    return pa.Table.from_batches([], schema=pa.schema(fields))


def _arrow_type(data_type: exp.DataType) -> Any:
    import pyarrow as pa

    raw_name = data_type.this
    name = str(getattr(raw_name, "value", raw_name)).upper()
    primitive_types = {
        "BIGINT": pa.int64,
        "BOOLEAN": pa.bool_,
        "DATE": pa.date32,
        "DOUBLE": pa.float64,
        "FLOAT": pa.float32,
        "INT": pa.int32,
        "INTEGER": pa.int32,
        "SMALLINT": pa.int16,
        "STRING": pa.string,
        "TEXT": pa.string,
        "TINYINT": pa.int8,
        "VARCHAR": pa.string,
    }
    if name == "DECIMAL":
        parameters = data_type.expressions
        if len(parameters) != 2:
            raise UnsupportedSqlStatementError("DECIMAL requires explicit precision and scale")
        try:
            precision = int(parameters[0].this.this)
            scale = int(parameters[1].this.this)
            return pa.decimal128(precision, scale)
        except (AttributeError, TypeError, ValueError) as error:
            raise UnsupportedSqlStatementError(
                "DECIMAL precision and scale must be integers"
            ) from error
    if data_type.expressions:
        raise UnsupportedSqlStatementError(f"SQL type parameters are unsupported for {name}")
    arrow_type = primitive_types.get(name)
    if arrow_type is None:
        raise UnsupportedSqlStatementError(f"SQL data type is not supported: {name}")
    return arrow_type()


def _required_sql(plan: SqlStatementPlan) -> str:
    if plan.rendered_sql is None:
        raise UnsupportedSqlStatementError("SELECT plan has no bound SQL text")
    return plan.rendered_sql


def _required_table_target(plan: SqlStatementPlan) -> SqlObjectName:
    if plan.table_target is None:
        raise UnsupportedSqlStatementError("table DDL plan has no bound target")
    return plan.table_target


def _required_schema_target(plan: SqlStatementPlan) -> Any:
    if plan.schema_target is None:
        raise UnsupportedSqlStatementError("schema DDL plan has no bound target")
    return plan.schema_target


def _target_name(plan: SqlStatementPlan) -> str | None:
    if plan.table_target is not None:
        return plan.table_target.full_name
    if plan.schema_target is not None:
        return plan.schema_target.full_name
    return None


def _describe_schema(schema_json: str) -> Any:
    import pyarrow as pa

    try:
        decoded = json.loads(schema_json)
    except json.JSONDecodeError as error:
        raise UnsupportedSqlStatementError("registered Delta schema is malformed") from error
    fields = decoded.get("fields") if isinstance(decoded, dict) else None
    if not isinstance(fields, list):
        raise UnsupportedSqlStatementError("registered Delta schema has no field list")

    rows: list[dict[str, str | None]] = []
    for field in fields:
        if not isinstance(field, dict) or not isinstance(field.get("name"), str):
            raise UnsupportedSqlStatementError("registered Delta schema has a malformed field")
        metadata = field.get("metadata", {})
        comment = metadata.get("comment") if isinstance(metadata, dict) else None
        if comment is not None and not isinstance(comment, str):
            raise UnsupportedSqlStatementError("Delta column comment is not text")
        rows.append(
            {
                "col_name": field["name"],
                "data_type": _delta_sql_type(field.get("type")),
                "comment": comment,
            }
        )
    schema = pa.schema(
        [
            pa.field("col_name", pa.string(), nullable=False),
            pa.field("data_type", pa.string(), nullable=False),
            pa.field("comment", pa.string(), nullable=True),
        ]
    )
    return pa.Table.from_pylist(rows, schema=schema)


def _delta_sql_type(value: object) -> str:
    if isinstance(value, str):
        normalized = value.strip().lower()
        primitive_types = {
            "binary": "binary",
            "boolean": "boolean",
            "byte": "tinyint",
            "date": "date",
            "double": "double",
            "float": "float",
            "integer": "int",
            "long": "bigint",
            "null": "void",
            "short": "smallint",
            "string": "string",
            "timestamp": "timestamp",
            "timestamp_ntz": "timestamp_ntz",
        }
        if normalized in primitive_types:
            return primitive_types[normalized]
        if re.fullmatch(r"decimal\(\s*\d+\s*,\s*\d+\s*\)", normalized):
            return re.sub(r"\s+", "", normalized)
        if re.fullmatch(r"(?:char|varchar)\(\s*\d+\s*\)", normalized):
            return re.sub(r"\s+", "", normalized)
        raise UnsupportedSqlStatementError(f"DESCRIBE TABLE does not support Delta type {value!r}")
    if not isinstance(value, dict):
        raise UnsupportedSqlStatementError("registered Delta schema has an invalid type")

    kind = value.get("type")
    if kind == "array":
        if "elementType" not in value:
            raise UnsupportedSqlStatementError("Delta array type has no element type")
        return f"array<{_delta_sql_type(value['elementType'])}>"
    if kind == "map":
        if "keyType" not in value or "valueType" not in value:
            raise UnsupportedSqlStatementError("Delta map type is incomplete")
        return f"map<{_delta_sql_type(value['keyType'])},{_delta_sql_type(value['valueType'])}>"
    if kind == "struct":
        nested_fields = value.get("fields")
        if not isinstance(nested_fields, list):
            raise UnsupportedSqlStatementError("Delta struct type has no field list")
        descriptions: list[str] = []
        for field in nested_fields:
            if not isinstance(field, dict) or not isinstance(field.get("name"), str):
                raise UnsupportedSqlStatementError("Delta struct has a malformed field")
            name = _describe_nested_field_name(field["name"])
            descriptions.append(f"{name}:{_delta_sql_type(field.get('type'))}")
        return f"struct<{','.join(descriptions)}>"
    raise UnsupportedSqlStatementError(f"DESCRIBE TABLE does not support Delta type {kind!r}")


def _describe_nested_field_name(name: str) -> str:
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        return name
    return f"`{name.replace('`', '``')}`"


def _describe_history(entries: tuple[TableHistoryEntry, ...]) -> Any:
    import pyarrow as pa

    string_map = pa.map_(pa.string(), pa.string())
    schema = pa.schema(
        [
            pa.field("version", pa.int64(), nullable=False),
            pa.field("timestamp", pa.timestamp("ms", tz="UTC"), nullable=False),
            pa.field("operation", pa.string(), nullable=False),
            pa.field("operationParameters", string_map, nullable=True),
            pa.field("readVersion", pa.int64(), nullable=True),
            pa.field("operationMetrics", string_map, nullable=True),
            pa.field("engineInfo", pa.string(), nullable=True),
        ]
    )
    rows = [
        {
            "version": entry.version,
            "timestamp": entry.timestamp,
            "operation": entry.operation,
            "operationParameters": (
                list(entry.operation_parameters) if entry.operation_parameters is not None else None
            ),
            "readVersion": entry.read_version,
            "operationMetrics": (
                list(entry.operation_metrics) if entry.operation_metrics is not None else None
            ),
            "engineInfo": entry.engine_info,
        }
        for entry in entries
    ]
    return pa.Table.from_pylist(rows, schema=schema)


def _empty_result_table() -> Any:
    import pyarrow as pa

    return pa.table({})


def _canonical_provider_column(column: exp.Column, field_names: tuple[str, ...]) -> str:
    if any(column.args.get(key) is not None for key in ("table", "db", "catalog")):
        raise UnsupportedSqlStatementError("qualified SQL mutation columns are unsupported")
    if not isinstance(column.this, exp.Identifier):
        raise UnsupportedSqlStatementError("SQL mutation columns must be simple names")
    supplied = str(column.this.this)
    matches = [name for name in field_names if name.casefold() == supplied.casefold()]
    if len(matches) != 1:
        reason = "unknown" if not matches else "ambiguous"
        raise UnsupportedSqlStatementError(f"SQL mutation column is {reason}: {supplied}")
    return matches[0]


def _canonicalize_provider_columns(
    expression: exp.Expression,
    field_names: tuple[str, ...],
) -> exp.Expression:
    copied = expression.copy()
    for node in copied.walk():
        if isinstance(node, exp.Column):
            canonical_name = _canonical_provider_column(node, field_names)
            assert isinstance(node.this, exp.Identifier)
            quoted = bool(node.this.args.get("quoted")) or not canonical_name.isidentifier()
            node.set("this", exp.to_identifier(canonical_name, quoted=quoted))
    return copied


def _render_provider_expression(
    expression: exp.Expression,
    field_names: tuple[str, ...],
) -> str:
    return render_sparkless_sql(_canonicalize_provider_columns(expression, field_names))


def _canonical_merge_field(name: str, field_names: tuple[str, ...], role: str) -> str:
    matches = [field for field in field_names if field.casefold() == name.casefold()]
    if len(matches) != 1:
        reason = "unknown" if not matches else "ambiguous"
        raise UnsupportedSqlStatementError(f"MERGE {role} is {reason}: {name}")
    return matches[0]


def _quote_delta_identifier(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def _delta_column_reference(alias: str, name: str) -> str:
    return f"{alias}.{_quote_delta_identifier(name)}"
