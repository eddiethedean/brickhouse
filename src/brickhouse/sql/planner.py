"""AST classification and catalog binding for the initial Databricks SQL subset."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import cast

import sqlglot
from sqlglot import exp
from sqlglot.errors import ErrorLevel, ParseError

from brickhouse.catalog.identifiers import (
    AmbiguousIdentifierError,
    NamespaceState,
    ParsedIdentifier,
    parse_identifier,
)
from brickhouse.catalog.repository import (
    NamespaceAlreadyExistsError,
    NamespaceNotFoundError,
    TableNotFoundError,
)
from brickhouse.sql.errors import SqlParseError, UnsupportedSqlStatementError
from brickhouse.sql.parameters import (
    SqlStatementParameter,
    bind_expression_parameters,
)
from brickhouse.sql.render import render_sparkless_sql
from brickhouse.tables.contracts import TableHandle
from brickhouse.workspace.context import WorkspaceContext


class SqlOperation(StrEnum):
    """Statement kinds implemented by the first SQL execution slice."""

    SELECT = "select"
    CREATE_SCHEMA = "create_schema"
    DROP_SCHEMA = "drop_schema"
    CREATE_TABLE = "create_table"
    DROP_TABLE = "drop_table"
    UPDATE = "update"
    DELETE = "delete"
    MERGE = "merge"
    INSERT = "insert"
    DESCRIBE_TABLE = "describe_table"
    DESCRIBE_HISTORY = "describe_history"


@dataclass(frozen=True, slots=True)
class BoundTableReference:
    """One source table resolved to its authoritative BrickHouse registration."""

    input_name: str
    table: TableHandle
    snapshot_version: int | None = None


@dataclass(frozen=True, slots=True)
class SqlObjectName:
    """Canonical catalog/schema/table coordinates for a DDL target."""

    catalog_name: str
    schema_name: str
    object_name: str

    @property
    def full_name(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.object_name}"


@dataclass(frozen=True, slots=True)
class SqlSchemaName:
    """Canonical catalog/schema coordinates for a schema DDL target."""

    catalog_name: str
    schema_name: str

    @property
    def full_name(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}"


@dataclass(frozen=True, slots=True)
class SqlMergeActions:
    """One matched update and unmatched insert for a qualified single-key MERGE."""

    target_alias: str
    source_alias: str
    target_key: str
    source_key: str
    matched_updates: tuple[tuple[str, str], ...]
    not_matched_inserts: tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class SqlStatementPlan:
    """Validated AST, bound sources, and target for one local statement."""

    operation: SqlOperation
    expression: exp.Expression
    rendered_sql: str | None = None
    source_tables: tuple[BoundTableReference, ...] = ()
    table_target: SqlObjectName | None = None
    schema_target: SqlSchemaName | None = None
    create_columns: tuple[exp.ColumnDef, ...] = ()
    create_if_missing: bool = False
    target_exists: bool = False
    no_op: bool = False
    merge_actions: SqlMergeActions | None = None
    insert_columns: tuple[str, ...] | None = None


class SqlStatementPlanner:
    """Parse one statement, bind table references, and reject unsupported ASTs."""

    def __init__(
        self,
        context: WorkspaceContext,
        namespace: NamespaceState | None = None,
    ) -> None:
        self._context = context
        self._namespace = namespace or context.new_namespace_state()

    def plan(
        self,
        statement: str,
        *,
        parameters: tuple[SqlStatementParameter, ...] = (),
    ) -> SqlStatementPlan:
        """Return a bound plan without executing or mutating workspace state."""
        if not isinstance(statement, str) or not statement.strip():
            raise SqlParseError("SQL statement must be a non-empty string")
        try:
            parsed = sqlglot.parse(statement, read="databricks", error_level=ErrorLevel.RAISE)
        except ParseError as error:
            raise SqlParseError(f"could not parse SQL statement: {error}") from error
        if len(parsed) != 1 or parsed[0] is None:
            raise SqlParseError("exactly one non-empty SQL statement is supported")
        expression = cast(exp.Expression, parsed[0])
        bind_expression_parameters(expression, parameters)

        if isinstance(expression, exp.Select):
            return self._select_plan(expression)
        if isinstance(expression, exp.Create):
            return self._create_plan(expression)
        if isinstance(expression, exp.Drop):
            return self._drop_plan(expression)
        if isinstance(expression, exp.Describe):
            return self._describe_plan(expression)
        if isinstance(expression, exp.Update):
            return self._update_plan(expression)
        if isinstance(expression, exp.Delete):
            return self._delete_plan(expression)
        if isinstance(expression, exp.Insert):
            return self._insert_plan(expression)
        if isinstance(expression, exp.Merge):
            return self._merge_plan(expression)
        raise UnsupportedSqlStatementError(
            f"SQL statement type is not supported by the local profile: {type(expression).__name__}"
        )

    def _select_plan(self, expression: exp.Select) -> SqlStatementPlan:
        self._validate_query(expression)
        tables = self._bind_query_tables(expression)
        return SqlStatementPlan(
            operation=SqlOperation.SELECT,
            expression=expression,
            rendered_sql=render_sparkless_sql(expression),
            source_tables=tables,
        )

    def _create_plan(self, expression: exp.Create) -> SqlStatementPlan:
        kind = str(expression.args.get("kind", "")).upper()
        if expression.args.get("replace") or expression.args.get("temporary"):
            raise UnsupportedSqlStatementError(
                "CREATE OR REPLACE and temporary DDL are unsupported"
            )
        if kind == "SCHEMA":
            target_identifier = _table_identifier(_require_table(expression.this, "CREATE SCHEMA"))
            if expression.args.get("expression") is not None or expression.args.get("properties"):
                raise UnsupportedSqlStatementError("CREATE SCHEMA properties are unsupported")
            schema_name = self._schema_name(target_identifier, allow_missing=True)
            existing = self._schema_exists(schema_name)
            if existing and not bool(expression.args.get("exists")):
                raise NamespaceAlreadyExistsError(f"schema already exists: {schema_name.full_name}")
            return SqlStatementPlan(
                operation=SqlOperation.CREATE_SCHEMA,
                expression=expression,
                schema_target=schema_name,
                create_if_missing=bool(expression.args.get("exists")),
                no_op=existing,
            )
        if kind != "TABLE":
            raise UnsupportedSqlStatementError(f"CREATE {kind or 'OBJECT'} is unsupported")
        self._validate_delta_create(expression)
        target_expression = expression.this
        columns: tuple[exp.ColumnDef, ...] = ()
        if isinstance(target_expression, exp.Schema):
            if not all(
                isinstance(column, exp.ColumnDef) for column in target_expression.expressions
            ):
                raise UnsupportedSqlStatementError("table constraints are unsupported")
            columns = tuple(cast(exp.ColumnDef, column) for column in target_expression.expressions)
            target_expression = target_expression.this
        table_target = self._table_name(
            _table_identifier(_require_table(target_expression, "CREATE TABLE")),
            allow_missing=True,
        )
        exists = self._table_exists(table_target)

        query = expression.args.get("expression")
        source_tables: tuple[BoundTableReference, ...] = ()
        if query is not None:
            if not isinstance(query, exp.Select) or columns:
                raise UnsupportedSqlStatementError(
                    "CREATE TABLE supports either an explicit primitive schema or SELECT AS"
                )
            self._validate_query(query)
            source_tables = self._bind_query_tables(query)
        elif not columns:
            raise UnsupportedSqlStatementError(
                "CREATE TABLE requires primitive column definitions or AS SELECT"
            )
        else:
            self._validate_columns(columns)
        return SqlStatementPlan(
            operation=SqlOperation.CREATE_TABLE,
            expression=expression,
            rendered_sql=render_sparkless_sql(expression),
            source_tables=source_tables,
            table_target=table_target,
            create_columns=columns,
            create_if_missing=bool(expression.args.get("exists")),
            target_exists=exists,
            no_op=False,
        )

    def _drop_plan(self, expression: exp.Drop) -> SqlStatementPlan:
        kind = str(expression.args.get("kind", "")).upper()
        if (
            expression.args.get("cascade")
            or expression.args.get("restrict")
            or expression.args.get("purge")
            or expression.args.get("temporary")
            or expression.args.get("materialized")
            or expression.args.get("force")
            or expression.args.get("concurrently")
            or expression.args.get("sync")
            or expression.args.get("iceberg")
            or expression.args.get("constraints")
            or expression.args.get("cluster")
        ):
            raise UnsupportedSqlStatementError("the requested DROP options are unsupported")
        targets = expression.args.get("tables")
        if not isinstance(targets, list) or len(targets) != 1:
            raise UnsupportedSqlStatementError("DROP must name exactly one object")
        target = _require_table(targets[0], "DROP")
        conditional = bool(expression.args.get("exists"))
        if kind == "TABLE":
            table_name = self._table_name(
                _table_identifier(target),
                allow_missing=conditional,
                allow_missing_namespace=conditional,
            )
            exists = self._table_exists(table_name)
            if not exists and not conditional:
                raise TableNotFoundError(f"table does not exist: {table_name.full_name}")
            return SqlStatementPlan(
                operation=SqlOperation.DROP_TABLE,
                expression=expression,
                table_target=table_name,
                create_if_missing=conditional,
                no_op=not exists,
            )
        if kind == "SCHEMA":
            schema_target = self._schema_name(
                _table_identifier(target),
                allow_missing=conditional,
                allow_missing_catalog=conditional,
            )
            exists = self._schema_exists(schema_target)
            if not exists and not conditional:
                raise NamespaceNotFoundError(f"schema does not exist: {schema_target.full_name}")
            return SqlStatementPlan(
                operation=SqlOperation.DROP_SCHEMA,
                expression=expression,
                schema_target=schema_target,
                create_if_missing=conditional,
                no_op=not exists,
            )
        raise UnsupportedSqlStatementError(f"DROP {kind or 'OBJECT'} is unsupported")

    def _describe_plan(self, expression: exp.Describe) -> SqlStatementPlan:
        if (
            expression.args.get("expressions") is not None
            or expression.args.get("partition") is not None
            or expression.args.get("format") is not None
            or expression.args.get("as_json")
        ):
            raise UnsupportedSqlStatementError("DESCRIBE options are unsupported")
        style = expression.args.get("style")
        kind = expression.args.get("kind")
        if style == "HISTORY" and kind is None:
            operation = SqlOperation.DESCRIBE_HISTORY
        elif style is None and str(kind).upper() == "TABLE":
            operation = SqlOperation.DESCRIBE_TABLE
        else:
            raise UnsupportedSqlStatementError(
                "this SQL profile supports only DESCRIBE TABLE and DESCRIBE HISTORY"
            )
        target = _require_table(expression.this, "DESCRIBE")
        if target.args.get("alias") is not None:
            raise UnsupportedSqlStatementError("DESCRIBE table aliases are unsupported")
        table_target = self._table_name(_table_identifier(target))
        return SqlStatementPlan(
            operation=operation,
            expression=expression,
            table_target=table_target,
        )

    def _update_plan(self, expression: exp.Update) -> SqlStatementPlan:
        target = _require_table(expression.this, "UPDATE")
        if target.args.get("alias") is not None:
            raise UnsupportedSqlStatementError("UPDATE target aliases are unsupported")
        if expression.args.get("hint") is not None or any(
            expression.args.get(key) is not None for key in ("from", "returning")
        ):
            raise UnsupportedSqlStatementError(
                "UPDATE hints, FROM clauses, and RETURNING are unsupported"
            )
        assignments = expression.expressions
        if not assignments:
            raise UnsupportedSqlStatementError("UPDATE requires at least one assignment")
        assigned_names: set[str] = set()
        for assignment in assignments:
            if not isinstance(assignment, exp.EQ) or not isinstance(assignment.this, exp.Column):
                raise UnsupportedSqlStatementError("UPDATE assignments must target one column")
            target_column = assignment.this
            if any(target_column.args.get(key) is not None for key in ("table", "db", "catalog")):
                raise UnsupportedSqlStatementError(
                    "qualified UPDATE assignment columns are unsupported"
                )
            name = _column_identifier(target_column)
            if name.casefold() in assigned_names:
                raise UnsupportedSqlStatementError(f"UPDATE assigns column more than once: {name}")
            assigned_names.add(name.casefold())
            self._validate_mutation_expression(assignment.expression, predicate=False)

        where = expression.args.get("where")
        if not isinstance(where, exp.Where):
            raise UnsupportedSqlStatementError("UPDATE requires a WHERE predicate in this profile")
        self._validate_mutation_expression(where.this, predicate=True)
        table_target = self._table_name(_table_identifier(target))
        return SqlStatementPlan(
            operation=SqlOperation.UPDATE,
            expression=expression,
            table_target=table_target,
        )

    def _delete_plan(self, expression: exp.Delete) -> SqlStatementPlan:
        target = _require_table(expression.this, "DELETE")
        if target.args.get("alias") is not None:
            raise UnsupportedSqlStatementError("DELETE target aliases are unsupported")
        if expression.args.get("tables") is not None or expression.args.get("using"):
            raise UnsupportedSqlStatementError("multi-table DELETE is unsupported")
        if any(
            expression.args.get(key) is not None for key in ("hint", "returning", "order", "limit")
        ) or expression.args.get("cluster"):
            raise UnsupportedSqlStatementError(
                "DELETE hints, RETURNING, ordering, and limits are unsupported"
            )
        where = expression.args.get("where")
        if not isinstance(where, exp.Where):
            raise UnsupportedSqlStatementError("DELETE requires a WHERE predicate in this profile")
        self._validate_mutation_expression(where.this, predicate=True)
        table_target = self._table_name(_table_identifier(target))
        return SqlStatementPlan(
            operation=SqlOperation.DELETE,
            expression=expression,
            table_target=table_target,
        )

    def _insert_plan(self, expression: exp.Insert) -> SqlStatementPlan:
        if (
            any(
                expression.args.get(key)
                for key in (
                    "stored",
                    "by_name",
                    "exists",
                    "partition",
                    "settings",
                    "default",
                    "conflict",
                    "returning",
                    "overwrite",
                    "alternative",
                    "ignore",
                    "source",
                )
            )
            or expression.args.get("hint") is not None
        ):
            raise UnsupportedSqlStatementError("INSERT options are unsupported in this profile")

        target_expression = expression.this
        columns: tuple[str, ...] | None = None
        if isinstance(target_expression, exp.Schema):
            if not all(isinstance(item, exp.Identifier) for item in target_expression.expressions):
                raise UnsupportedSqlStatementError("INSERT target columns must be identifiers")
            columns = tuple(str(item.this) for item in target_expression.expressions)
            folded = [name.casefold() for name in columns]
            if not columns or len(folded) != len(set(folded)):
                raise UnsupportedSqlStatementError(
                    "INSERT target columns must be non-empty and unique"
                )
            target_expression = target_expression.this
        target = _require_table(target_expression, "INSERT target")
        if target.args.get("alias") is not None:
            raise UnsupportedSqlStatementError("INSERT target aliases are unsupported")
        table_target = self._table_name(_table_identifier(target))

        source = expression.args.get("expression")
        if isinstance(source, exp.Select):
            self._validate_query(source)
            source_tables = self._bind_query_tables(source)
            rendered_sql = render_sparkless_sql(source)
        elif isinstance(source, exp.Values):
            self._validate_insert_values(source)
            source_tables = ()
            rendered_sql = _render_insert_values_query(source)
        else:
            raise UnsupportedSqlStatementError("INSERT requires VALUES or a SELECT query")
        return SqlStatementPlan(
            operation=SqlOperation.INSERT,
            expression=expression,
            rendered_sql=rendered_sql,
            source_tables=source_tables,
            table_target=table_target,
            insert_columns=columns,
        )

    @staticmethod
    def _validate_insert_values(values: exp.Values) -> None:
        rows = values.expressions
        if not rows or len(rows) > 10_000:
            raise UnsupportedSqlStatementError("INSERT VALUES must contain 1 to 10000 rows")
        allowed = (exp.Literal, exp.Null, exp.Boolean, exp.Cast, exp.DataType, exp.Identifier)
        column_count: int | None = None
        for row in rows:
            if not isinstance(row, exp.Tuple) or not row.expressions:
                raise UnsupportedSqlStatementError("INSERT VALUES rows must be literal tuples")
            if column_count is None:
                column_count = len(row.expressions)
            elif len(row.expressions) != column_count:
                raise UnsupportedSqlStatementError("INSERT VALUES rows must have equal widths")
            for value in row.expressions:
                if any(not isinstance(node, allowed) for node in value.walk()):
                    raise UnsupportedSqlStatementError(
                        "INSERT VALUES supports only scalar literals and typed literal casts"
                    )
                for cast_expr in value.find_all(exp.Cast):
                    if not isinstance(cast_expr.this, (exp.Literal, exp.Null, exp.Boolean)):
                        raise UnsupportedSqlStatementError(
                            "INSERT VALUES casts must wrap scalar literals"
                        )
                    if not isinstance(cast_expr.to, exp.DataType):
                        raise UnsupportedSqlStatementError("INSERT VALUES cast type is invalid")
                    _arrow_type_name(cast_expr.to)

    def _merge_plan(self, expression: exp.Merge) -> SqlStatementPlan:
        target = _require_table(expression.this, "MERGE target")
        source = expression.args.get("using")
        if not isinstance(source, exp.Table) or not isinstance(source.this, exp.Identifier):
            raise UnsupportedSqlStatementError(
                "MERGE source must be one registered table; subqueries and table functions "
                "are unsupported"
            )
        if expression.args.get("returning") is not None or expression.args.get("using_cond"):
            raise UnsupportedSqlStatementError(
                "MERGE RETURNING and USING conditions are unsupported"
            )
        if any(expression.find_all(exp.Placeholder)) or any(expression.find_all(exp.Parameter)):
            raise UnsupportedSqlStatementError("MERGE parameters are unsupported in this profile")

        target_alias = _merge_alias(target)
        source_alias = _merge_alias(source)
        if target_alias.casefold() == source_alias.casefold():
            raise UnsupportedSqlStatementError("MERGE target and source aliases must be distinct")

        on = expression.args.get("on")
        if not isinstance(on, exp.EQ) or not all(
            isinstance(side, exp.Column) for side in (on.this, on.expression)
        ):
            raise UnsupportedSqlStatementError(
                "MERGE ON must contain one target-key = source-key equality"
            )
        left = cast(exp.Column, on.this)
        right = cast(exp.Column, on.expression)
        left_alias = _qualified_merge_column_alias(left)
        right_alias = _qualified_merge_column_alias(right)
        if (left_alias, right_alias) == (target_alias.casefold(), source_alias.casefold()):
            target_key, source_key = _column_identifier(left), _column_identifier(right)
        elif (left_alias, right_alias) == (source_alias.casefold(), target_alias.casefold()):
            source_key, target_key = _column_identifier(left), _column_identifier(right)
        else:
            raise UnsupportedSqlStatementError(
                "MERGE ON columns must qualify one target key and one source key"
            )

        whens = expression.args.get("whens")
        clauses = whens.expressions if isinstance(whens, exp.Whens) else []
        if len(clauses) != 2 or not all(isinstance(clause, exp.When) for clause in clauses):
            raise UnsupportedSqlStatementError(
                "MERGE requires exactly one matched UPDATE and one unmatched INSERT clause"
            )
        matched = [clause for clause in clauses if clause.args.get("matched") is True]
        unmatched = [clause for clause in clauses if clause.args.get("matched") is False]
        if (
            len(matched) != 1
            or len(unmatched) != 1
            or any(
                clause.args.get("source") or clause.args.get("condition") is not None
                for clause in clauses
            )
        ):
            raise UnsupportedSqlStatementError(
                "MERGE supports only unconditional matched UPDATE and unmatched INSERT clauses"
            )

        matched_action = matched[0].args.get("then")
        if (
            not isinstance(matched_action, exp.Update)
            or matched_action.args.get("where") is not None
        ):
            raise UnsupportedSqlStatementError(
                "MERGE matched clause must be an unconditional UPDATE"
            )
        updates: list[tuple[str, str]] = []
        update_targets: set[str] = set()
        for assignment in matched_action.expressions:
            if not isinstance(assignment, exp.EQ) or not isinstance(assignment.this, exp.Column):
                raise UnsupportedSqlStatementError("MERGE UPDATE assignments must target columns")
            target_column = assignment.this
            _validate_merge_column_qualifier(
                target_column,
                expected_alias=target_alias,
                allow_unqualified=True,
                role="target assignment",
            )
            source_column = assignment.expression
            if not isinstance(source_column, exp.Column):
                raise UnsupportedSqlStatementError(
                    "MERGE UPDATE values must be source columns in this profile"
                )
            _validate_merge_column_qualifier(
                source_column,
                expected_alias=source_alias,
                allow_unqualified=False,
                role="source assignment",
            )
            target_name = _column_identifier(target_column)
            if target_name.casefold() in update_targets:
                raise UnsupportedSqlStatementError(
                    f"MERGE UPDATE assigns a target column more than once: {target_name}"
                )
            update_targets.add(target_name.casefold())
            updates.append((target_name, _column_identifier(source_column)))
        if not updates:
            raise UnsupportedSqlStatementError("MERGE matched UPDATE requires assignments")

        insert_action = unmatched[0].args.get("then")
        if not isinstance(insert_action, exp.Insert) or insert_action.args.get("where") is not None:
            raise UnsupportedSqlStatementError("MERGE unmatched clause must be an explicit INSERT")
        target_values = insert_action.args.get("this")
        source_values = insert_action.args.get("expression")
        if not isinstance(target_values, exp.Tuple) or not isinstance(source_values, exp.Tuple):
            raise UnsupportedSqlStatementError(
                "MERGE INSERT must list target columns and source columns explicitly"
            )
        if len(target_values.expressions) == 0 or len(target_values.expressions) != len(
            source_values.expressions
        ):
            raise UnsupportedSqlStatementError("MERGE INSERT columns and values must correspond")
        inserts: list[tuple[str, str]] = []
        insert_targets: set[str] = set()
        for target_value, source_value in zip(
            target_values.expressions, source_values.expressions, strict=True
        ):
            if not isinstance(target_value, exp.Column) or not isinstance(source_value, exp.Column):
                raise UnsupportedSqlStatementError(
                    "MERGE INSERT values must be source columns in this profile"
                )
            _validate_merge_column_qualifier(
                target_value,
                expected_alias=target_alias,
                allow_unqualified=True,
                role="target insert",
            )
            _validate_merge_column_qualifier(
                source_value,
                expected_alias=source_alias,
                allow_unqualified=False,
                role="source insert",
            )
            target_name = _column_identifier(target_value)
            if target_name.casefold() in insert_targets:
                raise UnsupportedSqlStatementError(
                    f"MERGE INSERT names a target column more than once: {target_name}"
                )
            insert_targets.add(target_name.casefold())
            inserts.append((target_name, _column_identifier(source_value)))

        table_target = self._table_name(_table_identifier(target))
        source_identifier = _table_identifier(source)
        source_registration = self._namespace.resolve_table(_render_identifier(source_identifier))
        source_handle = TableHandle.from_registration(source_registration)
        target_registration = self._context.tables.get(
            table_target.catalog_name,
            table_target.schema_name,
            table_target.object_name,
        )
        target_handle = TableHandle.from_registration(target_registration)
        if target_handle.table_id == source_handle.table_id:
            raise UnsupportedSqlStatementError("MERGE self-matches are unsupported")
        return SqlStatementPlan(
            operation=SqlOperation.MERGE,
            expression=expression,
            table_target=table_target,
            source_tables=(
                BoundTableReference(_render_identifier(source_identifier), source_handle),
            ),
            merge_actions=SqlMergeActions(
                target_alias=target_alias,
                source_alias=source_alias,
                target_key=target_key,
                source_key=source_key,
                matched_updates=tuple(updates),
                not_matched_inserts=tuple(inserts),
            ),
        )

    @staticmethod
    def _validate_mutation_expression(expression: exp.Expression, *, predicate: bool) -> None:
        """Keep provider SQL to column/literal operations with no calls or subqueries."""
        leaf_types = (exp.Column, exp.Identifier, exp.Literal, exp.Null, exp.Boolean)
        arithmetic_types = (exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Mod, exp.Neg, exp.Paren)
        predicate_types = (
            exp.And,
            exp.Or,
            exp.Not,
            exp.EQ,
            exp.NEQ,
            exp.LT,
            exp.LTE,
            exp.GT,
            exp.GTE,
            exp.Is,
            exp.In,
        )
        allowed = leaf_types + arithmetic_types + (predicate_types if predicate else ())
        for node in expression.walk():
            if not isinstance(node, allowed):
                raise UnsupportedSqlStatementError(
                    f"SQL mutation expression is unsupported: {type(node).__name__}"
                )
            if isinstance(node, exp.Column) and any(
                node.args.get(key) is not None for key in ("table", "db", "catalog")
            ):
                raise UnsupportedSqlStatementError("qualified SQL mutation columns are unsupported")
            if isinstance(node, exp.Is) and not isinstance(node.expression, exp.Null):
                raise UnsupportedSqlStatementError(
                    "IS predicates only support NULL in this profile"
                )
            if isinstance(node, exp.In) and (
                not node.expressions
                or not all(
                    isinstance(item, (exp.Literal, exp.Null, exp.Boolean))
                    for item in node.expressions
                )
                or node.args.get("query") is not None
            ):
                raise UnsupportedSqlStatementError("IN predicates require a literal value list")

    def _validate_delta_create(self, expression: exp.Create) -> None:
        if any(
            expression.args.get(key)
            for key in (
                "replace",
                "temporary",
                "refresh",
                "unique",
                "indexes",
                "no_schema_binding",
                "begin",
                "clone",
                "concurrently",
                "clustered",
            )
        ):
            raise UnsupportedSqlStatementError("the requested CREATE TABLE options are unsupported")
        if expression.args.get("properties") is None:
            raise UnsupportedSqlStatementError("CREATE TABLE requires USING DELTA")
        properties = expression.args["properties"].expressions
        if len(properties) != 1 or not isinstance(properties[0], exp.FileFormatProperty):
            raise UnsupportedSqlStatementError("only the USING DELTA table property is supported")
        if str(properties[0].this).upper() != "DELTA":
            raise UnsupportedSqlStatementError("only USING DELTA is supported")
        if expression.args.get("indexes"):
            raise UnsupportedSqlStatementError("table indexes are unsupported")

    def _validate_columns(self, columns: tuple[exp.ColumnDef, ...]) -> None:
        names: set[str] = set()
        for column in columns:
            if column.args.get("constraints") or column.args.get("position"):
                raise UnsupportedSqlStatementError(
                    "column constraints and positional declarations are unsupported"
                )
            if not isinstance(column.this, exp.Identifier) or not isinstance(
                column.kind, exp.DataType
            ):
                raise UnsupportedSqlStatementError(
                    "each table column needs a name and primitive type"
                )
            name = str(column.this.this)
            if name.casefold() in names:
                raise UnsupportedSqlStatementError(f"duplicate table column name: {name}")
            names.add(name.casefold())
            _arrow_type_name(column.kind)

    def _validate_query(self, query: exp.Select) -> None:
        if query.args.get("with_") or len(list(query.find_all(exp.Select))) > 1:
            raise UnsupportedSqlStatementError("CTEs and nested SELECT statements are unsupported")
        if any(query.find_all(exp.Placeholder)) or any(query.find_all(exp.Parameter)):
            raise UnsupportedSqlStatementError(
                "bound SQL parameters are not supported in this slice"
            )
        for function in query.find_all(exp.Func):
            if isinstance(function, exp.Anonymous):
                raise UnsupportedSqlStatementError(
                    f"SQL function is not supported in this slice: {function.name}"
                )
            if isinstance(function, (exp.Count, exp.Sum, exp.Condition)):
                continue
            raise UnsupportedSqlStatementError(
                f"SQL function is not supported in this slice: {function.sql_name()}"
            )
        joins = query.args.get("joins") or []
        if len(joins) > 1:
            raise UnsupportedSqlStatementError("at most one equijoin is supported")
        for join in joins:
            kind = join.args.get("kind")
            if (
                join.args.get("side")
                or join.args.get("method")
                or (kind is not None and str(kind).upper() != "INNER")
            ):
                raise UnsupportedSqlStatementError("only an inner equijoin is supported")
            condition = join.args.get("on")
            if not isinstance(condition, exp.EQ) or not all(
                isinstance(side, exp.Column) for side in (condition.this, condition.expression)
            ):
                raise UnsupportedSqlStatementError("joins must use one column-to-column equality")
            left, right = cast(exp.Column, condition.this), cast(exp.Column, condition.expression)
            if not left.table or not right.table or left.table.casefold() == right.table.casefold():
                raise UnsupportedSqlStatementError(
                    "equijoin columns must refer to different table aliases"
                )
        for table in query.find_all(exp.Table):
            if not isinstance(table.this, exp.Identifier):
                raise UnsupportedSqlStatementError(
                    "table functions and path-based sources are unsupported"
                )
            if any(table.args.get(key) for key in ("when", "sample", "pivots", "hints")):
                raise UnsupportedSqlStatementError(
                    "table time travel and sampling clauses are unsupported"
                )
            _table_snapshot_version(table)

    def _bind_query_tables(self, query: exp.Expression) -> tuple[BoundTableReference, ...]:
        bindings: list[BoundTableReference] = []
        table_bindings: list[tuple[exp.Table, TableHandle]] = []
        source_ids: set[str] = set()
        for table in query.find_all(exp.Table):
            if not isinstance(table.this, exp.Identifier):
                raise UnsupportedSqlStatementError(
                    "table functions and path-based sources are unsupported"
                )
            source_identifier = _table_identifier(table)
            registration = self._namespace.resolve_table(_render_identifier(source_identifier))
            handle = TableHandle.from_registration(registration)
            snapshot_version = _table_snapshot_version(table)
            if handle.table_id in source_ids:
                raise UnsupportedSqlStatementError("self-joins are unsupported in this slice")
            source_ids.add(handle.table_id)
            bindings.append(
                BoundTableReference(
                    _render_identifier(source_identifier),
                    handle,
                    snapshot_version=snapshot_version,
                )
            )
            table_bindings.append((table, handle))
        name_counts: dict[str, int] = {}
        for _, handle in table_bindings:
            folded_name = handle.name.casefold()
            name_counts[folded_name] = name_counts.get(folded_name, 0) + 1

        alias_targets: dict[str, str] = {}
        for table, handle in table_bindings:
            alias = table.args.get("alias")
            if (
                isinstance(alias, exp.TableAlias)
                and isinstance(alias.this, exp.Identifier)
                and name_counts[handle.name.casefold()] == 1
            ):
                alias_targets[str(alias.this.this).casefold()] = handle.name
                table.set("alias", None)
            table.set("version", None)
            table.set("catalog", exp.to_identifier(handle.catalog_name, quoted=True))
            table.set("db", exp.to_identifier(handle.schema_name, quoted=True))
            table.set("this", exp.to_identifier(handle.name, quoted=True))
        for column in query.find_all(exp.Column):
            qualifier = column.args.get("table")
            if isinstance(qualifier, exp.Identifier):
                target = alias_targets.get(str(qualifier.this).casefold())
                if target is not None:
                    column.set("catalog", None)
                    column.set("db", None)
                    column.set("table", exp.to_identifier(target, quoted=True))
        return tuple(bindings)

    def _table_name(
        self,
        identifier: ParsedIdentifier,
        *,
        allow_missing: bool = False,
        allow_missing_namespace: bool = False,
    ) -> SqlObjectName:
        parts = identifier.parts
        if len(parts) == 1:
            catalog_name = self._namespace.current_catalog
            schema_name = self._namespace.current_schema
            object_name = parts[0].value
        elif len(parts) == 2:
            catalog_name = self._namespace.current_catalog
            schema_name = parts[0].value
            object_name = parts[1].value
        elif len(parts) == 3:
            catalog_name, schema_name, object_name = (part.value for part in parts)
        else:  # parse_identifier currently guarantees one to three parts.
            raise UnsupportedSqlStatementError("table names must have one to three parts")
        catalog_name = self._canonical_catalog(catalog_name, allow_missing=allow_missing_namespace)
        schema_name = self._canonical_schema(
            catalog_name, schema_name, allow_missing=allow_missing_namespace
        )
        object_name = self._canonical_table(
            catalog_name, schema_name, object_name, allow_missing=allow_missing
        )
        return SqlObjectName(catalog_name, schema_name, object_name)

    def _schema_name(
        self,
        identifier: ParsedIdentifier,
        *,
        allow_missing: bool = False,
        allow_missing_catalog: bool = False,
    ) -> SqlSchemaName:
        parts = identifier.parts
        if len(parts) == 1:
            catalog_name = self._namespace.current_catalog
            schema_name = parts[0].value
        elif len(parts) == 2:
            catalog_name, schema_name = (part.value for part in parts)
        else:
            raise UnsupportedSqlStatementError("schema names must have one or two parts")
        catalog_name = self._canonical_catalog(catalog_name, allow_missing=allow_missing_catalog)
        schema_name = self._canonical_schema(catalog_name, schema_name, allow_missing=allow_missing)
        return SqlSchemaName(catalog_name, schema_name)

    def _canonical_catalog(self, name: str, *, allow_missing: bool) -> str:
        matches = [
            item
            for item in self._context.catalogs.list()
            if item.name.casefold() == name.casefold()
        ]
        if len(matches) == 1:
            return matches[0].name
        if allow_missing and not matches:
            return name
        if len(matches) > 1:
            raise AmbiguousIdentifierError(f"catalog identifier matches multiple names: {name}")
        raise NamespaceNotFoundError(f"catalog does not exist: {name}")

    def _canonical_schema(self, catalog_name: str, name: str, *, allow_missing: bool) -> str:
        try:
            schemas = self._context.schemas.list(catalog_name)
        except NamespaceNotFoundError:
            if allow_missing:
                return name
            raise
        matches = [item for item in schemas if item.name.casefold() == name.casefold()]
        if len(matches) == 1:
            return matches[0].name
        if allow_missing and not matches:
            _validate_new_name("schema", name)
            return name
        if len(matches) > 1:
            raise AmbiguousIdentifierError(
                f"schema identifier matches multiple names: {catalog_name}.{name}"
            )
        raise NamespaceNotFoundError(f"schema does not exist: {catalog_name}.{name}")

    def _canonical_table(
        self,
        catalog_name: str,
        schema_name: str,
        name: str,
        *,
        allow_missing: bool,
    ) -> str:
        matches = [
            item
            for item in self._context.tables.list(catalog_name, schema_name)
            if item.name.casefold() == name.casefold()
        ]
        if len(matches) == 1:
            return matches[0].name
        if len(matches) > 1:
            raise AmbiguousIdentifierError(
                f"table identifier matches multiple names: {catalog_name}.{schema_name}.{name}"
            )
        if allow_missing:
            _validate_new_name("table", name)
            return name
        raise TableNotFoundError(f"table does not exist: {catalog_name}.{schema_name}.{name}")

    def _schema_exists(self, name: SqlSchemaName) -> bool:
        try:
            schemas = self._context.schemas.list(name.catalog_name)
        except NamespaceNotFoundError:
            return False
        return any(item.name.casefold() == name.schema_name.casefold() for item in schemas)

    def _table_exists(self, name: SqlObjectName) -> bool:
        try:
            self._context.tables.get(name.catalog_name, name.schema_name, name.object_name)
        except TableNotFoundError:
            return False
        return True


def _render_insert_values_query(values: exp.Values) -> str:
    projections: list[str] = []
    for row in values.expressions:
        assert isinstance(row, exp.Tuple)
        projections.append(
            "SELECT "
            + ", ".join(
                f"{render_sparkless_sql(value)} AS `__brickhouse_insert_{index}`"
                for index, value in enumerate(row.expressions)
            )
        )
    return " UNION ALL ".join(projections)


def _table_identifier(table: exp.Table) -> ParsedIdentifier:
    identifiers: list[exp.Expression] = []
    for key in ("catalog", "db", "this"):
        value = table.args.get(key)
        if value is not None:
            identifiers.append(value)
    if not identifiers or not isinstance(identifiers[-1], exp.Identifier):
        raise UnsupportedSqlStatementError("SQL object name must use identifier parts")
    parts: list[str] = []
    for identifier in identifiers:
        if not isinstance(identifier, exp.Identifier):
            raise UnsupportedSqlStatementError("SQL object name must use identifier parts")
        value = str(identifier.this)
        parts.append(f"`{value.replace('`', '``')}`")
    return parse_identifier(".".join(parts))


def _table_snapshot_version(table: exp.Table) -> int | None:
    version = table.args.get("version")
    if version is None:
        return None
    if (
        not isinstance(version, exp.Version)
        or str(version.args.get("kind", "")).upper() != "AS OF"
        or str(version.this).upper() != "VERSION"
    ):
        raise UnsupportedSqlStatementError(
            "only numeric VERSION AS OF table selectors are supported"
        )
    value = version.expression
    if not isinstance(value, exp.Literal) or value.is_string:
        raise UnsupportedSqlStatementError("VERSION AS OF requires a non-negative integer literal")
    raw_value = str(value.this)
    if re.fullmatch(r"[0-9]+", raw_value) is None:
        raise UnsupportedSqlStatementError("VERSION AS OF requires a non-negative integer literal")
    try:
        return int(raw_value)
    except ValueError as error:
        raise UnsupportedSqlStatementError(
            "VERSION AS OF is outside the supported range"
        ) from error


def _render_identifier(identifier: ParsedIdentifier) -> str:
    return ".".join(f"`{part.value.replace('`', '``')}`" for part in identifier.parts)


def _require_table(value: object, statement_kind: str) -> exp.Table:
    if not isinstance(value, exp.Table):
        raise UnsupportedSqlStatementError(f"{statement_kind} target must be a table identifier")
    return value


def _column_identifier(column: exp.Column) -> str:
    if not isinstance(column.this, exp.Identifier):
        raise UnsupportedSqlStatementError("SQL mutation target must be a simple column name")
    return str(column.this.this)


def _merge_alias(table: exp.Table) -> str:
    alias = table.args.get("alias")
    identifier = alias.this if isinstance(alias, exp.TableAlias) else table.this
    if not isinstance(identifier, exp.Identifier):
        raise UnsupportedSqlStatementError("MERGE table aliases must be simple identifiers")
    value = str(identifier.this)
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value) is None:
        raise UnsupportedSqlStatementError("MERGE aliases must be simple identifiers")
    return value


def _qualified_merge_column_alias(column: exp.Column) -> str:
    qualifier = column.args.get("table")
    if not isinstance(qualifier, exp.Identifier):
        raise UnsupportedSqlStatementError(
            "MERGE key columns must be qualified by their table alias"
        )
    if column.args.get("db") is not None or column.args.get("catalog") is not None:
        raise UnsupportedSqlStatementError("qualified multipart MERGE columns are unsupported")
    return str(qualifier.this).casefold()


def _validate_merge_column_qualifier(
    column: exp.Column,
    *,
    expected_alias: str,
    allow_unqualified: bool,
    role: str,
) -> None:
    if column.args.get("db") is not None or column.args.get("catalog") is not None:
        raise UnsupportedSqlStatementError(f"multipart MERGE {role} columns are unsupported")
    qualifier = column.args.get("table")
    if qualifier is None and allow_unqualified:
        return
    if not isinstance(qualifier, exp.Identifier) or str(qualifier.this).casefold() != (
        expected_alias.casefold()
    ):
        raise UnsupportedSqlStatementError(
            f"MERGE {role} columns must reference alias {expected_alias!r}"
        )


def _arrow_type_name(data_type: exp.DataType) -> str:
    """Reject non-primitive DDL types before table registration or Delta writes."""
    raw_name = data_type.this
    name = str(getattr(raw_name, "value", raw_name)).upper()
    allowed = {
        "BIGINT",
        "BOOLEAN",
        "DATE",
        "DECIMAL",
        "DOUBLE",
        "FLOAT",
        "INT",
        "INTEGER",
        "SMALLINT",
        "STRING",
        "TEXT",
        "TINYINT",
        "VARCHAR",
    }
    if name not in allowed:
        raise UnsupportedSqlStatementError(f"SQL data type is not supported: {name}")
    return name


def _validate_new_name(kind: str, name: str) -> None:
    if re.fullmatch(r"[a-z_][a-z0-9_]*", name) is None:
        raise UnsupportedSqlStatementError(
            f"new {kind} names must be lowercase unquoted identifiers"
        )
