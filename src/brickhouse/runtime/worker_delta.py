"""Worker-only DeltaTable compatibility backed by registered BrickHouse tables."""

from __future__ import annotations

from typing import Any

from brickhouse.runtime.worker_session import WorkerSparkSessionFacade


class UnsupportedWorkerDeltaOperationError(RuntimeError):
    """A DeltaTable operation is outside the registered read-only worker profile."""


class DeltaTable:
    """Small worker facade for registered table names and local paths."""

    def __init__(self, spark_session: Any, identifier: str, alias: str | None = None) -> None:
        self._spark_session = spark_session
        self._identifier = identifier
        self._alias = alias

    @classmethod
    def forName(cls, sparkSession: object, tableOrViewName: object) -> DeltaTable:
        """Resolve one registered table name with the worker SparkSession."""
        session = _require_worker_session(sparkSession)
        if not isinstance(tableOrViewName, str) or not tableOrViewName.strip():
            raise ValueError("DeltaTable.forName requires a non-empty registered table name")
        identifier = session._resolve_registered_name(tableOrViewName)
        if identifier is None:
            raise KeyError(f"DeltaTable name is not registered with BrickHouse: {tableOrViewName}")
        session.table(identifier)
        return cls(session, identifier)

    @classmethod
    def forPath(
        cls,
        sparkSession: object,
        path: object,
        hadoopConf: object = None,
    ) -> DeltaTable:
        """Resolve a canonical local path only when the parent catalog owns it."""
        session = _require_worker_session(sparkSession)
        if hadoopConf is not None:
            raise UnsupportedWorkerDeltaOperationError(
                "DeltaTable.forPath hadoopConf is unsupported in the local worker profile"
            )
        if not isinstance(path, str) or not path:
            raise ValueError("DeltaTable.forPath requires a registered local path")
        identifier = session._resolve_registered_path(path)
        if identifier is None:
            raise KeyError(f"DeltaTable path is not registered with BrickHouse: {path}")
        session.table(identifier)
        return cls(session, identifier)

    @classmethod
    def isDeltaTable(cls, sparkSession: object, identifier: object) -> bool:
        """Return whether a local path is registered as a BrickHouse Delta table."""
        session = _require_worker_session(sparkSession)
        if not isinstance(identifier, str) or not identifier:
            raise ValueError("DeltaTable.isDeltaTable requires a local path string")
        return session._is_registered_table_path(identifier)

    def toDF(self) -> Any:
        """Read the registered table through the session's shared Sparkless provider."""
        dataframe = self._spark_session.table(self._identifier)
        return dataframe if self._alias is None else dataframe.alias(self._alias)

    def alias(self, alias: object) -> DeltaTable:
        """Return a new read facade with a DataFrame alias."""
        if not isinstance(alias, str) or not alias.strip():
            raise ValueError("DeltaTable.alias requires a non-empty alias string")
        return type(self)(self._spark_session, self._identifier, alias)

    def delete(self, condition: object = None) -> None:
        del condition
        raise UnsupportedWorkerDeltaOperationError(
            "DeltaTable.delete is unsupported; worker table mutations must use "
            "BrickHouse's table service"
        )

    def update(
        self,
        condition: object = None,
        set: object = None,
    ) -> None:
        del condition, set
        raise UnsupportedWorkerDeltaOperationError(
            "DeltaTable.update is unsupported; worker table mutations must use "
            "BrickHouse's table service"
        )

    def merge(self, source: object, condition: object) -> None:
        del source, condition
        raise UnsupportedWorkerDeltaOperationError(
            "DeltaTable.merge is unsupported; worker table mutations must use "
            "BrickHouse's table service"
        )

    def restoreToVersion(self, version: object) -> None:
        del version
        raise UnsupportedWorkerDeltaOperationError(
            "DeltaTable.restoreToVersion is unsupported by the local worker profile"
        )

    def restoreToTimestamp(self, timestamp: object) -> None:
        del timestamp
        raise UnsupportedWorkerDeltaOperationError(
            "DeltaTable.restoreToTimestamp is unsupported by the local worker profile"
        )

    def vacuum(
        self, retentionHours: object = None, enforceRetentionDuration: object = True
    ) -> None:
        del retentionHours, enforceRetentionDuration
        raise UnsupportedWorkerDeltaOperationError(
            "DeltaTable.vacuum is unsupported by the local worker profile"
        )


def _require_worker_session(spark_session: object) -> WorkerSparkSessionFacade:
    if not isinstance(spark_session, WorkerSparkSessionFacade):
        raise UnsupportedWorkerDeltaOperationError(
            "DeltaTable factories require the managed BrickHouse worker SparkSession"
        )
    return spark_session
