"""Worker-scoped PySpark import compatibility backed by Sparkless."""

from __future__ import annotations

import importlib
import importlib.abc
import importlib.machinery
import sys
from types import ModuleType


class WorkerPySparkImportError(ImportError):
    """A PySpark import is unavailable in the BrickHouse worker profile."""


class ConflictingPySparkPackageError(WorkerPySparkImportError):
    """An installed or preloaded PySpark package conflicts with the worker bridge."""


class UnsupportedPySparkImportError(WorkerPySparkImportError):
    """The requested PySpark namespace is outside the worker compatibility surface."""


class ConflictingDeltaPackageError(WorkerPySparkImportError):
    """An installed or preloaded Delta Lake package conflicts with the worker facade."""


_SQL_EXPORTS = (
    "SparkSession",
    "SparkSessionBuilder",
    "DataFrame",
    "Column",
    "GroupedData",
    "DataFrameReader",
    "DataFrameWriter",
    "SparklessError",
    "Row",
)
_SUPPORTED_MODULES = {
    "pyspark": True,
    "pyspark.sql": True,
    "pyspark.sql.functions": False,
    "pyspark.sql.types": False,
    "pyspark.sql.window": False,
    "delta": True,
    "delta.tables": False,
}
_SPARKLESS_MODULES = {
    "pyspark.sql.functions": "sparkless.sql.functions",
    "pyspark.sql.types": "sparkless.sql.types",
    "pyspark.sql.window": "sparkless.sql.window",
}
_INSTALLED_FINDER: _WorkerImportFinder | None = None


def _unsupported_attribute(module_name: str, attribute: str) -> object:
    if attribute.startswith("__") and attribute.endswith("__"):
        raise AttributeError(attribute)
    raise UnsupportedPySparkImportError(
        f"BrickHouse worker does not support {module_name}.{attribute}"
    )


def _make_bridge_module(
    target_name: str,
    source_name: str,
    *,
    exports: tuple[str, ...] | None = None,
) -> ModuleType:
    try:
        source = importlib.import_module(source_name)
    except ImportError as error:
        raise WorkerPySparkImportError(
            f"BrickHouse worker requires the pinned Sparkless public module {source_name}"
        ) from error

    module = ModuleType(target_name, source.__doc__)
    module.__package__ = target_name.rpartition(".")[0]
    module.__loader__ = None
    module.__spec__ = importlib.machinery.ModuleSpec(target_name, loader=None)
    source_exports = exports
    if source_exports is None:
        declared = getattr(source, "__all__", None)
        if isinstance(declared, (tuple, list)) and all(isinstance(item, str) for item in declared):
            source_exports = tuple(declared)
        else:
            source_exports = tuple(name for name in vars(source) if not name.startswith("_"))
    for name in source_exports:
        try:
            value = getattr(source, name)
        except AttributeError as error:
            raise WorkerPySparkImportError(
                f"Sparkless public module {source_name} is missing declared export {name}"
            ) from error
        setattr(module, name, value)
    module.__dict__["__all__"] = list(source_exports)
    return module


def _install_sql_children(sql_module: ModuleType) -> None:
    children: dict[str, ModuleType] = {}
    for target_name, source_name in _SPARKLESS_MODULES.items():
        if target_name.endswith(".window"):
            exports = ("Window", "WindowSpec")
        else:
            exports = None
        children[target_name.rsplit(".", 1)[-1]] = _make_bridge_module(
            target_name,
            source_name,
            exports=exports,
        )

    for child_name, child_module in children.items():
        full_name = f"pyspark.sql.{child_name}"
        sys.modules[full_name] = child_module
        setattr(sql_module, child_name, child_module)


class _WorkerImportLoader(importlib.abc.Loader):
    def __init__(
        self,
        *,
        spark_session_class: type[object] | None,
        spark_session_builder_class: type[object] | None,
    ) -> None:
        self._spark_session_class = spark_session_class
        self._spark_session_builder_class = spark_session_builder_class

    def create_module(self, spec: importlib.machinery.ModuleSpec) -> ModuleType | None:
        del spec
        return None

    def exec_module(self, module: ModuleType) -> None:
        name = module.__name__
        if name == "delta":
            module.__dict__["__brickhouse_worker_delta__"] = True
            module.__dict__["__path__"] = []
            module.__dict__["__all__"] = ["tables"]

            def delta_attribute(attribute: str) -> object:
                if attribute == "tables":
                    return importlib.import_module("delta.tables")
                return _unsupported_attribute(name, attribute)

            module.__dict__["__getattr__"] = delta_attribute
            return
        if name == "delta.tables":
            try:
                from brickhouse.runtime.worker_delta import DeltaTable
            except ImportError as error:
                raise WorkerPySparkImportError(
                    "BrickHouse worker DeltaTable facade is unavailable"
                ) from error
            module.__dict__["__brickhouse_worker_delta__"] = True
            module.__dict__["DeltaTable"] = DeltaTable
            module.__dict__["__all__"] = ["DeltaTable"]
            module.__dict__["__getattr__"] = lambda attribute: _unsupported_attribute(
                name, attribute
            )
            return

        module.__dict__["__brickhouse_worker_sparkless__"] = True
        if name == "pyspark":
            module.__dict__["__path__"] = []
            module.__dict__["__all__"] = ["sql"]
            module.__dict__["__getattr__"] = lambda attribute: _unsupported_attribute(
                name, attribute
            )
            return

        if name != "pyspark.sql":
            raise UnsupportedPySparkImportError(
                f"BrickHouse worker does not support the PySpark namespace {name}"
            )

        if self._spark_session_class is not None:
            from brickhouse.runtime.worker_session import activate_worker_spark_session

            activate_worker_spark_session()

        module.__dict__["__path__"] = []
        source_sql = importlib.import_module("sparkless.sql")
        for attribute in _SQL_EXPORTS:
            try:
                value = getattr(source_sql, attribute)
            except AttributeError as error:
                raise WorkerPySparkImportError(
                    f"Sparkless public module sparkless.sql is missing export {attribute}"
                ) from error
            if attribute == "SparkSession" and self._spark_session_class is not None:
                value = self._spark_session_class
            elif (
                attribute == "SparkSessionBuilder" and self._spark_session_builder_class is not None
            ):
                value = self._spark_session_builder_class
            setattr(module, attribute, value)
        module.__dict__["__all__"] = list(_SQL_EXPORTS)
        module.__dict__["__getattr__"] = lambda attribute: _unsupported_attribute(name, attribute)
        _install_sql_children(module)


class _WorkerImportFinder(importlib.abc.MetaPathFinder):
    def __init__(
        self,
        *,
        spark_session_class: type[object] | None,
        spark_session_builder_class: type[object] | None,
    ) -> None:
        self._loader = _WorkerImportLoader(
            spark_session_class=spark_session_class,
            spark_session_builder_class=spark_session_builder_class,
        )

    def find_spec(
        self,
        fullname: str,
        path: object = None,
        target: ModuleType | None = None,
    ) -> importlib.machinery.ModuleSpec | None:
        del path, target
        root = fullname.split(".", 1)[0]
        if root not in {"pyspark", "delta"}:
            return None
        if fullname not in _SUPPORTED_MODULES:
            if root == "delta":
                raise UnsupportedPySparkImportError(
                    f"BrickHouse worker does not support the Delta namespace {fullname}; "
                    "supported imports are delta.tables and DeltaTable"
                )
            raise UnsupportedPySparkImportError(
                f"BrickHouse worker does not support the PySpark namespace {fullname}; "
                "supported imports are pyspark.sql, pyspark.sql.functions, "
                "pyspark.sql.types, and pyspark.sql.window"
            )
        if fullname in {"pyspark", "delta"}:
            loaded = sys.modules.get(root)
            marker = (
                "__brickhouse_worker_sparkless__"
                if root == "pyspark"
                else "__brickhouse_worker_delta__"
            )
            if loaded is not None and not getattr(loaded, marker, False):
                error_class = (
                    ConflictingPySparkPackageError
                    if root == "pyspark"
                    else ConflictingDeltaPackageError
                )
                raise error_class(
                    f"BrickHouse worker refuses to replace an already loaded {root} package"
                )
            installed = importlib.machinery.PathFinder.find_spec(root)
            if installed is not None:
                origin = installed.origin or "namespace package"
                if root == "pyspark":
                    raise ConflictingPySparkPackageError(
                        "BrickHouse worker refuses installed PySpark package "
                        f"({origin}); remove the conflicting pyspark package "
                        "to use Sparkless imports"
                    )
                raise ConflictingDeltaPackageError(
                    "BrickHouse worker refuses installed Delta Lake package "
                    f"({origin}); remove delta-spark to use the BrickHouse DeltaTable facade"
                )
        return importlib.machinery.ModuleSpec(
            fullname,
            self._loader,
            is_package=_SUPPORTED_MODULES[fullname],
        )


def install_worker_imports(
    *,
    spark_session_class: type[object] | None = None,
    spark_session_builder_class: type[object] | None = None,
) -> None:
    """Install lazy PySpark and DeltaTable imports in this child process only.

    Sparkless is imported only if user code requests a supported ``pyspark`` module.
    Conflicting installed packages are checked when the corresponding root import is
    requested, so jobs that do not use either compatibility surface keep their normal
    startup behavior.
    """
    global _INSTALLED_FINDER
    if _INSTALLED_FINDER is not None and _INSTALLED_FINDER in sys.meta_path:
        return
    preloaded = [
        name
        for name in sys.modules
        if name in {"pyspark", "delta"} or name.startswith(("pyspark.", "delta."))
    ]
    if preloaded:
        message = "BrickHouse worker refuses preloaded compatibility modules: " + ", ".join(
            sorted(preloaded)[:5]
        )
        if any(name == "delta" or name.startswith("delta.") for name in preloaded):
            raise ConflictingDeltaPackageError(message)
        raise ConflictingPySparkPackageError(message)
    finder = _WorkerImportFinder(
        spark_session_class=spark_session_class,
        spark_session_builder_class=spark_session_builder_class,
    )
    sys.meta_path.insert(0, finder)
    _INSTALLED_FINDER = finder
