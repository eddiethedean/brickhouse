"""Optional execution engine adapters.

Adapters load their engine dependency only when a session is created so the
base BrickHouse package remains dependency-free.
"""

from brickhouse.engines.sparkless import (
    SparklessAdapterError,
    SparklessSession,
    SparklessSessionFactory,
    UnsupportedDeltaSchemaError,
    UnsupportedSparklessConfigurationError,
    UnsupportedSparklessSessionOperationError,
)

__all__ = [
    "SparklessAdapterError",
    "SparklessSession",
    "SparklessSessionFactory",
    "UnsupportedDeltaSchemaError",
    "UnsupportedSparklessConfigurationError",
    "UnsupportedSparklessSessionOperationError",
]
