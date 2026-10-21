"""Temporary Sparkless session probe; research evidence, not a CI test."""

from __future__ import annotations

from sparkless.sql import SparkSession


def main() -> None:
    base = SparkSession.builder.appName("b0033-session-isolation").getOrCreate()
    left = base.newSession()
    left.conf.set("spark.b0033.marker", "left")
    left.createDataFrame([(1,)], ["value"]).createOrReplaceTempView("isolated_values")

    # newSession() makes this the active session on the current thread. Keep
    # DataFrame view registration adjacent to session creation because the
    # current PyO3 convenience method consults that thread-local active stack.
    right = left.newSession()
    right.conf.set("spark.b0033.marker", "right")
    right.createDataFrame([(2,)], ["value"]).createOrReplaceTempView("isolated_values")

    assert left.conf.get("spark.b0033.marker") == "left"
    assert right.conf.get("spark.b0033.marker") == "right"
    assert left.sql("select value from isolated_values").collect() == [(1,)]
    assert right.sql("select value from isolated_values").collect() == [(2,)]

    try:
        left.sql("select missing_column from isolated_values").collect()
    except Exception as exc:
        assert type(exc).__name__ == "SparklessError"
        print(f"invalid SQL raised {type(exc).__name__}: {exc}")
    else:
        raise AssertionError("invalid SQL unexpectedly succeeded")

    print("same-thread session config/view isolation: passed")
    for session in (right, left, base):
        session.stop()


if __name__ == "__main__":
    main()
