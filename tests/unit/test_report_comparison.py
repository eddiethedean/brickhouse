from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest


def test_report_comparison_allows_float_rounding_but_keeps_values_bounded(
    assert_report_matches: Callable[[Any, Any], None],
) -> None:
    assert_report_matches(
        {"groups": [{"mean_ms": 0.1 + 0.2}], "samples": 150},
        {"groups": [{"mean_ms": 0.3}], "samples": 150},
    )

    with pytest.raises(AssertionError, match="report.groups\\[0\\].mean_ms"):
        assert_report_matches(
            {"groups": [{"mean_ms": 0.3 + 1e-10}], "samples": 150},
            {"groups": [{"mean_ms": 0.3}], "samples": 150},
        )
