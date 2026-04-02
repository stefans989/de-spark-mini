from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from pyspark.sql import DataFrame, functions as F


@dataclass(frozen=True)
class QualityIssue:
    rule: str
    bad_count: int


def check_not_null(df: DataFrame, col: str) -> QualityIssue:
    bad = df.filter(F.col(col).isNull()).count()
    return QualityIssue(rule=f"not_null({col})", bad_count=bad)


def check_contains(df: DataFrame, col: str, needle: str) -> QualityIssue:
    bad = df.filter(~F.col(col).contains(needle) | F.col(col).isNull()).count()
    return QualityIssue(rule=f"contains({col}, '{needle}')", bad_count=bad)


def check_in_set(df: DataFrame, col: str, allowed: Iterable[str]) -> QualityIssue:
    allowed_list = list(allowed)
    bad = df.filter((F.col(col).isNull()) | (~F.col(col).isin(allowed_list))).count()
    return QualityIssue(rule=f"in_set({col}, {allowed_list})", bad_count=bad)


def assert_quality(issues: list[QualityIssue]) -> None:
    failed = [i for i in issues if i.bad_count > 0]
    if failed:
        details = ", ".join([f"{i.rule} failed: {i.bad_count}" for i in failed])
        raise ValueError(f"Data quality checks failed: {details}")
