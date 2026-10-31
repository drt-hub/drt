"""Pure first-batch validation for portable sync column contracts (#761).

Python values map to the public portable types as follows:

* ``str`` -> ``string``
* ``bool`` -> ``boolean`` (checked before ``int``; bool is never a number)
* ``int`` -> ``integer`` and is also compatible with ``number``
* ``float`` and ``decimal.Decimal`` -> ``number``
* ``datetime.datetime`` -> ``timestamp``
* ``datetime.date`` -> ``date``
* ``dict`` and ``list`` -> ``json``
* ``None`` is compatible with every declared type

Other Python types retain their class name in a violation so the operator can
see what the connector actually returned. The module performs no I/O and has
no observer dependency; the sync engine owns reporting and control flow.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal

from drt.config.models import ColumnConfig

PortableType = Literal["string", "number", "integer", "boolean", "timestamp", "date", "json"]
ExtraColumnsPolicy = Literal["fail", "ignore", "drop"]


@dataclass(frozen=True)
class ContractViolation:
    column: str
    expected: str
    actual: str


@dataclass(frozen=True)
class ContractCheck:
    violations: tuple[ContractViolation, ...] = ()

    @property
    def passed(self) -> bool:
        return not self.violations

    def message(self) -> str:
        details = "; ".join(
            f"{v.column!r} expected {v.expected}, actual {v.actual}" for v in self.violations
        )
        return f"Column contract violation: {details}."


def python_value_type(value: Any) -> str:
    """Return the portable name for a supported value, or its Python class name."""
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, str):
        return "string"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, (float, Decimal)):
        return "number"
    if isinstance(value, datetime):
        return "timestamp"
    if isinstance(value, date):
        return "date"
    if isinstance(value, (dict, list)):
        return "json"
    return type(value).__name__


def _compatible(expected: PortableType, value: Any) -> bool:
    actual = python_value_type(value)
    return actual == expected or (expected == "number" and actual == "integer")


def check_contract(
    records: Sequence[dict[str, Any]],
    columns: Sequence[ColumnConfig],
    on_extra_columns: ExtraColumnsPolicy,
) -> ContractCheck:
    """Compare a first extracted batch with a declared portable schema.

    Keys are unioned across the batch so sparse dictionaries do not make a
    column disappear merely because the first row omitted it. Types are checked
    for every non-null value in the batch, while duplicate actual type names are
    collapsed into one deterministic diagnostic.
    """
    declared = {column.name: column.type for column in columns}
    actual_names = {name for record in records for name in record}
    violations: list[ContractViolation] = []

    for name, expected in declared.items():
        if name not in actual_names:
            violations.append(ContractViolation(name, expected, "missing"))
            continue
        incompatible = sorted(
            {
                python_value_type(record[name])
                for record in records
                if name in record
                and record[name] is not None
                and not _compatible(expected, record[name])
            }
        )
        if incompatible:
            violations.append(ContractViolation(name, expected, " | ".join(incompatible)))

    if on_extra_columns == "fail":
        for name in sorted(actual_names - declared.keys()):
            actual_types = sorted(
                {
                    python_value_type(record[name])
                    for record in records
                    if name in record and record[name] is not None
                }
            )
            violations.append(
                ContractViolation(name, "not declared", " | ".join(actual_types) or "null")
            )

    return ContractCheck(tuple(violations))


def drop_extra_columns(
    records: Sequence[dict[str, Any]], columns: Sequence[ColumnConfig]
) -> list[dict[str, Any]]:
    """Return fresh records containing only declared keys, preserving key order."""
    declared = {column.name for column in columns}
    return [
        {name: value for name, value in record.items() if name in declared} for record in records
    ]
