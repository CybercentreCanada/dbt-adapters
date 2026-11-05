"""Partition config for Spark Iceberg tables.

Compares model partition_by config against the existing table partition spec
obtained from SHOW CREATE TABLE output.
"""

import re
from dataclasses import dataclass, field
from typing import List, Optional, TYPE_CHECKING

import sqlglot
from sqlglot import exp
from sqlglot.dialects.spark import Spark
from sqlglot.tokens import TokenType

from dbt_common.exceptions import DbtRuntimeError
from dbt.adapters.spark.relation_configs.write_order import quote_identifier

if TYPE_CHECKING:
    import agate


class _SparkPartitionDialect(Spark):
    class Parser(Spark.Parser):
        # Spark emits OPTIONS in table DDL; SQLGlot's Spark property parser omits it.
        PROPERTY_PARSERS = {
            **Spark.Parser.PROPERTY_PARSERS,
            "OPTIONS": lambda self: self._parse_wrapped_properties(),
        }


def _normalize_expression(expression: exp.Expression, case_sensitive: bool) -> str:
    if isinstance(expression, exp.Identifier):
        name = expression.name if case_sensitive else expression.name.lower()
        return name if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) else quote_identifier(name)
    if isinstance(expression, exp.Column):
        return ".".join(_normalize_expression(part, case_sensitive) for part in expression.parts)
    if isinstance(expression, exp.Dot):
        return (
            _normalize_expression(expression.this, case_sensitive)
            + "."
            + _normalize_expression(expression.expression, case_sensitive)
        )
    if isinstance(expression, (exp.PartitionedByBucket, exp.PartitionByTruncate)):
        width = expression.expression
        if (
            not isinstance(width, exp.Literal)
            or width.is_string
            or not re.fullmatch(r"[1-9][0-9]*", width.this)
        ):
            raise DbtRuntimeError("Invalid partition transform width; expected a positive integer")
        name = "bucket" if isinstance(expression, exp.PartitionedByBucket) else "truncate"
        return f"{name}({width.this}, {_normalize_expression(expression.this, case_sensitive)})"
    if isinstance(expression, exp.Anonymous):
        name = expression.name.lower()
        if name in ("years", "months", "days", "hours") and len(expression.expressions) == 1:
            return f"{name}({_normalize_expression(expression.expressions[0], case_sensitive)})"
    raise DbtRuntimeError(f"Invalid or unsupported partition expression: {expression}")


def _parse_partition_spec(show_create_output: str, *, case_sensitive: bool = False) -> List[str]:
    """Parse and normalize Iceberg partition expressions without losing identifier boundaries."""
    try:
        statements = sqlglot.parse(show_create_output, read=_SparkPartitionDialect)
        if len(statements) != 1 or not isinstance(statements[0], exp.Create):
            raise DbtRuntimeError("Invalid partition metadata; expected CREATE TABLE DDL")
        ddl = statements[0]
        if ddl.kind != "TABLE":
            raise DbtRuntimeError("Invalid partition metadata; expected CREATE TABLE DDL")
        clauses = list(ddl.find_all(exp.PartitionedByProperty))
        if not clauses:
            return []
        if len(clauses) != 1 or not clauses[0].this.expressions:
            raise DbtRuntimeError("Invalid partition metadata; expected non-empty PARTITIONED BY")
        # SQLGlot accepts trailing commas, but they are not valid partition expressions.
        tokens = sqlglot.Dialect.get_or_raise("spark").tokenize(show_create_output)
        if any(
            left.token_type == TokenType.COMMA and right.token_type == TokenType.R_PAREN
            for left, right in zip(tokens, tokens[1:])
        ):
            raise DbtRuntimeError("Invalid partition metadata; trailing comma")
        return [
            _normalize_expression(expression, case_sensitive)
            for expression in clauses[0].this.expressions
        ]
    except sqlglot.errors.SqlglotError as exc:
        raise DbtRuntimeError(f"Unable to parse partition metadata: {exc}") from exc


def _normalize_model_partitions(
    partition_by: Optional[object], *, case_sensitive: bool = False
) -> List[str]:
    """Normalize each configured expression using the database partition grammar."""
    if partition_by is None:
        return []
    values = [partition_by] if isinstance(partition_by, str) else partition_by
    if not isinstance(values, list):
        raise DbtRuntimeError("Invalid partition_by config; expected a string or list of strings")
    normalized = []
    for value in values:
        if not isinstance(value, str) or not value.strip():
            raise DbtRuntimeError("Invalid partition_by config; expected non-empty expressions")
        parsed = _parse_partition_spec(
            f"CREATE TABLE __dbt_partition_check USING iceberg PARTITIONED BY (\n{value}\n)",
            case_sensitive=case_sensitive,
        )
        if len(parsed) != 1:
            raise DbtRuntimeError("Invalid partition_by config; expected one expression per entry")
        normalized.extend(parsed)
    return normalized


@dataclass(frozen=True)
class PartitionConfig:
    """Represents the partition spec of a table."""

    partition_columns: List[str] = field(default_factory=list)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PartitionConfig):
            return NotImplemented
        return self.partition_columns == other.partition_columns

    def __hash__(self) -> int:
        return hash(tuple(self.partition_columns))


class PartitionProcessor:
    """Builds PartitionConfig from model config or SHOW CREATE TABLE output,
    and compares them to detect drift."""

    @classmethod
    def from_show_create_table(
        cls, results: "agate.Table", *, case_sensitive: bool = False
    ) -> PartitionConfig:
        """Parse SHOW CREATE TABLE output into a PartitionConfig.

        Args:
            results: agate.Table with the SHOW CREATE TABLE output (single row, single column).
        """
        if results is None or len(results.rows) != 1 or len(results.rows[0]) != 1:
            raise DbtRuntimeError(
                "Invalid partition metadata; expected one SHOW CREATE TABLE row and column"
            )

        # SHOW CREATE TABLE returns a single row with the full DDL
        ddl = results.rows[0][0]
        if not isinstance(ddl, str) or not ddl.strip():
            raise DbtRuntimeError("Invalid partition metadata; expected CREATE TABLE DDL")
        partitions = _parse_partition_spec(ddl, case_sensitive=case_sensitive)
        return PartitionConfig(partition_columns=partitions)

    @classmethod
    def from_model_config(
        cls, partition_by: Optional[object], *, case_sensitive: bool = False
    ) -> PartitionConfig:
        """Build config from the model's partition_by value.

        Args:
            partition_by: The raw partition_by config (str, list, or None).
        """
        return PartitionConfig(
            partition_columns=_normalize_model_partitions(
                partition_by, case_sensitive=case_sensitive
            )
        )

    @classmethod
    def is_out_of_sync(cls, desired: PartitionConfig, existing: PartitionConfig) -> bool:
        """Check if partitions are out of sync (order-sensitive comparison)."""
        return desired != existing

    @classmethod
    def describe_diff(cls, desired: PartitionConfig, existing: PartitionConfig) -> str:
        """Return a human-readable description of the partition difference."""
        return (
            f"Model partition_by config: {desired.partition_columns}\n"
            f"Existing table partitions: {existing.partition_columns}"
        )
