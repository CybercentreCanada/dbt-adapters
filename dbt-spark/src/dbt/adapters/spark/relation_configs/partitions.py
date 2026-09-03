"""Partition config for Spark Iceberg tables.

Compares model partition_by config against the existing table partition spec
obtained from SHOW CREATE TABLE output.
"""

import re
from dataclasses import dataclass, field
from typing import List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import agate


# Regex to extract the PARTITIONED BY clause from SHOW CREATE TABLE output.
# Captures everything between PARTITIONED BY ( ... )
# Handles multi-line output and nested parentheses for transforms like bucket(16, id).
_PARTITIONED_BY_RE = re.compile(
    r"PARTITIONED\s+BY\s*\((.+?)\)\s*(?:TBLPROPERTIES|LOCATION|OPTIONS|COMMENT|$)",
    re.IGNORECASE | re.DOTALL,
)


def _parse_partition_spec(show_create_output: str) -> List[str]:
    """Parse the PARTITIONED BY clause from SHOW CREATE TABLE output.

    Returns a list of partition expressions in order, e.g.:
        ['days(ts)', 'bucket(16, id)', 'category']

    If no PARTITIONED BY clause is found, returns an empty list.
    """
    match = _PARTITIONED_BY_RE.search(show_create_output)
    if not match:
        return []

    raw = match.group(1).strip()
    # Split on commas that are NOT inside parentheses
    parts: List[str] = []
    depth = 0
    current: List[str] = []
    for char in raw:
        if char == "(":
            depth += 1
            current.append(char)
        elif char == ")":
            depth -= 1
            current.append(char)
        elif char == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(char)
    if current:
        parts.append("".join(current).strip())

    # Normalize: strip backticks and extra whitespace, lowercase
    normalized = []
    for part in parts:
        cleaned = part.replace("`", "").strip()
        # Collapse internal whitespace
        cleaned = re.sub(r"\s+", " ", cleaned).lower()
        if cleaned:
            normalized.append(cleaned)
    return normalized


def _normalize_model_partitions(partition_by: Optional[object]) -> List[str]:
    """Normalize the model's partition_by config to a list of lowercase strings.

    Handles:
    - None -> []
    - "col" -> ["col"]
    - ["col1", "col2"] -> ["col1", "col2"]
    """
    if partition_by is None:
        return []
    if isinstance(partition_by, str):
        return [partition_by.strip().lower()]
    if isinstance(partition_by, list):
        return [str(p).strip().lower() for p in partition_by if p]
    return []


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
    def from_show_create_table(cls, results: "agate.Table") -> PartitionConfig:
        """Parse SHOW CREATE TABLE output into a PartitionConfig.

        Args:
            results: agate.Table with the SHOW CREATE TABLE output (single row, single column).
        """
        if not results or not results.rows:
            return PartitionConfig(partition_columns=[])

        # SHOW CREATE TABLE returns a single row with the full DDL
        ddl = str(results.rows[0][0])
        partitions = _parse_partition_spec(ddl)
        return PartitionConfig(partition_columns=partitions)

    @classmethod
    def from_model_config(cls, partition_by: Optional[object]) -> PartitionConfig:
        """Build config from the model's partition_by value.

        Args:
            partition_by: The raw partition_by config (str, list, or None).
        """
        return PartitionConfig(partition_columns=_normalize_model_partitions(partition_by))

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
