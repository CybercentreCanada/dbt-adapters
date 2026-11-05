"""Write-order contracts and SQL lifecycle shared with generated Python jobs.

This module deliberately depends only on the standard library: Python submission
environments need not have dbt installed.
"""

import json
import re
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple


_IDENTIFIER = r"(?:`(?:``|[^`])+`|[A-Za-z_][A-Za-z0-9_]*)"
_TERM = re.compile(
    rf"^\s*(?P<column>{_IDENTIFIER}(?:\s*\.\s*{_IDENTIFIER})*)"
    r"(?:\s+(?P<direction>ASC|DESC))?"
    r"(?:\s+NULLS\s+(?P<nulls>FIRST|LAST))?\s*$",
    re.IGNORECASE,
)
_PART = re.compile(_IDENTIFIER)


def quote_identifier(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


@dataclass(frozen=True)
class SortTerm:
    column: Tuple[str, ...]
    direction: str = "ASC"
    nulls: str = "FIRST"
    transform: str = "identity"

    @property
    def sql(self) -> str:
        if self.transform != "identity":
            raise ValueError("Cannot render a non-column Iceberg sort transform")
        return (
            ".".join(quote_identifier(part) for part in self.column)
            + f" {self.direction} NULLS {self.nulls}"
        )


@dataclass(frozen=True)
class WriteOrder:
    terms: Tuple[SortTerm, ...] = ()

    @classmethod
    def from_config(cls, value: object) -> "WriteOrder":
        if not isinstance(value, list):
            raise ValueError("write_order must be a list of column sort terms (or [])")
        terms = []
        seen = set()
        for entry in value:
            match = _TERM.fullmatch(entry) if isinstance(entry, str) else None
            if match is None:
                raise ValueError(
                    f"Invalid write_order term {entry!r}; expected a column sort term"
                )
            parts = tuple(
                part[1:-1].replace("``", "`") if part.startswith("`") else part
                for part in _PART.findall(match["column"])
            )
            if parts in seen:
                raise ValueError(f"Duplicate write_order column {entry!r}")
            seen.add(parts)
            direction = (match["direction"] or "ASC").upper()
            nulls = (match["nulls"] or ("FIRST" if direction == "ASC" else "LAST")).upper()
            terms.append(SortTerm(parts, direction, nulls))
        return cls(tuple(terms))

    @property
    def sql(self) -> str:
        return ", ".join(term.sql for term in self.terms)

    def alter_sql(self, target: str) -> str:
        policy = "ORDERED BY " + self.sql if self.terms else "UNORDERED"
        return f"ALTER TABLE {target} WRITE {policy}"

    def validate_identifiers(self, case_sensitive: bool) -> None:
        columns = [
            term.column if case_sensitive else tuple(part.lower() for part in term.column)
            for term in self.terms
        ]
        if len(set(columns)) != len(columns):
            raise ValueError("Duplicate write_order column under Spark identifier resolution")


@dataclass(frozen=True)
class CurrentWriteOrder:
    order: Optional[WriteOrder] = None
    source: str = "unavailable"
    display: Optional[str] = None

    @property
    def state(self) -> str:
        if self.order is None:
            return "unknown"
        return "ordered" if self.order.terms else "unordered"

    @classmethod
    def from_describe(cls, rows: Sequence[Sequence[Any]]) -> "CurrentWriteOrder":
        metadata = {str(row[0]).strip(): row[1] for row in rows}
        if metadata.get("Provider") != "iceberg":
            return cls()
        properties = str(metadata.get("Table Properties", ""))
        complete = (
            "# Detailed Table Information" in metadata
            and properties.startswith("[")
            and properties.endswith("]")
            and re.search(r"(?:\[|,\s*)format-version=[23](?:,|])", properties)
            and "redacted" not in properties.lower()
            and "********" not in properties
        )
        if complete:
            match = re.search(r"(?:\[|,\s*)sort-order=(.*?)(?=,\s*[\w.-]+=|]$)", properties)
            if match:
                entries = match[1].split(", ")
                if all(_TERM.fullmatch(entry) for entry in entries):
                    try:
                        order = WriteOrder.from_config(entries)
                    except ValueError:
                        return cls(source="describe", display=properties)
                    # DESCRIBE does not quote source names. Only single, regular
                    # column names can be safely interpreted without schema IDs.
                    if all(
                        len(term.column) == 1
                        and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", term.column[0])
                        and term.column[0] in metadata
                        for term in order.terms
                    ):
                        return cls(order, "describe", match[1])
            elif "sort-order" not in properties:
                return cls(WriteOrder(), "describe", properties)
        return cls(source="describe", display=properties)


def metadata_order(metadata: Dict[str, Any]) -> WriteOrder:
    schema = next(
        schema
        for schema in metadata["schemas"]
        if schema["schema-id"] == metadata["current-schema-id"]
    )
    paths: Dict[int, Tuple[str, ...]] = {}

    def visit(fields: List[Dict[str, Any]], prefix: Tuple[str, ...] = ()) -> None:
        for field in fields:
            path = prefix + (field["name"],)
            paths[field["id"]] = path
            dtype = field["type"]
            if isinstance(dtype, dict) and dtype.get("type") == "struct":
                visit(dtype["fields"], path)

    visit(schema["fields"])
    order = next(
        order
        for order in metadata["sort-orders"]
        if order["order-id"] == metadata["default-sort-order-id"]
    )
    return WriteOrder(
        tuple(
            SortTerm(
                paths[field["source-id"]],
                field["direction"].upper(),
                field["null-order"].removeprefix("nulls-").upper(),
                field["transform"],
            )
            for field in order["fields"]
        )
    )


class WriteOrderManager:
    def __init__(self, execute: Callable[[str], Sequence[Sequence[Any]]]) -> None:
        self.execute = execute

    def supported(self, explicit: bool) -> bool:
        version = str(self.execute("SELECT version()")[0][0])
        supported = re.match(r"^4\.0\.", version) is not None
        if explicit and not supported:
            raise ValueError(f"write_order requires certified Spark 4.0.x; found {version}")
        return supported

    def metadata(self, target: str) -> Dict[str, Any]:
        rows = self.execute(
            f"SELECT file FROM {target}.metadata_log_entries ORDER BY timestamp DESC LIMIT 1"
        )
        if not rows:
            raise ValueError(f"Cannot establish authoritative Iceberg metadata for {target}")
        path = str(rows[0][0])
        view = quote_identifier("__dbt_write_order_metadata_" + uuid.uuid4().hex)
        self.execute(
            f"CREATE TEMPORARY VIEW {view} USING text "
            f"OPTIONS (path {sql_string(path)}, wholetext 'true')"
        )
        try:
            result = self.execute(f"SELECT value FROM {view}")
            if len(result) != 1 or not result[0][0]:
                raise ValueError(f"Invalid authoritative Iceberg metadata for {target}")
            metadata = json.loads(str(result[0][0]))
            # Resolve eagerly so malformed metadata cannot be interpreted as unordered.
            if metadata["format-version"] in (2, 3):
                metadata_order(metadata)
            return metadata
        finally:
            self.execute(f"DROP VIEW {view}")

    def current(self, target: str) -> CurrentWriteOrder:
        rows = self.execute(f"DESCRIBE TABLE EXTENDED {target}")
        described = CurrentWriteOrder.from_describe(rows)
        if described.source != "describe":
            raise ValueError(f"write_order target {target} is not an Iceberg table")
        return CurrentWriteOrder(
            metadata_order(self.metadata(target)),
            "authoritative_iceberg_metadata",
            described.display,
        )

    def enabled(self, target: Optional[str], explicit: bool, properties: Dict[str, str]) -> bool:
        if not self.supported(explicit):
            return False
        if target:
            rows = self.execute(f"DESCRIBE TABLE EXTENDED {target}")
            if CurrentWriteOrder.from_describe(rows).source != "describe":
                if explicit:
                    raise ValueError(f"write_order target {target} is not an Iceberg table")
                return False
            metadata = self.metadata(target)
            version = metadata["format-version"]
        else:
            version = int(properties.get("format-version", "2"))
        if version not in (2, 3):
            if explicit:
                raise ValueError(f"write_order requires Iceberg format v2/v3; found v{version}")
            return False
        extensions = str(self.execute("SET spark.sql.extensions")[0][1])
        if "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" not in extensions:
            raise ValueError("write_order requires preconfigured Iceberg Spark SQL extensions")
        return True

    def sync(self, target: str, desired: WriteOrder, force: bool = False) -> CurrentWriteOrder:
        current = self.current(target)
        # Spark resolves identifiers using its session case-sensitivity setting.
        case_sensitive = str(self.execute("SET spark.sql.caseSensitive")[0][1]).lower() == "true"
        desired.validate_identifiers(case_sensitive)
        if desired.terms:
            self.execute(f"SELECT * FROM {target} ORDER BY {desired.sql} LIMIT 0")
        if not case_sensitive:

            def normalized(order: WriteOrder) -> Tuple[Tuple[Any, ...], ...]:
                return tuple(
                    (
                        tuple(part.lower() for part in term.column),
                        term.direction,
                        term.nulls,
                        term.transform,
                    )
                    for term in order.terms
                )

            same = current.order is not None and normalized(current.order) == normalized(desired)
        else:
            same = current.order == desired
        if force or not same:
            self.execute(desired.alter_sql(target))
            return self.current(target)
        return current

    def require_atomic_catalog(self, target: str) -> None:
        parts = _PART.findall(target)
        catalog = (
            parts[0] if len(parts) >= 3 else str(self.execute("SELECT current_catalog()")[0][0])
        )
        catalog = catalog[1:-1].replace("``", "`") if catalog.startswith("`") else catalog
        setting = str(
            self.execute(f"SET {quote_identifier('spark.sql.catalog.' + catalog)}")[0][1]
        )
        if setting != "org.apache.iceberg.spark.SparkCatalog":
            raise ValueError(
                f"write_order replacement requires an atomic Iceberg SparkCatalog; found {setting}"
            )

    def drop_owned(self, target: str, owner: str) -> None:
        properties = {
            str(row[0]): str(row[1]) for row in self.execute(f"SHOW TBLPROPERTIES {target}")
        }
        if properties.get("dbt.write-order-owner") != owner:
            raise ValueError(f"Refusing to clean up {target}: write_order ownership changed")
        self.execute(f"DROP TABLE {target}")

    def validate_on_probe(
        self,
        target: str,
        desired: WriteOrder,
        create_empty: Callable[[str, str], None],
    ) -> str:
        case_sensitive = str(self.execute("SET spark.sql.caseSensitive")[0][1]).lower() == "true"
        desired.validate_identifiers(case_sensitive)
        owner = uuid.uuid4().hex
        namespace = ".".join(_PART.findall(target)[:-1])
        probe = (namespace + "." if namespace else "") + quote_identifier(
            "__dbt_write_order_probe_" + owner
        )
        create_empty(probe, owner)
        try:
            self.execute(desired.alter_sql(probe))
        except Exception as exc:
            try:
                self.drop_owned(probe, owner)
            except Exception as cleanup:
                raise RuntimeError(
                    f"write_order validation failed: {exc}; probe cleanup failed: {cleanup}"
                ) from exc
            raise
        else:
            self.drop_owned(probe, owner)
        return owner

    def materialize(
        self,
        target: str,
        desired: WriteOrder,
        exists: bool,
        create_empty: Callable[[str, str], None],
        write: Callable[[bool], None],
    ) -> None:
        """Validate on an owned empty probe before touching the user-facing target."""
        if exists:
            self.require_atomic_catalog(target)
        owner = self.validate_on_probe(target, desired, create_empty)

        if exists:
            write(True)
            try:
                self.sync(target, desired)
            except Exception as exc:
                raise RuntimeError(
                    f"Replacement of {target} succeeded but write_order synchronization failed: {exc}"
                ) from exc
            return

        # CREATE without IF NOT EXISTS claims ownership atomically. A concurrent
        # creator causes CREATE to fail outside the cleanup block.
        create_empty(target, owner)
        try:
            self.sync(target, desired, force=True)
            write(False)
        except Exception as exc:
            try:
                self.drop_owned(target, owner)
            except Exception as cleanup:
                raise RuntimeError(
                    f"write_order creation of {target} failed: {exc}; cleanup failed: {cleanup}"
                ) from exc
            raise
        self.execute(f"ALTER TABLE {target} UNSET TBLPROPERTIES ('dbt.write-order-owner')")
