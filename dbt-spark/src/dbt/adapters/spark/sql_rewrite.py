from collections.abc import Mapping

import sqlglot
from sqlglot import exp

from dbt_common.exceptions import DbtRuntimeError


def rewrite_streaming_sql(compiled_sql: str, stream_inputs: Mapping[str, str]) -> str:
    """Replace declared streaming input relations with local temporary views."""
    try:
        expression = sqlglot.parse_one(compiled_sql, read="spark")
    except sqlglot.errors.ParseError as exc:
        raise DbtRuntimeError(f"Unable to parse SQL streaming model: {exc}") from exc

    normalized_inputs = {
        _normalize_relation(relation): temporary_view
        for relation, temporary_view in stream_inputs.items()
    }
    matched_relations: set[str] = set()

    for table in expression.find_all(exp.Table):
        relation = _normalize_table(table)
        temporary_view = normalized_inputs.get(relation)
        if temporary_view is not None:
            replacement = exp.to_table(temporary_view, dialect="spark")
            replacement.set("alias", table.args.get("alias"))
            table.replace(replacement)
            matched_relations.add(relation)

    unmatched_relations = set(normalized_inputs) - matched_relations
    if unmatched_relations:
        raise DbtRuntimeError(
            "SQL streaming model did not reference declared streaming inputs: "
            + ", ".join(sorted(unmatched_relations))
        )

    return expression.sql(dialect="spark")


def _normalize_relation(relation: str) -> str:
    try:
        expression = sqlglot.parse_one(f"SELECT * FROM {relation}", read="spark")
        table = expression.find(exp.Table)
    except sqlglot.errors.ParseError as exc:
        raise DbtRuntimeError(
            f"Unable to parse streaming input relation '{relation}': {exc}"
        ) from exc

    if table is None:
        raise DbtRuntimeError(f"Unable to parse streaming input relation '{relation}'")

    return _normalize_table(table)


def _normalize_table(table: exp.Table) -> str:
    normalized_table = table.copy()
    normalized_table.set("alias", None)
    return normalized_table.sql(dialect="spark")
