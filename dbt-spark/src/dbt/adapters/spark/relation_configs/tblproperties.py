"""Tblproperties relation config for Spark Iceberg tables.

Supports structural change detection with explicit SET and UNSET operations
so that removed properties are cleaned up rather than left behind.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import agate


# CCCS prefixes for properties managed by the system that should never be compared or synced.
IGNORE_PREFIXES = ("polaris_",)

# CCCS exact property names that should also be ignored during comparison and sync.
IGNORE_PROPERTIES: List[str] = ["current-snapshot-id", "format", "format-version", "tblproperties"]


def _filter_properties(props: Dict[str, str]) -> Dict[str, str]:
    """Remove system-managed properties from a dict."""
    return {
        k: v
        for k, v in props.items()
        if not any(k.startswith(p) for p in IGNORE_PREFIXES) and k not in IGNORE_PROPERTIES
    }


@dataclass(frozen=True)
class TblPropertiesConfig:
    """Represents the desired or existing tblproperties for a relation."""

    tblproperties: Dict[str, str] = field(default_factory=dict)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, TblPropertiesConfig):
            return NotImplemented
        return _filter_properties(self.tblproperties) == _filter_properties(other.tblproperties)

    def __hash__(self) -> int:
        return hash(tuple(sorted(_filter_properties(self.tblproperties).items())))


@dataclass(frozen=True)
class TblPropertiesDiff:
    """The delta between desired and existing tblproperties.

    Attributes:
        set_properties: Properties to add or update via ALTER TABLE SET TBLPROPERTIES.
        unset_properties: Property keys to remove via ALTER TABLE UNSET TBLPROPERTIES.
    """

    set_properties: Dict[str, str] = field(default_factory=dict)
    unset_properties: List[str] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return bool(self.set_properties) or bool(self.unset_properties)


class TblPropertiesProcessor:
    """Builds TblPropertiesConfig from model config or SHOW TBLPROPERTIES output,
    and computes diffs between desired and existing states."""

    @classmethod
    def from_relation_results(cls, results: "agate.Table") -> TblPropertiesConfig:
        """Parse the output of SHOW TBLPROPERTIES into a config object.

        Args:
            results: agate.Table with columns (key, value).
        """
        tblproperties: Dict[str, str] = {}
        if results:
            for row in results.rows:
                key = str(row[0])
                value = str(row[1])
                if (
                    not any(key.startswith(p) for p in IGNORE_PREFIXES)
                    and key not in IGNORE_PROPERTIES
                ):
                    tblproperties[key] = value
        return TblPropertiesConfig(tblproperties=tblproperties)

    @classmethod
    def from_relation_config(cls, config_dict: Optional[Dict[str, str]]) -> TblPropertiesConfig:
        """Build config from the model's tblproperties dict.

        Args:
            config_dict: The raw tblproperties dict from model config, or None.
        """
        if config_dict is None:
            return TblPropertiesConfig(tblproperties={})
        return TblPropertiesConfig(
            tblproperties=_filter_properties({str(k): str(v) for k, v in config_dict.items()})
        )

    @classmethod
    def get_diff(
        cls, desired: TblPropertiesConfig, existing: TblPropertiesConfig
    ) -> Optional[TblPropertiesDiff]:
        """Compute the set/unset diff between desired and existing configs.

        Returns None if no changes are needed.
        """
        desired_filtered = _filter_properties(desired.tblproperties)
        existing_filtered = _filter_properties(existing.tblproperties)

        # Properties to set: new keys or changed values
        set_properties: Dict[str, str] = {}
        for k, v in desired_filtered.items():
            if k not in existing_filtered or existing_filtered[k] != v:
                set_properties[k] = v

        # Properties to unset: keys present in existing but absent in desired
        unset_properties: List[str] = [k for k in existing_filtered if k not in desired_filtered]

        diff = TblPropertiesDiff(set_properties=set_properties, unset_properties=unset_properties)
        if diff.has_changes:
            return diff
        return None
