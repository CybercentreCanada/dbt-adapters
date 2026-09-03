from dbt.adapters.spark.relation_configs.tblproperties import (
    TblPropertiesConfig,
    TblPropertiesProcessor,
    TblPropertiesDiff,
)
from dbt.adapters.spark.relation_configs.partitions import (
    PartitionConfig,
    PartitionProcessor,
)

__all__ = [
    "TblPropertiesConfig",
    "TblPropertiesProcessor",
    "TblPropertiesDiff",
    "PartitionConfig",
    "PartitionProcessor",
]
