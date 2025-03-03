from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, TypeAlias


class ProfileStatisticType(Enum):
    COLUMN_DISTINCT_COUNT = "column_distinct_count"
    TABLE_ROW_COUNT = "table_row_count"

    def is_table_level(self) -> bool:
        return self.value in [v.value for v in [ProfileStatisticType.TABLE_ROW_COUNT]]

    def is_column_level(self) -> bool:
        return not self.is_table_level()


StatisticFQName: TypeAlias = str
"""Fully qualified name of the statistic

It needs to be unique and fully identify the statistic in the catalogue considering all following dimensions: 
dataset, optional column, optional partitions, statistic 
"""


@dataclass
class StatisticSpec:
    fq_name: StatisticFQName


@dataclass
class CustomStatistic(StatisticSpec):
    sql: str


@dataclass
class TypedStatistic(StatisticSpec):
    type: ProfileStatisticType  # Type of the statistic
    columns: List[str] = field(
        default_factory=list
    )  # Target columns to calculate the statistic
    approximate: bool = False  # Whether to calculate the statistic approximately

    def __post_init__(self):
        if self.type.is_column_level() and not self.columns:
            raise ValueError(
                f"Column-level TypedStatistic of type {self.type} must set columns"
            )
        if self.type.is_table_level() and self.columns:
            raise ValueError(
                f"Table-level TypedStatistic of type {self.type} must not set columns"
            )


@dataclass
class SampleSpec:
    size: int


@dataclass
class PartitionSpec:
    column: str
    values: List[str]


@dataclass
class PartitionsSpec:
    columns: List[PartitionSpec]


class DataSourceType(Enum):
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"


@dataclass
class DataSource:
    source: DataSourceType
    connection_string: str  # eg: snowflake://<USER_NAME>:<PASSWORD>@<ACCOUNT_NAME>/<DATABASE_NAME>/<SCHEMA_NAME>?warehouse=<WAREHOUSE_NAME>&role=<ROLE_NAME>&application=datahub
    extra_config: Optional[Dict[str, Any]] = None


DatasetFQName: TypeAlias = str
"""Fully qualified dataset name
eg: bigquery "project.dataset.table"
eg: snowflake "database.schema.table"
"""


@dataclass
class BatchSpec:
    fq_dataset_name: DatasetFQName
    partitions: Optional[PartitionsSpec] = None  # Partitions specification
    sample: Optional[SampleSpec] = None  # Sample specification


@dataclass
class ProfileRequest:
    statistics: List[StatisticSpec]
    batch: BatchSpec


@dataclass
class StatisticResult:
    pass


@dataclass
class SuccessStatisticResult(StatisticResult):
    value: Any


class UnsuccessfulStatisticResultType(Enum):
    FAILURE = "failure"
    UNSUPPORTED = "unsupported"
    SKIPPED = "skipped"


@dataclass
class UnsuccessfulStatisticResult(StatisticResult):
    type: UnsuccessfulStatisticResultType
    message: Optional[str] = None
    exception: Optional[Exception] = None


@dataclass
class ProfileResponse:
    data: Dict[StatisticFQName, StatisticResult] = field(default_factory=dict)

    def update(self, other: "ProfileResponse"):
        self.data.update(other.data)
