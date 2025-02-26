from collections import defaultdict
from dataclasses import dataclass, field
from typing import (
    Any,
    Dict,
    List,
    Optional,
)
from enum import Enum
from typing import TypeAlias


class ProfileStatisticType(Enum):
    DISTINCT_COUNT = "distinct_count"

StatisticName: TypeAlias = str
StatisticFQName: TypeAlias = str

@dataclass
class StatisticSpec:
    name: StatisticName
    fq_name: StatisticFQName

@dataclass
class CustomStatistic(StatisticSpec):
    sql: str

@dataclass
class TypedStatistic(StatisticSpec):
    columns: List[str]  # Target columns to calculate the statistic
    type: ProfileStatisticType  # Type of the statistic
    approximate: bool = False  # Whether to calculate the statistic approximately

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

@dataclass
class DataSource:
    name: str  # TODO: enum?
    connection_string: str  # eg: snowflake://<USER_NAME>:<PASSWORD>@<ACCOUNT_NAME>/<DATABASE_NAME>/<SCHEMA_NAME>?warehouse=<WAREHOUSE_NAME>&role=<ROLE_NAME>&application=datahub

@dataclass
class BatchSpec:
    fully_qualified_dataset_name: str  # Fully qualified name for the target dataset
    partitions: Optional[PartitionsSpec] = None  # Partitions specification
    sample: Optional[SampleSpec] = None # Sample specification

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

class FailureStatisticResultType(Enum):
    FAILURE = "failure"
    UNSUPPORTED = "unsupported"

@dataclass
class FailureStatisticResult(StatisticResult):
    type: FailureStatisticResultType
    message: Optional[str] = None
    exception: Optional[Exception] = None

@dataclass
class ProfileResponse:
    data: Dict[StatisticFQName, StatisticResult] = field(default_factory=defaultdict)
