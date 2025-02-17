from collections import defaultdict
from dataclasses import dataclass, field
from typing import (
    Dict,
    List,
    Optional,
)
from enum import Enum


class ProfileStatisticType(Enum):
    DISTINCT_COUNT = "distinct_count"

@dataclass
class StatisticSpec:
    name: str  # Name of the statistic

@dataclass
class CustomStatistic(StatisticSpec):
    sql: str

@dataclass
class TypedStatistic(StatisticSpec):
    columns: List[str]  # Target columns to calculate the statistic
    statistic: ProfileStatisticType  # Type of the statistic
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
    name: str  # TODO: enum
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
class ProfileResponse:
    # TODO: Define the response data structure
    # - oversimplified by assuming all statistics results are a single float value
    data: Dict[str, float] = field(default_factory=defaultdict) # key = StatisticSpec.name, value = calculated statistic
    errors: List[str] = field(default_factory=list)


