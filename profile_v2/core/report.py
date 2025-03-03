from collections import defaultdict
from dataclasses import dataclass, field
from threading import Lock
from typing import Dict, Tuple, TypeAlias

from profile_v2.core.model import UnsuccessfulStatisticResultType

EngineName: TypeAlias = str


@dataclass
class ProfileCoreReport:
    """Thread-safe report for the profile core."""

    num_issued_queries_by_engine: Dict[EngineName, int] = field(
        default_factory=lambda: defaultdict(int)
    )
    num_successful_queries_by_engine: Dict[EngineName, int] = field(
        default_factory=lambda: defaultdict(int)
    )
    num_unsuccessful_queries_by_engine_and_status: Dict[
        Tuple[EngineName, UnsuccessfulStatisticResultType], int
    ] = field(default_factory=lambda: defaultdict(int))

    _lock: Lock = Lock()

    def issue_query(self, engine: EngineName) -> None:
        with self._lock:
            self.num_issued_queries_by_engine[engine] += 1

    def successful_query(self, engine: EngineName) -> None:
        with self._lock:
            self.num_successful_queries_by_engine[engine] += 1

    def unsuccessful_query(
        self, engine: EngineName, status: UnsuccessfulStatisticResultType
    ) -> None:
        with self._lock:
            self.num_unsuccessful_queries_by_engine_and_status[(engine, status)] += 1

    def __repr__(self) -> str:
        return (
            f"ProfileCoreReport("
            f"num_issued_queries_by_engine={dict(self.num_issued_queries_by_engine)}, "
            f"num_successful_queries_by_engine={dict(self.num_successful_queries_by_engine)}, "
            f"num_unsuccessful_queries_by_engine_and_status={dict({(k[0], k[1].value): v for k, v in self.num_unsuccessful_queries_by_engine_and_status.items()})})"
        )
