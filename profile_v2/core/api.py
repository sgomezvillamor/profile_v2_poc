from abc import ABC, abstractmethod
from typing import List

from profile_v2.core.model import DataSource, ProfileRequest, ProfileResponse


class ProfileEngine(ABC):

    @abstractmethod
    def do_profile(
        self, datasource: DataSource, requests: List[ProfileRequest]
    ) -> ProfileResponse:
        pass
