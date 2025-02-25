from abc import ABC, abstractmethod
from profile_v2.core.model import (
    DataSource,
    ProfileRequest,
    ProfileResponse,
)

class ProfileEngine(ABC):

    @abstractmethod
    def do_profile(self, datasource: DataSource, request: ProfileRequest) -> ProfileResponse:
        pass
