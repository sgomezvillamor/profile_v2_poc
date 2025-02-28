from abc import ABC, abstractmethod
from typing import List

from profile_v2.core.model import DataSource, ProfileRequest, ProfileResponse
from profile_v2.core.model_utils import ModelCollections


class ProfileEngineException(Exception):
    pass


class ProfileEngineValueError(ProfileEngineException, ValueError):
    pass


class ProfileEngine(ABC):

    def profile(
        self, datasource: DataSource, requests: List[ProfileRequest]
    ) -> ProfileResponse:
        self._requests_validations(requests)
        return self._do_profile(datasource, requests)

    @abstractmethod
    def _do_profile(
        self, datasource: DataSource, requests: List[ProfileRequest]
    ) -> ProfileResponse:
        pass

    def _requests_validations(self, requests: List[ProfileRequest]) -> None:
        if not ModelCollections.validate_fq_statistic_name_uniqueness(requests):
            raise ProfileEngineValueError(
                "FQ statistic names must be unique across all requests"
            )
