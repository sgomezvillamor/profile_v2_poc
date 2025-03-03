from abc import ABC, abstractmethod
from typing import List

from profile_v2.core.model import (DataSource,
                                   ProfileNonFunctionalRequirements,
                                   ProfileRequest, ProfileResponse,
                                   UnsuccessfulStatisticResultType)
from profile_v2.core.model_utils import ModelCollections
from profile_v2.core.report import ProfileCoreReport


class ProfileEngineException(Exception):
    pass


class ProfileEngineValueError(ProfileEngineException, ValueError):
    pass


class ProfileEngine(ABC):

    def __init__(self, report: ProfileCoreReport = ProfileCoreReport()):
        self.report = report

    def profile(
        self,
        datasource: DataSource,
        requests: List[ProfileRequest],
        non_functional_requirements: ProfileNonFunctionalRequirements = ProfileNonFunctionalRequirements(),
    ) -> ProfileResponse:
        self._requests_validations(requests)
        return self._do_profile(datasource, requests, non_functional_requirements)

    @abstractmethod
    def _do_profile(
        self,
        datasource: DataSource,
        requests: List[ProfileRequest],
        non_functional_requirements: ProfileNonFunctionalRequirements = ProfileNonFunctionalRequirements(),
    ) -> ProfileResponse:
        pass

    def _requests_validations(self, requests: List[ProfileRequest]) -> None:
        if not ModelCollections.validate_fq_statistic_name_uniqueness(requests):
            raise ProfileEngineValueError(
                "FQ statistic names must be unique across all requests"
            )

    def report_issue_query(self) -> None:
        self.report.issue_query(self.__class__.__name__)

    def report_successful_query(self) -> None:
        self.report.successful_query(self.__class__.__name__)

    def report_unsuccessful_query(
        self, status: UnsuccessfulStatisticResultType
    ) -> None:
        self.report.unsuccessful_query(self.__class__.__name__, status)
