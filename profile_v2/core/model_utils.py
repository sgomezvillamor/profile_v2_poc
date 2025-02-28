import logging
from collections import defaultdict
from typing import Callable, Dict, List, Optional, Type, TypeVar

from profile_v2.core.model import (BatchSpec, ProfileRequest, ProfileResponse,
                                   StatisticResult, StatisticSpec,
                                   SuccessStatisticResult,
                                   UnsuccessfulStatisticResult,
                                   UnsuccessfulStatisticResultType)

logger = logging.getLogger(__name__)

PredicateResponse = TypeVar("PredicateResponse")


class ModelCollections:

    @staticmethod
    def validate_fq_statistic_name_uniqueness(
        requests: List[ProfileRequest],
    ) -> bool:
        """
        Validates that the fq statistic names are unique across all requests.
        :param requests:
        :return:
        """
        fq_statistic_names = set()
        for request in requests:
            for statistic in request.statistics:
                fq_name = statistic.fq_name
                if fq_name in fq_statistic_names:
                    return False
                fq_statistic_names.add(fq_name)
        return True

    @staticmethod
    def group_request_by_statistics_predicate(
        requests: List[ProfileRequest],
        predicate: Callable[[StatisticSpec], PredicateResponse],
        group_results: bool = True,
    ) -> Dict[PredicateResponse, List[ProfileRequest]]:
        """
        Groups requests by the result of the predicate applied to the statistic spec.
        :param requests:
        :param predicate:
        :param group_results:
        :return:
        """
        requests_by_predicate: Dict[PredicateResponse, List[ProfileRequest]] = (
            defaultdict(list)
        )
        for request in requests:
            for statistic in request.statistics:
                key = predicate(statistic)
                requests_by_predicate[key].append(
                    ProfileRequest(statistics=[statistic], batch=request.batch)
                )

        if group_results:
            for key, value in requests_by_predicate.items():
                requests_by_predicate[key] = ModelCollections.join_statistics_by_batch(
                    value
                )

        return requests_by_predicate

    @staticmethod
    def join_statistics_by_batch(
        requests: List[ProfileRequest],
    ) -> List[ProfileRequest]:
        """
        Joins statistics across input requests if they share the same batch.
        :param requests:
        :return:
        """
        grouped_requests: List[ProfileRequest] = []
        for request in requests:
            found = False
            for grouped_request in grouped_requests:
                if request.batch == grouped_request.batch:
                    grouped_request.statistics.extend(request.statistics)
                    found = True
                    break
            if not found:
                grouped_requests.append(request)
        return grouped_requests

    @staticmethod
    def group_requests_by_batch_predicate(
        requests: List[ProfileRequest],
        predicate: Callable[[BatchSpec], PredicateResponse],
    ) -> Dict[PredicateResponse, List[ProfileRequest]]:
        """
        Groups requests by the result of the predicate applied to the batch spec.
        :param requests:
        :param predicate:
        :return:
        """
        requests_by_predicate: Dict[PredicateResponse, List[ProfileRequest]] = (
            defaultdict(list)
        )
        for request in requests:
            key = predicate(request.batch)
            requests_by_predicate[key].append(request)
        return requests_by_predicate

    @staticmethod
    def split_response_by_type(
        response: ProfileResponse,
    ) -> Dict[Type[StatisticResult], ProfileResponse]:
        """
        Splits the response by the type of the statistic result.
        :param response:
        :return:
        """
        responses_by_type: Dict[Type[StatisticResult], ProfileResponse] = defaultdict(
            ProfileResponse
        )
        for fq_statistic_name, result in response.data.items():
            responses_by_type[type(result)].data[fq_statistic_name] = result

        assert set(responses_by_type.keys()) <= {
            SuccessStatisticResult,
            UnsuccessfulStatisticResult,
        }, f"So far 2 types; unexpected response type: {responses_by_type.keys()}"

        return responses_by_type

    @staticmethod
    def failed_response_for_request(
        request: ProfileRequest,
        unsuccessful_result_type: UnsuccessfulStatisticResultType,
        message: Optional[str] = None,
        exception: Optional[Exception] = None,
    ) -> ProfileResponse:
        response = ProfileResponse()
        for statistic in request.statistics:
            response.data[statistic.fq_name] = UnsuccessfulStatisticResult(
                type=unsuccessful_result_type,
                message=message,
                exception=exception,
            )
        return response
