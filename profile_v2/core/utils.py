import concurrent.futures
import logging
from collections import defaultdict
from copy import deepcopy
from typing import Callable, Dict, List, Optional, Type, TypeVar

from profile_v2.core.api import ProfileEngine
from profile_v2.core.model import (BatchSpec, DataSource,
                                   FailureStatisticResult, ProfileRequest,
                                   ProfileResponse, StatisticResult,
                                   StatisticSpec, SuccessStatisticResult)

logger = logging.getLogger(__name__)

PredicateResponse = TypeVar("PredicateResponse")


class ModelCollections:

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
        return responses_by_type


class SequentialFallbackProfileEngine(ProfileEngine):
    """
    Profile engine that will try to profile the data using the engines in order.

    Requests are processed by the first engine and only the failed/unsupported ones will be tried with the next one.
    And so on, until no more pending requests or no more engines.
    """

    def __init__(self, engines: List[ProfileEngine]):
        self.engines = engines

    def do_profile(
        self, datasource: DataSource, requests: List[ProfileRequest]
    ) -> ProfileResponse:
        response = ProfileResponse()

        pending = deepcopy(requests)
        for engine in self.engines:
            engine_response = engine.do_profile(datasource, pending)

            engine_responses_by_type = ModelCollections.split_response_by_type(
                engine_response
            )
            success_response: Optional[ProfileResponse] = engine_responses_by_type.get(
                SuccessStatisticResult
            )
            failed_response: Optional[ProfileResponse] = engine_responses_by_type.get(
                FailureStatisticResult
            )

            if success_response:
                logger.info(
                    f"{engine.__class__.__name__} successfully processed: {success_response}"
                )
                response.data.update(success_response.data)

            if failed_response:
                # set the failed results in the response
                # next engine will overwrite if so
                response.data.update(failed_response.data)

                # only keep in pending the requests that failed
                aux: List[ProfileRequest] = []
                for request in pending:
                    aux_stats = []
                    for statistic in request.statistics:
                        fq_name = statistic.fq_name
                        if fq_name not in failed_response.data:
                            aux_stats.append(statistic)
                    if aux_stats:
                        aux.append(
                            ProfileRequest(batch=request.batch, statistics=aux_stats)
                        )

                pending = aux
                logger.info(f"Pending requests: {pending}")
            else:
                break

        return response


class ParallelProfileEngine(ProfileEngine):
    """
    Executes requests in parallel with a given engine.

    The requests are grouped in batches using the given predicate.
    """

    def __init__(
        self,
        engine: ProfileEngine,
        max_workers: int = 4,
        batch_requests_predicate: Optional[
            Callable[[List[ProfileRequest]], List[List[ProfileRequest]]]
        ] = None,
    ):
        self.engine = engine
        self.max_workers = max_workers
        self.group_requests_predicate = batch_requests_predicate

    def do_profile(
        self, datasource: DataSource, requests: List[ProfileRequest]
    ) -> ProfileResponse:
        response = ProfileResponse()

        batch_requests = (
            self.group_requests_predicate(requests)
            if self.group_requests_predicate
            else [requests]
        )
        logger.info(f"Requests batched in {len(batch_requests)} batches")
        logger.debug(batch_requests)

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_workers
        ) as executor:
            batch_response_futures = {
                executor.submit(self.engine.do_profile, datasource, batch): batch
                for batch in batch_requests
            }
            for batch_response_future in concurrent.futures.as_completed(
                batch_response_futures
            ):
                batch_response = batch_response_future.result()
                response.data.update(batch_response.data)

        return response
