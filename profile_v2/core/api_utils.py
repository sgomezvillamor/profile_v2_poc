import concurrent.futures
import logging
from copy import deepcopy
from typing import Callable, List, Optional

from profile_v2.core.api import ProfileEngine
from profile_v2.core.model import (DataSource, ProfileRequest, ProfileResponse,
                                   SuccessStatisticResult,
                                   UnsuccessfulStatisticResult)
from profile_v2.core.model_utils import ModelCollections

logger = logging.getLogger(__name__)


class SequentialFallbackProfileEngine(ProfileEngine):
    """
    Profile engine that will try to profile the data using the engines in order.

    Requests are processed by the first engine and only the failed/unsupported ones will be tried with the next one.
    And so on, until no more pending requests or no more engines.
    """

    def __init__(self, engines: List[ProfileEngine]):
        self.engines = engines

    def _do_profile(
        self, datasource: DataSource, requests: List[ProfileRequest]
    ) -> ProfileResponse:
        response = ProfileResponse()

        pending = deepcopy(requests)
        for engine in self.engines:
            engine_response = engine._do_profile(datasource, pending)

            engine_responses_by_type = ModelCollections.split_response_by_type(
                engine_response
            )
            success_response: Optional[ProfileResponse] = engine_responses_by_type.get(
                SuccessStatisticResult
            )
            unsuccessful_response: Optional[ProfileResponse] = (
                engine_responses_by_type.get(UnsuccessfulStatisticResult)
            )

            if success_response:
                logger.info(
                    f"{engine.__class__.__name__} successfully processed: {success_response}"
                )
                response.update(success_response)

            if unsuccessful_response:
                # set the failed results in the response
                # next engine will overwrite if so
                response.update(unsuccessful_response)

                # only keep in pending the requests that failed
                aux: List[ProfileRequest] = []
                for request in pending:
                    aux_stats = []
                    for statistic in request.statistics:
                        fq_name = statistic.fq_name
                        if fq_name not in unsuccessful_response.data:
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

    def _do_profile(
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
                executor.submit(self.engine._do_profile, datasource, batch): batch
                for batch in batch_requests
            }
            for batch_response_future in concurrent.futures.as_completed(
                batch_response_futures
            ):
                batch_response = batch_response_future.result()
                response.update(batch_response)

        return response
