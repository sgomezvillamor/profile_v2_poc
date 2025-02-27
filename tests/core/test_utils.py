import logging
import threading
import time
import unittest
from datetime import datetime
from typing import List, Optional

from pytest import approx

from profile_v2.core.api import ProfileEngine
from profile_v2.core.model import (BatchSpec, DataSource,
                                   FailureStatisticResult,
                                   FailureStatisticResultType, ProfileRequest,
                                   ProfileResponse, StatisticSpec,
                                   SuccessStatisticResult)
from profile_v2.core.utils import (ModelCollections, ParallelProfileEngine,
                                   SequentialFallbackProfileEngine)

logger = logging.getLogger(__name__)


class TestModelCollections(unittest.TestCase):

    def test_group_requests_by_statistics_and_grouped_results(self):
        requests = [
            ProfileRequest(
                statistics=[
                    StatisticSpec(name="stat_a", fq_name="fq_name_stat1"),
                    StatisticSpec(name="stat_a", fq_name="fq_name_stat2"),
                ],
                batch=BatchSpec(fq_dataset_name="batch1"),
            ),
            ProfileRequest(
                statistics=[
                    StatisticSpec(name="stat_a", fq_name="fq_name_stat3"),
                    StatisticSpec(name="stat_b", fq_name="fq_name_stat4"),
                ],
                batch=BatchSpec(fq_dataset_name="batch2"),
            ),
        ]
        predicate = lambda stat: stat.name == "stat_a"
        splits = ModelCollections.group_request_by_statistics_predicate(
            requests, predicate, group_results=True
        )
        print(splits)
        assert {
            True: [
                ProfileRequest(
                    statistics=[
                        StatisticSpec(name="stat_a", fq_name="fq_name_stat1"),
                        StatisticSpec(name="stat_a", fq_name="fq_name_stat2"),
                    ],
                    batch=BatchSpec(fq_dataset_name="batch1"),
                ),
                ProfileRequest(
                    statistics=[StatisticSpec(name="stat_a", fq_name="fq_name_stat3")],
                    batch=BatchSpec(fq_dataset_name="batch2"),
                ),
            ],
            False: [
                ProfileRequest(
                    statistics=[StatisticSpec(name="stat_b", fq_name="fq_name_stat4")],
                    batch=BatchSpec(fq_dataset_name="batch2"),
                ),
            ],
        }

    def test_group_requests_by_statistics_and_non_grouped_results(self):
        requests = [
            ProfileRequest(
                statistics=[
                    StatisticSpec(name="stat_a", fq_name="fq_name_stat1"),
                    StatisticSpec(name="stat_a", fq_name="fq_name_stat2"),
                ],
                batch=BatchSpec(fq_dataset_name="batch1"),
            ),
            ProfileRequest(
                statistics=[
                    StatisticSpec(name="stat_a", fq_name="fq_name_stat3"),
                    StatisticSpec(name="stat_b", fq_name="fq_name_stat4"),
                ],
                batch=BatchSpec(fq_dataset_name="batch2"),
            ),
        ]
        predicate = lambda stat: stat.name == "stat_a"
        splits = ModelCollections.group_request_by_statistics_predicate(
            requests, predicate, group_results=False
        )
        print(splits)
        assert {
            True: [
                ProfileRequest(
                    statistics=[
                        StatisticSpec(name="stat_a", fq_name="fq_name_stat1"),
                    ],
                    batch=BatchSpec(fq_dataset_name="batch1"),
                ),
                ProfileRequest(
                    statistics=[StatisticSpec(name="stat_a", fq_name="fq_name_stat2")],
                    batch=BatchSpec(fq_dataset_name="batch1"),
                ),
                ProfileRequest(
                    statistics=[StatisticSpec(name="stat_a", fq_name="fq_name_stat3")],
                    batch=BatchSpec(fq_dataset_name="batch2"),
                ),
            ],
            False: [
                ProfileRequest(
                    statistics=[StatisticSpec(name="stat_b", fq_name="fq_name_stat4")],
                    batch=BatchSpec(fq_dataset_name="batch2"),
                ),
            ],
        }

    def test_join_statistics_by_batch_with_same_batch(self):
        requests = [
            ProfileRequest(
                statistics=[StatisticSpec(name="stat1", fq_name="fq_name_stat1")],
                batch=BatchSpec(fq_dataset_name="batch1"),
            ),
            ProfileRequest(
                statistics=[StatisticSpec(name="stat2", fq_name="fq_name_stat2")],
                batch=BatchSpec(fq_dataset_name="batch1"),
            ),
        ]
        grouped = ModelCollections.join_statistics_by_batch(requests)
        print(grouped)
        assert grouped == [
            ProfileRequest(
                statistics=[
                    StatisticSpec(name="stat1", fq_name="fq_name_stat1"),
                    StatisticSpec(name="stat2", fq_name="fq_name_stat2"),
                ],
                batch=BatchSpec(fq_dataset_name="batch1"),
            ),
        ]

    def test_join_statistics_by_batch_with_different_batch(self):
        requests = [
            ProfileRequest(
                statistics=[StatisticSpec(name="stat1", fq_name="fq_name_stat1")],
                batch=BatchSpec(fq_dataset_name="batch1"),
            ),
            ProfileRequest(
                statistics=[StatisticSpec(name="stat2", fq_name="fq_name_stat2")],
                batch=BatchSpec(fq_dataset_name="batch2"),
            ),
        ]
        grouped = ModelCollections.join_statistics_by_batch(requests)
        print(grouped)
        assert grouped == [
            ProfileRequest(
                statistics=[
                    StatisticSpec(name="stat1", fq_name="fq_name_stat1"),
                ],
                batch=BatchSpec(fq_dataset_name="batch1"),
            ),
            ProfileRequest(
                statistics=[StatisticSpec(name="stat2", fq_name="fq_name_stat2")],
                batch=BatchSpec(fq_dataset_name="batch2"),
            ),
        ]

    def test_group_requests_by_batch(self):
        requests = [
            ProfileRequest(
                statistics=[StatisticSpec(name="stat1", fq_name="fq_name_stat1")],
                batch=BatchSpec(fq_dataset_name="batch_A"),
            ),
            ProfileRequest(
                statistics=[StatisticSpec(name="stat2", fq_name="fq_name_stat2")],
                batch=BatchSpec(fq_dataset_name="batch_B"),
            ),
            ProfileRequest(
                statistics=[StatisticSpec(name="stat3", fq_name="fq_name_stat3")],
                batch=BatchSpec(fq_dataset_name="batch_A"),
            ),
        ]
        predicate = lambda batch: batch.fq_dataset_name[-1]
        grouped = ModelCollections.group_requests_by_batch_predicate(
            requests, predicate
        )
        print(grouped)
        assert grouped == {
            "A": [
                ProfileRequest(
                    statistics=[StatisticSpec(name="stat1", fq_name="fq_name_stat1")],
                    batch=BatchSpec(fq_dataset_name="batch_A"),
                ),
                ProfileRequest(
                    statistics=[StatisticSpec(name="stat3", fq_name="fq_name_stat3")],
                    batch=BatchSpec(fq_dataset_name="batch_A"),
                ),
            ],
            "B": [
                ProfileRequest(
                    statistics=[StatisticSpec(name="stat2", fq_name="fq_name_stat2")],
                    batch=BatchSpec(fq_dataset_name="batch_B"),
                ),
            ],
        }

    def test_split_response_by_type(self):
        response = ProfileResponse(
            data={
                "fq_name_stat1": SuccessStatisticResult(value=1),
                "fq_name_stat2": SuccessStatisticResult(value=2),
                "fq_name_stat3": FailureStatisticResult(
                    type=FailureStatisticResultType.FAILURE
                ),
                "fq_name_stat4": SuccessStatisticResult(value=3),
                "fq_name_stat5": FailureStatisticResult(
                    type=FailureStatisticResultType.UNSUPPORTED
                ),
                "fq_name_stat6": FailureStatisticResult(
                    type=FailureStatisticResultType.UNSUPPORTED
                ),
            }
        )
        splits = ModelCollections.split_response_by_type(response)
        print(splits)
        assert splits == {
            SuccessStatisticResult: ProfileResponse(
                data={
                    "fq_name_stat1": SuccessStatisticResult(value=1),
                    "fq_name_stat2": SuccessStatisticResult(value=2),
                    "fq_name_stat4": SuccessStatisticResult(value=3),
                },
            ),
            FailureStatisticResult: ProfileResponse(
                data={
                    "fq_name_stat3": FailureStatisticResult(
                        type=FailureStatisticResultType.FAILURE
                    ),
                    "fq_name_stat5": FailureStatisticResult(
                        type=FailureStatisticResultType.UNSUPPORTED
                    ),
                    "fq_name_stat6": FailureStatisticResult(
                        type=FailureStatisticResultType.UNSUPPORTED
                    ),
                },
            ),
        }


class FixedResponseEngine(ProfileEngine):
    def __init__(self, response: ProfileResponse):
        self.response = response

    def do_profile(
        self, datasource: DataSource, requests: List[ProfileRequest]
    ) -> ProfileResponse:
        return self.response


class TestSequentialFallbackProfileEngine(unittest.TestCase):

    def test_with_single_successful_engine(self):
        requests = [
            ProfileRequest(
                statistics=[StatisticSpec(name="stat1", fq_name="fq_stat1")],
                batch=BatchSpec(fq_dataset_name="batch1"),
            )
        ]
        datasource = DataSource(
            name="datasource1", connection_string="connection_string1"
        )
        success_response = ProfileResponse(
            data={"fq_stat1": SuccessStatisticResult(value=1)}
        )
        engine = FixedResponseEngine(success_response)
        fallback_engine = SequentialFallbackProfileEngine([engine])

        response = fallback_engine.do_profile(datasource, requests)
        print(response)

        assert response == ProfileResponse(
            data={"fq_stat1": SuccessStatisticResult(value=1)}
        )

    def test_profile_with_fallback_to_next_engine(self):
        requests = [
            ProfileRequest(
                statistics=[
                    StatisticSpec(name="stat_a", fq_name="fq_stat_1a"),
                    StatisticSpec(name="stat_b", fq_name="fq_stat_1b"),
                    StatisticSpec(name="stat_c", fq_name="fq_stat_1c"),
                ],
                batch=BatchSpec(fq_dataset_name="batch1"),
            ),
            ProfileRequest(
                statistics=[
                    StatisticSpec(name="stat_a", fq_name="fq_stat_2a"),
                    StatisticSpec(name="stat_b", fq_name="fq_stat_2b"),
                    StatisticSpec(name="stat_c", fq_name="fq_stat_2c"),
                ],
                batch=BatchSpec(fq_dataset_name="batch2"),
            ),
            ProfileRequest(
                statistics=[
                    StatisticSpec(name="stat_a", fq_name="fq_stat_3a"),
                    StatisticSpec(name="stat_b", fq_name="fq_stat_3b"),
                    StatisticSpec(name="stat_c", fq_name="fq_stat_3c"),
                ],
                batch=BatchSpec(fq_dataset_name="batch3"),
            ),
        ]
        datasource = DataSource(
            name="datasource1", connection_string="connection_string1"
        )
        engine1 = FixedResponseEngine(
            ProfileResponse(
                data={
                    "fq_stat_1a": SuccessStatisticResult(value=1),
                    "fq_stat_1b": SuccessStatisticResult(value=1),
                    "fq_stat_1c": SuccessStatisticResult(value=1),
                    "fq_stat_2a": SuccessStatisticResult(value=1),
                    "fq_stat_2b": FailureStatisticResult(
                        type=FailureStatisticResultType.UNSUPPORTED
                    ),
                    "fq_stat_2c": FailureStatisticResult(
                        type=FailureStatisticResultType.UNSUPPORTED
                    ),
                    "fq_stat_3a": SuccessStatisticResult(value=1),
                    "fq_stat_3b": FailureStatisticResult(
                        type=FailureStatisticResultType.FAILURE
                    ),
                    "fq_stat_3c": FailureStatisticResult(
                        type=FailureStatisticResultType.FAILURE
                    ),
                }
            )
        )
        engine2 = FixedResponseEngine(
            ProfileResponse(
                data={
                    "fq_stat_2b": SuccessStatisticResult(value=2),
                    "fq_stat_2c": FailureStatisticResult(
                        type=FailureStatisticResultType.FAILURE
                    ),
                    "fq_stat_3b": FailureStatisticResult(
                        type=FailureStatisticResultType.FAILURE
                    ),
                    "fq_stat_3c": SuccessStatisticResult(value=3),
                }
            )
        )
        engine3 = FixedResponseEngine(
            ProfileResponse(
                data={
                    "fq_stat_2c": SuccessStatisticResult(value=3),
                    "fq_stat_3b": SuccessStatisticResult(value=3),
                }
            )
        )
        fallback_engine = SequentialFallbackProfileEngine([engine1, engine2, engine3])

        response = fallback_engine.do_profile(datasource, requests)
        print(response)
        assert response == ProfileResponse(
            data={
                "fq_stat_1a": SuccessStatisticResult(value=1),
                "fq_stat_1b": SuccessStatisticResult(value=1),
                "fq_stat_1c": SuccessStatisticResult(value=1),
                "fq_stat_2a": SuccessStatisticResult(value=1),
                "fq_stat_3a": SuccessStatisticResult(value=1),
                "fq_stat_2b": SuccessStatisticResult(value=2),
                "fq_stat_3c": SuccessStatisticResult(value=3),
                "fq_stat_2c": SuccessStatisticResult(value=3),
                "fq_stat_3b": SuccessStatisticResult(value=3),
            }
        )

    def test_profile_with_some_remaining_requests(self):
        requests = [
            ProfileRequest(
                statistics=[
                    StatisticSpec(name="stat_a", fq_name="fq_stat_1a"),
                    StatisticSpec(name="stat_b", fq_name="fq_stat_1b"),
                    StatisticSpec(name="stat_c", fq_name="fq_stat_1c"),
                ],
                batch=BatchSpec(fq_dataset_name="batch1"),
            ),
            ProfileRequest(
                statistics=[
                    StatisticSpec(name="stat_a", fq_name="fq_stat_2a"),
                    StatisticSpec(name="stat_b", fq_name="fq_stat_2b"),
                    StatisticSpec(name="stat_c", fq_name="fq_stat_2c"),
                ],
                batch=BatchSpec(fq_dataset_name="batch2"),
            ),
        ]
        datasource = DataSource(
            name="datasource1", connection_string="connection_string1"
        )
        engine1 = FixedResponseEngine(
            ProfileResponse(
                data={
                    "fq_stat_1a": SuccessStatisticResult(value=1),
                    "fq_stat_1b": SuccessStatisticResult(value=1),
                    "fq_stat_1c": SuccessStatisticResult(value=1),
                    "fq_stat_2a": SuccessStatisticResult(value=1),
                    "fq_stat_2b": FailureStatisticResult(
                        type=FailureStatisticResultType.UNSUPPORTED
                    ),
                    "fq_stat_2c": FailureStatisticResult(
                        type=FailureStatisticResultType.UNSUPPORTED
                    ),
                }
            )
        )
        engine2 = FixedResponseEngine(
            ProfileResponse(
                data={
                    "fq_stat_2b": FailureStatisticResult(
                        type=FailureStatisticResultType.UNSUPPORTED
                    ),
                    "fq_stat_2c": FailureStatisticResult(
                        type=FailureStatisticResultType.FAILURE
                    ),
                }
            )
        )
        fallback_engine = SequentialFallbackProfileEngine([engine1, engine2])

        response = fallback_engine.do_profile(datasource, requests)
        print(response)
        assert response == ProfileResponse(
            data={
                "fq_stat_1a": SuccessStatisticResult(value=1),
                "fq_stat_1b": SuccessStatisticResult(value=1),
                "fq_stat_1c": SuccessStatisticResult(value=1),
                "fq_stat_2a": SuccessStatisticResult(value=1),
                "fq_stat_2b": FailureStatisticResult(
                    type=FailureStatisticResultType.UNSUPPORTED
                ),
                "fq_stat_2c": FailureStatisticResult(
                    type=FailureStatisticResultType.FAILURE
                ),
            }
        )


class SuccessResponseEngine(ProfileEngine):
    def __init__(
        self, success_value: int = 0, elapsed_time_millis: Optional[int] = None
    ):
        self.success_value = success_value
        self.elapsed_time_millis = elapsed_time_millis

    def do_profile(
        self, datasource: DataSource, requests: List[ProfileRequest]
    ) -> ProfileResponse:
        response = ProfileResponse()
        if self.elapsed_time_millis:
            logger.info(
                f"[{threading.get_ident()} {datetime.now()}] Sleeping for {self.elapsed_time_millis / 1000} seconds..."
            )
            time.sleep(self.elapsed_time_millis / 1000)

        for request in requests:
            for statistic in request.statistics:
                response.data[statistic.fq_name] = SuccessStatisticResult(
                    value=self.success_value
                )
        logger.info(f"[{threading.get_ident()} {datetime.now()}] Done!")
        return response


class TestParallelProfileEngine(unittest.TestCase):
    requests = [
        ProfileRequest(
            statistics=[
                StatisticSpec(name="stat1", fq_name="fq_stat1_1"),
                StatisticSpec(name="stat2", fq_name="fq_stat2_1"),
            ],
            batch=BatchSpec(fq_dataset_name="batch1"),
        ),
        ProfileRequest(
            statistics=[
                StatisticSpec(name="stat1", fq_name="fq_stat1_2"),
                StatisticSpec(name="stat2", fq_name="fq_stat2_2"),
            ],
            batch=BatchSpec(fq_dataset_name="batch2"),
        ),
        ProfileRequest(
            statistics=[
                StatisticSpec(name="stat1", fq_name="fq_stat1_3"),
                StatisticSpec(name="stat2", fq_name="fq_stat2_3"),
            ],
            batch=BatchSpec(fq_dataset_name="batch3"),
        ),
    ]
    datasource = DataSource(name="datasource1", connection_string="connection_string1")

    expected_response = ProfileResponse(
        data={
            "fq_stat1_1": SuccessStatisticResult(value=1),
            "fq_stat2_1": SuccessStatisticResult(value=1),
            "fq_stat1_2": SuccessStatisticResult(value=1),
            "fq_stat2_2": SuccessStatisticResult(value=1),
            "fq_stat1_3": SuccessStatisticResult(value=1),
            "fq_stat2_3": SuccessStatisticResult(value=1),
        }
    )

    def _batch_requests_individually(
        self, requests: List[ProfileRequest]
    ) -> List[List[ProfileRequest]]:
        return [[request] for request in requests]

    def _batch_statistics_individually(
        self, requests: List[ProfileRequest]
    ) -> List[List[ProfileRequest]]:
        result: List[List[ProfileRequest]] = []
        for request in requests:
            for statistic in request.statistics:
                result.append(
                    [ProfileRequest(statistics=[statistic], batch=request.batch)]
                )
        return result

    def test_do_profile_all_in_parallel(self):
        parallel_engine = ParallelProfileEngine(
            engine=SuccessResponseEngine(success_value=1, elapsed_time_millis=1000),
            max_workers=3,
            batch_requests_predicate=self._batch_requests_individually,
        )

        start_time = time.time()
        response = parallel_engine.do_profile(self.datasource, self.requests)
        end_time = time.time()
        elapsed_time = end_time - start_time

        assert response == self.expected_response
        # all requests in parallel, so elapsed time should be around 1 second = time of the slowest request
        assert elapsed_time == approx(1, abs=0.1)

    def test_do_profile_each_statistic_individually(self):
        parallel_engine = ParallelProfileEngine(
            engine=SuccessResponseEngine(success_value=1, elapsed_time_millis=1000),
            max_workers=2,
            batch_requests_predicate=self._batch_statistics_individually,
        )

        start_time = time.time()
        response = parallel_engine.do_profile(self.datasource, self.requests)
        end_time = time.time()
        elapsed_time = end_time - start_time

        assert response == self.expected_response
        # all 6 statistics in individual batches, so elapsed time should be statistics=6/workers=2 = 3 seconds
        assert elapsed_time == approx(3, abs=0.1)
