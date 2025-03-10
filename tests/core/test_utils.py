import asyncio
import logging
import time
import unittest
from typing import List

from pytest import approx

from profile_v2.core.api_utils import (AsyncProfileEngine, ModelCollections,
                                       ParallelProfileEngine,
                                       SequentialFallbackProfileEngine)
from profile_v2.core.model import (BatchSpec, CustomStatistic, DataSource,
                                   DataSourceType,
                                   ProfileNonFunctionalRequirements,
                                   ProfileRequest, ProfileResponse,
                                   StatisticSpec, SuccessStatisticResult,
                                   UnsuccessfulStatisticResult,
                                   UnsuccessfulStatisticResultType)
from tests.core.common import FixedResponseEngine, SuccessResponseEngine

logger = logging.getLogger(__name__)


class TestModelCollections(unittest.TestCase):

    def test_validate_fq_statistic_name_uniqueness(self):
        # unique fq statistic names
        requests = [
            ProfileRequest(
                statistics=[
                    StatisticSpec(fq_name="fq_name_stat1"),
                    StatisticSpec(fq_name="fq_name_stat2"),
                ],
                batch=BatchSpec(fq_dataset_name="batch1"),
            ),
            ProfileRequest(
                statistics=[
                    StatisticSpec(fq_name="fq_name_stat3"),
                    StatisticSpec(fq_name="fq_name_stat4"),
                ],
                batch=BatchSpec(fq_dataset_name="batch2"),
            ),
        ]
        assert ModelCollections.validate_fq_statistic_name_uniqueness(requests)

        # adding a duplicate fq statistic name
        requests.append(
            ProfileRequest(
                statistics=[
                    StatisticSpec(fq_name="fq_name_stat1"),
                    StatisticSpec(fq_name="fq_name_stat6"),
                ],
                batch=BatchSpec(fq_dataset_name="batch3"),
            )
        )
        assert not ModelCollections.validate_fq_statistic_name_uniqueness(requests)

    def test_group_requests_by_statistics_and_grouped_results(self):
        requests = [
            ProfileRequest(
                statistics=[
                    StatisticSpec(fq_name="fq_name_stat1_A"),
                    StatisticSpec(fq_name="fq_name_stat2_A"),
                ],
                batch=BatchSpec(fq_dataset_name="batch1"),
            ),
            ProfileRequest(
                statistics=[
                    StatisticSpec(fq_name="fq_name_stat3_A"),
                    StatisticSpec(fq_name="fq_name_stat4_B"),
                ],
                batch=BatchSpec(fq_dataset_name="batch2"),
            ),
        ]
        predicate = lambda stat: stat.fq_name.endswith("_A")
        splits = ModelCollections.group_request_by_statistics_predicate(
            requests, predicate, group_results=True
        )
        print(splits)
        assert {
            True: [
                ProfileRequest(
                    statistics=[
                        StatisticSpec(fq_name="fq_name_stat1_A"),
                        StatisticSpec(fq_name="fq_name_stat2_A"),
                    ],
                    batch=BatchSpec(fq_dataset_name="batch1"),
                ),
                ProfileRequest(
                    statistics=[StatisticSpec(fq_name="fq_name_stat3_A")],
                    batch=BatchSpec(fq_dataset_name="batch2"),
                ),
            ],
            False: [
                ProfileRequest(
                    statistics=[StatisticSpec(fq_name="fq_name_stat4_B")],
                    batch=BatchSpec(fq_dataset_name="batch2"),
                ),
            ],
        }

    def test_group_requests_by_statistics_and_non_grouped_results(self):
        requests = [
            ProfileRequest(
                statistics=[
                    StatisticSpec(fq_name="fq_name_stat1_A"),
                    StatisticSpec(fq_name="fq_name_stat2_A"),
                ],
                batch=BatchSpec(fq_dataset_name="batch1"),
            ),
            ProfileRequest(
                statistics=[
                    StatisticSpec(fq_name="fq_name_stat3_A"),
                    StatisticSpec(fq_name="fq_name_stat4_B"),
                ],
                batch=BatchSpec(fq_dataset_name="batch2"),
            ),
        ]
        predicate = lambda stat: stat.fq_name == "stat_a"
        splits = ModelCollections.group_request_by_statistics_predicate(
            requests, predicate, group_results=False
        )
        print(splits)
        assert {
            True: [
                ProfileRequest(
                    statistics=[
                        StatisticSpec(fq_name="fq_name_stat1_A"),
                    ],
                    batch=BatchSpec(fq_dataset_name="batch1"),
                ),
                ProfileRequest(
                    statistics=[StatisticSpec(fq_name="fq_name_stat2_A")],
                    batch=BatchSpec(fq_dataset_name="batch1"),
                ),
                ProfileRequest(
                    statistics=[StatisticSpec(fq_name="fq_name_stat3_A")],
                    batch=BatchSpec(fq_dataset_name="batch2"),
                ),
            ],
            False: [
                ProfileRequest(
                    statistics=[StatisticSpec(fq_name="fq_name_stat4_B")],
                    batch=BatchSpec(fq_dataset_name="batch2"),
                ),
            ],
        }

    def test_join_statistics_by_batch_with_same_batch(self):
        requests = [
            ProfileRequest(
                statistics=[StatisticSpec(fq_name="fq_name_stat1")],
                batch=BatchSpec(fq_dataset_name="batch1"),
            ),
            ProfileRequest(
                statistics=[StatisticSpec(fq_name="fq_name_stat2")],
                batch=BatchSpec(fq_dataset_name="batch1"),
            ),
        ]
        grouped = ModelCollections.join_statistics_by_batch(requests)
        print(grouped)
        assert grouped == [
            ProfileRequest(
                statistics=[
                    StatisticSpec(fq_name="fq_name_stat1"),
                    StatisticSpec(fq_name="fq_name_stat2"),
                ],
                batch=BatchSpec(fq_dataset_name="batch1"),
            ),
        ]

    def test_join_statistics_by_batch_with_different_batch(self):
        requests = [
            ProfileRequest(
                statistics=[StatisticSpec(fq_name="fq_name_stat1")],
                batch=BatchSpec(fq_dataset_name="batch1"),
            ),
            ProfileRequest(
                statistics=[StatisticSpec(fq_name="fq_name_stat2")],
                batch=BatchSpec(fq_dataset_name="batch2"),
            ),
        ]
        grouped = ModelCollections.join_statistics_by_batch(requests)
        print(grouped)
        assert grouped == [
            ProfileRequest(
                statistics=[
                    StatisticSpec(fq_name="fq_name_stat1"),
                ],
                batch=BatchSpec(fq_dataset_name="batch1"),
            ),
            ProfileRequest(
                statistics=[StatisticSpec(fq_name="fq_name_stat2")],
                batch=BatchSpec(fq_dataset_name="batch2"),
            ),
        ]

    def test_group_requests_by_batch(self):
        requests = [
            ProfileRequest(
                statistics=[StatisticSpec(fq_name="fq_name_stat1")],
                batch=BatchSpec(fq_dataset_name="batch_A"),
            ),
            ProfileRequest(
                statistics=[StatisticSpec(fq_name="fq_name_stat2")],
                batch=BatchSpec(fq_dataset_name="batch_B"),
            ),
            ProfileRequest(
                statistics=[StatisticSpec(fq_name="fq_name_stat3")],
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
                    statistics=[StatisticSpec(fq_name="fq_name_stat1")],
                    batch=BatchSpec(fq_dataset_name="batch_A"),
                ),
                ProfileRequest(
                    statistics=[StatisticSpec(fq_name="fq_name_stat3")],
                    batch=BatchSpec(fq_dataset_name="batch_A"),
                ),
            ],
            "B": [
                ProfileRequest(
                    statistics=[StatisticSpec(fq_name="fq_name_stat2")],
                    batch=BatchSpec(fq_dataset_name="batch_B"),
                ),
            ],
        }

    def test_split_response_by_type(self):
        response = ProfileResponse(
            data={
                "fq_name_stat1": SuccessStatisticResult(value=1),
                "fq_name_stat2": SuccessStatisticResult(value=2),
                "fq_name_stat3": UnsuccessfulStatisticResult(
                    type=UnsuccessfulStatisticResultType.FAILURE
                ),
                "fq_name_stat4": SuccessStatisticResult(value=3),
                "fq_name_stat5": UnsuccessfulStatisticResult(
                    type=UnsuccessfulStatisticResultType.UNSUPPORTED
                ),
                "fq_name_stat6": UnsuccessfulStatisticResult(
                    type=UnsuccessfulStatisticResultType.UNSUPPORTED
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
            UnsuccessfulStatisticResult: ProfileResponse(
                data={
                    "fq_name_stat3": UnsuccessfulStatisticResult(
                        type=UnsuccessfulStatisticResultType.FAILURE
                    ),
                    "fq_name_stat5": UnsuccessfulStatisticResult(
                        type=UnsuccessfulStatisticResultType.UNSUPPORTED
                    ),
                    "fq_name_stat6": UnsuccessfulStatisticResult(
                        type=UnsuccessfulStatisticResultType.UNSUPPORTED
                    ),
                },
            ),
        }


class TestSequentialFallbackProfileEngine(unittest.TestCase):

    _datasource = DataSource(
        source=DataSourceType.SNOWFLAKE, connection_string="connection_string1"
    )

    def test_with_single_successful_engine(self):
        requests = [
            ProfileRequest(
                statistics=[StatisticSpec(fq_name="fq_stat1")],
                batch=BatchSpec(fq_dataset_name="batch1"),
            )
        ]
        success_response = ProfileResponse(
            data={"fq_stat1": SuccessStatisticResult(value=1)}
        )
        engine = FixedResponseEngine(success_response)
        fallback_engine = SequentialFallbackProfileEngine([engine])

        response = fallback_engine.profile(self._datasource, requests)
        print(response)

        assert response == ProfileResponse(
            data={"fq_stat1": SuccessStatisticResult(value=1)}
        )

    def test_profile_with_fallback_to_next_engine(self):
        requests = [
            ProfileRequest(
                statistics=[
                    StatisticSpec(fq_name="fq_stat_1a"),
                    StatisticSpec(fq_name="fq_stat_1b"),
                    StatisticSpec(fq_name="fq_stat_1c"),
                ],
                batch=BatchSpec(fq_dataset_name="batch1"),
            ),
            ProfileRequest(
                statistics=[
                    StatisticSpec(fq_name="fq_stat_2a"),
                    StatisticSpec(fq_name="fq_stat_2b"),
                    StatisticSpec(fq_name="fq_stat_2c"),
                ],
                batch=BatchSpec(fq_dataset_name="batch2"),
            ),
            ProfileRequest(
                statistics=[
                    StatisticSpec(fq_name="fq_stat_3a"),
                    StatisticSpec(fq_name="fq_stat_3b"),
                    StatisticSpec(fq_name="fq_stat_3c"),
                ],
                batch=BatchSpec(fq_dataset_name="batch3"),
            ),
        ]
        engine1 = FixedResponseEngine(
            ProfileResponse(
                data={
                    "fq_stat_1a": SuccessStatisticResult(value=1),
                    "fq_stat_1b": SuccessStatisticResult(value=1),
                    "fq_stat_1c": SuccessStatisticResult(value=1),
                    "fq_stat_2a": SuccessStatisticResult(value=1),
                    "fq_stat_2b": UnsuccessfulStatisticResult(
                        type=UnsuccessfulStatisticResultType.UNSUPPORTED
                    ),
                    "fq_stat_2c": UnsuccessfulStatisticResult(
                        type=UnsuccessfulStatisticResultType.UNSUPPORTED
                    ),
                    "fq_stat_3a": SuccessStatisticResult(value=1),
                    "fq_stat_3b": UnsuccessfulStatisticResult(
                        type=UnsuccessfulStatisticResultType.FAILURE
                    ),
                    "fq_stat_3c": UnsuccessfulStatisticResult(
                        type=UnsuccessfulStatisticResultType.FAILURE
                    ),
                }
            )
        )
        engine2 = FixedResponseEngine(
            ProfileResponse(
                data={
                    "fq_stat_2b": SuccessStatisticResult(value=2),
                    "fq_stat_2c": UnsuccessfulStatisticResult(
                        type=UnsuccessfulStatisticResultType.FAILURE
                    ),
                    "fq_stat_3b": UnsuccessfulStatisticResult(
                        type=UnsuccessfulStatisticResultType.FAILURE
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

        response = fallback_engine.profile(self._datasource, requests)
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
                    StatisticSpec(fq_name="fq_stat_1a"),
                    StatisticSpec(fq_name="fq_stat_1b"),
                    StatisticSpec(fq_name="fq_stat_1c"),
                ],
                batch=BatchSpec(fq_dataset_name="batch1"),
            ),
            ProfileRequest(
                statistics=[
                    StatisticSpec(fq_name="fq_stat_2a"),
                    StatisticSpec(fq_name="fq_stat_2b"),
                    StatisticSpec(fq_name="fq_stat_2c"),
                ],
                batch=BatchSpec(fq_dataset_name="batch2"),
            ),
        ]
        engine1 = FixedResponseEngine(
            ProfileResponse(
                data={
                    "fq_stat_1a": SuccessStatisticResult(value=1),
                    "fq_stat_1b": SuccessStatisticResult(value=1),
                    "fq_stat_1c": SuccessStatisticResult(value=1),
                    "fq_stat_2a": SuccessStatisticResult(value=1),
                    "fq_stat_2b": UnsuccessfulStatisticResult(
                        type=UnsuccessfulStatisticResultType.UNSUPPORTED
                    ),
                    "fq_stat_2c": UnsuccessfulStatisticResult(
                        type=UnsuccessfulStatisticResultType.UNSUPPORTED
                    ),
                }
            )
        )
        engine2 = FixedResponseEngine(
            ProfileResponse(
                data={
                    "fq_stat_2b": UnsuccessfulStatisticResult(
                        type=UnsuccessfulStatisticResultType.UNSUPPORTED
                    ),
                    "fq_stat_2c": UnsuccessfulStatisticResult(
                        type=UnsuccessfulStatisticResultType.FAILURE
                    ),
                }
            )
        )
        fallback_engine = SequentialFallbackProfileEngine([engine1, engine2])

        response = fallback_engine.profile(self._datasource, requests)
        print(response)
        assert response == ProfileResponse(
            data={
                "fq_stat_1a": SuccessStatisticResult(value=1),
                "fq_stat_1b": SuccessStatisticResult(value=1),
                "fq_stat_1c": SuccessStatisticResult(value=1),
                "fq_stat_2a": SuccessStatisticResult(value=1),
                "fq_stat_2b": UnsuccessfulStatisticResult(
                    type=UnsuccessfulStatisticResultType.UNSUPPORTED
                ),
                "fq_stat_2c": UnsuccessfulStatisticResult(
                    type=UnsuccessfulStatisticResultType.FAILURE
                ),
            }
        )


class TestParallelProfileEngine(unittest.TestCase):
    _requests = [
        ProfileRequest(
            statistics=[
                CustomStatistic(fq_name="fq_stat1_1", sql="1"),
                CustomStatistic(fq_name="fq_stat2_1", sql="1"),
            ],
            batch=BatchSpec(fq_dataset_name="batch1"),
        ),
        ProfileRequest(
            statistics=[
                CustomStatistic(fq_name="fq_stat1_2", sql="1"),
                CustomStatistic(fq_name="fq_stat2_2", sql="1"),
            ],
            batch=BatchSpec(fq_dataset_name="batch2"),
        ),
        ProfileRequest(
            statistics=[
                CustomStatistic(fq_name="fq_stat1_3", sql="1"),
                CustomStatistic(fq_name="fq_stat2_3", sql="1"),
            ],
            batch=BatchSpec(fq_dataset_name="batch3"),
        ),
    ]
    _datasource = DataSource(
        source=DataSourceType.SNOWFLAKE, connection_string="connection_string1"
    )

    _expected_response = ProfileResponse(
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

    def test_profile_all_in_parallel(self):
        parallel_engine = ParallelProfileEngine(
            engine=SuccessResponseEngine(success_value=1, elapsed_time_millis=1000),
            max_workers=3,
            batch_requests_predicate=self._batch_requests_individually,
        )

        start_time = time.time()
        response = parallel_engine.profile(self._datasource, self._requests)
        end_time = time.time()
        elapsed_time = end_time - start_time

        assert response == self._expected_response
        # all requests in parallel, so elapsed time should be around 1 second = time of the slowest request
        assert elapsed_time == approx(1, abs=0.1)

    def test_profile_each_statistic_individually(self):
        parallel_engine = ParallelProfileEngine(
            engine=SuccessResponseEngine(success_value=1, elapsed_time_millis=1000),
            max_workers=2,
            batch_requests_predicate=self._batch_statistics_individually,
        )

        start_time = time.time()
        response = parallel_engine.profile(self._datasource, self._requests)
        end_time = time.time()
        elapsed_time = end_time - start_time

        assert response == self._expected_response
        # all 6 statistics in individual batches, so elapsed time should be statistics=6/workers=2 = 3 seconds
        assert elapsed_time == approx(3, abs=0.1)


class TestAsyncProfileEngine(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()

        self.requests = [
            ProfileRequest(
                statistics=[
                    CustomStatistic(fq_name="fq_stat1_1", sql="1"),
                    CustomStatistic(fq_name="fq_stat1_2", sql="2"),
                ],
                batch=BatchSpec(fq_dataset_name="batch1"),
            )
        ]
        self.response = ProfileResponse(
            data={
                "fq_stat1_1": SuccessStatisticResult(value=1),
                "fq_stat1_2": SuccessStatisticResult(value=2),
            }
        )
        self.engine = FixedResponseEngine(self.response)
        self.async_engine = AsyncProfileEngine(self.engine, self.loop)
        self.datasource = DataSource(
            source=DataSourceType.SNOWFLAKE, connection_string="connection_string1"
        )
        self.non_functional_requirements = ProfileNonFunctionalRequirements()

    def tearDown(self):
        self.loop.close()

    def test_profile(self):
        async def test_coroutine():
            future = self.async_engine.profile(
                self.datasource, self.requests, self.non_functional_requirements
            )
            assert isinstance(future, asyncio.Future)
            print(future)
            await future  # Wait until the future is completed
            assert future.done()
            assert future.result() == self.response

        self.loop.run_until_complete(test_coroutine())
