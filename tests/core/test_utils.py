import unittest
from typing import List

from profile_v2.core.api import ProfileEngine
from profile_v2.core.model import (BatchSpec, DataSource,
                                   FailureStatisticResult,
                                   FailureStatisticResultType, ProfileRequest,
                                   ProfileResponse, StatisticSpec,
                                   SuccessStatisticResult)
from profile_v2.core.utils import (ModelCollections,
                                   SequentialFallbackProfileEngine)


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
