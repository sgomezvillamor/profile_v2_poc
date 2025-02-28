import unittest

import pytest

from profile_v2.core.api import ProfileEngineValueError
from profile_v2.core.model import (BatchSpec, DataSource, ProfileRequest,
                                   ProfileResponse, StatisticSpec,
                                   SuccessStatisticResult)
from tests.core.common import FixedResponseEngine


class TestApi(unittest.TestCase):

    def test_api_requests_validations_pass(self):
        response = ProfileResponse(
            data={
                "fq_name_1": SuccessStatisticResult(value=0),
                "fq_name_2": SuccessStatisticResult(value=0),
            }
        )
        profile_engine = FixedResponseEngine(response)
        requests = [
            ProfileRequest(
                statistics=[
                    StatisticSpec(name="stat_1", fq_name="fq_name_1"),
                    StatisticSpec(name="stat_2", fq_name="fq_name_2"),
                ],
                batch=BatchSpec(fq_dataset_name="dataset_1"),
            )
        ]
        datasource = DataSource(name="source", connection_string="connection_string")
        assert profile_engine.profile(datasource, requests) == response

        # adding a duplicated fq statistic name
        requests.append(
            ProfileRequest(
                statistics=[
                    StatisticSpec(name="stat_3", fq_name="fq_name_1"),
                ],
                batch=BatchSpec(fq_dataset_name="dataset_2"),
            )
        )
        with pytest.raises(
            ProfileEngineValueError,
            match="FQ statistic names must be unique across all requests",
        ):
            profile_engine.profile(datasource, requests)
