import logging
import os
import threading
import time
import urllib.parse
from datetime import datetime
from typing import List, Optional

from profile_v2.core.api import ProfileEngine
from profile_v2.core.model import (DataSource,
                                   ProfileNonFunctionalRequirements,
                                   ProfileRequest, ProfileResponse,
                                   SuccessStatisticResult)

logger = logging.getLogger(__name__)


SNOWFLAKE_USER = urllib.parse.quote(os.environ["SNOWFLAKE_USER"])
SNOWFLAKE_PASSWORD = urllib.parse.quote(os.environ["SNOWFLAKE_PASSWORD"])
SNOWFLAKE_ACCOUNT = "cfa31444"
SNOWFLAKE_DATABASE = "SMOKE_TEST_DB"
SNOWFLAKE_SCHEMA = "PUBLIC"
SNOWFLAKE_WAREHOUSE = "SMOKE_TEST"
SNOWFLAKE_ROLE = "datahub_role"
SNOWFLAKE_CONNECTION_STRING = f"snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}@{SNOWFLAKE_ACCOUNT}/{SNOWFLAKE_DATABASE}/{SNOWFLAKE_SCHEMA}?warehouse={SNOWFLAKE_WAREHOUSE}&role={SNOWFLAKE_ROLE}&application=datahub"

BIGQUERY_CREDENTIALS_PATH = "/Users/sergio/workspace/github/acryldata/connector-tests/smoke-test/credentials/smoke-test.json"
BIGQUERY_PROJECT = "acryl-staging"
BIGQUERY_DATASET_CUSTOMER_DEMO = "customer_demo"
BIGQUERY_DATASET_DEPLOY_TEST_1K = "deploy_test_1k"
BIGQUERY_CONNECTION_STRING = f"bigquery://{BIGQUERY_PROJECT}"


class FixedResponseEngine(ProfileEngine):
    def __init__(self, response: ProfileResponse):
        self.response = response

    def _do_profile(
        self,
        datasource: DataSource,
        requests: List[ProfileRequest],
        non_functional_requirements: ProfileNonFunctionalRequirements = ProfileNonFunctionalRequirements(),
    ) -> ProfileResponse:
        return self.response


class SuccessResponseEngine(ProfileEngine):
    def __init__(
        self, success_value: int = 0, elapsed_time_millis: Optional[int] = None
    ):
        self.success_value = success_value
        self.elapsed_time_millis = elapsed_time_millis

    def _do_profile(
        self,
        datasource: DataSource,
        requests: List[ProfileRequest],
        non_functional_requirements: ProfileNonFunctionalRequirements = ProfileNonFunctionalRequirements(),
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
