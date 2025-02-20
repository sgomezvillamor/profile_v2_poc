import unittest
import urllib.parse
import os

from profile_v2.core.model import (
    BatchSpec,
    DataSource,
    ProfileRequest,
    ProfileStatisticType,
    TypedStatistic,
)
from profile_v2.core.gx.gx import do_profile_gx


SNOWFLAKE_USER=urllib.parse.quote(os.environ["SNOWFLAKE_USER"])
SNOWFLAKE_PASSWORD=urllib.parse.quote(os.environ["SNOWFLAKE_PASSWORD"])
SNOWFLAKE_ACCOUNT="cfa31444"
SNOWFLAKE_DATABASE="SMOKE_TEST_DB"
SNOWFLAKE_SCHEMA="PUBLIC"
SNOWFLAKE_WAREHOUSE="SMOKE_TEST"
SNOWFLAKE_ROLE="datahub_role"
SNOWFLAKE_CONNECTION_STRING=f"snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}@{SNOWFLAKE_ACCOUNT}/{SNOWFLAKE_DATABASE}/{SNOWFLAKE_SCHEMA}?warehouse={SNOWFLAKE_WAREHOUSE}&role={SNOWFLAKE_ROLE}&application=datahub"


class GxTest(unittest.TestCase):

    def test_hello_world(self):
        result = do_profile_gx(
            datasource=DataSource(
                name="snowlake",
                connection_string=SNOWFLAKE_CONNECTION_STRING,
            ),
            request=ProfileRequest(
                statistics=[
                    TypedStatistic(
                        name=ProfileStatisticType.DISTINCT_COUNT.value,
                        fq_name="SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.ID.distinct_count",
                        columns=["ID"],
                        statistic=ProfileStatisticType.DISTINCT_COUNT,
                    ),
                    TypedStatistic(
                        name=ProfileStatisticType.DISTINCT_COUNT.value,
                        fq_name="SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.LABEL.distinct_count",
                        columns=["LABEL"],
                        statistic=ProfileStatisticType.DISTINCT_COUNT,
                    ),
                ],
                batch=BatchSpec(
                    fully_qualified_dataset_name=f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.COVID19_EXTERNAL_TABLE"
                )
            ),
        )
        print(result)


if __name__ == '__main__':
    unittest.main()
