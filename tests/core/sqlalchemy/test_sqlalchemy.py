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
from profile_v2.core.sqla.sqlalchemy import do_profile_sqlalchemy


SNOWFLAKE_USER=urllib.parse.quote(os.environ["SNOWFLAKE_USER"])
SNOWFLAKE_PASSWORD=urllib.parse.quote(os.environ["SNOWFLAKE_PASSWORD"])
SNOWFLAKE_ACCOUNT="cfa31444"
SNOWFLAKE_DATABASE="SMOKE_TEST_DB"
SNOWFLAKE_SCHEMA="PUBLIC"
SNOWFLAKE_WAREHOUSE="SMOKE_TEST"
SNOWFLAKE_ROLE="datahub_role"
SNOWFLAKE_CONNECTION_STRING=f"snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}@{SNOWFLAKE_ACCOUNT}/{SNOWFLAKE_DATABASE}/{SNOWFLAKE_SCHEMA}?warehouse={SNOWFLAKE_WAREHOUSE}&role={SNOWFLAKE_ROLE}&application=datahub"


class SqlAlchemyTest(unittest.TestCase):

    def test_hello_world(self):
        result = do_profile_sqlalchemy(
            datasource=DataSource(
                name="snowlake",
                connection_string=SNOWFLAKE_CONNECTION_STRING,
            ),
            request=ProfileRequest(
                statistics=[
                    TypedStatistic(
                        name="distinct_count",
                        columns=["name"],
                        statistic=ProfileStatisticType.DISTINCT_COUNT,
                    )
                ],
                batch=BatchSpec(
                    fully_qualified_dataset_name="smoke_test_db.public.person_distinct_names"
                )
            ),
        )
        print(result)


if __name__ == '__main__':
    unittest.main()
