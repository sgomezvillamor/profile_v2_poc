import unittest
import urllib.parse
import os

from profile_v2.core.model import (
    BatchSpec,
    CustomStatistic,
    DataSource,
    ProfileRequest,
    ProfileResponse,
    ProfileStatisticType,
    TypedStatistic,
)
from profile_v2.core.sqlalchemy.sqlalchemy import do_profile_sqlalchemy


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
                    TypedStatistic(
                        name=ProfileStatisticType.DISTINCT_COUNT.value,
                        fq_name="SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.ID+LABEL.distinct_count",
                        columns=["ID", "LABEL"],
                        statistic=ProfileStatisticType.DISTINCT_COUNT,
                    ),
                    CustomStatistic(
                        name="custom_average_str_length",
                        fq_name="SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.LABEL.custom_average_str_length",
                        sql="CEIL(AVG(LEN(LABEL)))",
                    ),
                ],
                batch=BatchSpec(
                    fully_qualified_dataset_name=f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.COVID19_EXTERNAL_TABLE"
                )
            ),
        )
        print(result)
        assert result == ProfileResponse(
            data={
                'SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.ID.DISTINCT_COUNT': 398880,
                'SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.LABEL.DISTINCT_COUNT': 5,
                'SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.ID+LABEL.DISTINCT_COUNT': 398880,
                'SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.LABEL.CUSTOM_AVERAGE_STR_LENGTH': 7,
            },
            errors=[],
        )


if __name__ == '__main__':
    unittest.main()
