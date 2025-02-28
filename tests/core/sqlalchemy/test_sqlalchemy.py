import unittest
from unittest.mock import Mock

from profile_v2.core.model import (BatchSpec, CustomStatistic, DataSource,
                                   DataSourceType, ProfileRequest,
                                   ProfileResponse, ProfileStatisticType,
                                   SampleSpec, SuccessStatisticResult,
                                   TypedStatistic, UnsuccessfulStatisticResult,
                                   UnsuccessfulStatisticResultType)
from profile_v2.core.sqlalchemy.sqlalchemy import SqlAlchemyProfileEngine
from tests.core.common import (BIGQUERY_CONNECTION_STRING,
                               BIGQUERY_CREDENTIALS_PATH,
                               BIGQUERY_DATASET_CUSTOMER_DEMO,
                               BIGQUERY_PROJECT, SNOWFLAKE_CONNECTION_STRING,
                               SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA)


class TestSqlAlchemyProfileEngine(unittest.TestCase):

    def test_sqlglotfriendly_table_name(self):
        assert (
            SqlAlchemyProfileEngine._sqlglotfriendly_table_name("XXXX.YYY.ZZZ")
            == "YYY.ZZZ"
        )
        assert (
            SqlAlchemyProfileEngine._sqlglotfriendly_table_name("YYY.ZZZ") == "YYY.ZZZ"
        )
        # should these be allowed?
        assert SqlAlchemyProfileEngine._sqlglotfriendly_table_name("ZZZ") == "ZZZ"
        assert SqlAlchemyProfileEngine._sqlglotfriendly_table_name("") == ""
        assert (
            SqlAlchemyProfileEngine._sqlglotfriendly_table_name("AAA.BBB.CCC.DDD.EEE")
            == "DDD.EEE"
        )

    def test_sqlfriendly_column_name(self):
        assert (
            SqlAlchemyProfileEngine._sqlfriendly_column_name("column.name")
            == "column_name"
        )
        assert (
            SqlAlchemyProfileEngine._sqlfriendly_column_name("column name")
            == "column_name"
        )
        assert (
            SqlAlchemyProfileEngine._sqlfriendly_column_name("column-name")
            == "column_name"
        )
        assert (
            SqlAlchemyProfileEngine._sqlfriendly_column_name("COLUMN_NAME")
            == "column_name"
        )
        assert (
            SqlAlchemyProfileEngine._sqlfriendly_column_name("Column-Name With.Dots")
            == "column_name_with_dots"
        )

    _snowflake_datasource = DataSource(
        source=DataSourceType.SNOWFLAKE,
        connection_string=SNOWFLAKE_CONNECTION_STRING,
    )

    _bigquery_datasource = DataSource(
        source=DataSourceType.BIGQUERY,
        connection_string=BIGQUERY_CONNECTION_STRING,
        extra_config={
            "credentials_path": BIGQUERY_CREDENTIALS_PATH,
        },
    )

    def test_distinct_count(self):
        profile_engine = SqlAlchemyProfileEngine()

        result = profile_engine.profile(
            datasource=self._snowflake_datasource,
            requests=[
                ProfileRequest(
                    statistics=[
                        TypedStatistic(
                            fq_name="SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.ID.distinct_count",
                            columns=["ID"],
                            type=ProfileStatisticType.COLUMN_DISTINCT_COUNT,
                        ),
                    ],
                    batch=BatchSpec(
                        fq_dataset_name=f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.COVID19_EXTERNAL_TABLE"
                    ),
                )
            ],
        )
        print(result)
        assert result == ProfileResponse(
            data={
                "SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.ID.distinct_count": SuccessStatisticResult(
                    value=398880
                ),
            },
        )

    def test_distinct_count_multiple(self):
        profile_engine = SqlAlchemyProfileEngine()

        result = profile_engine.profile(
            datasource=self._snowflake_datasource,
            requests=[
                ProfileRequest(
                    statistics=[
                        TypedStatistic(
                            fq_name="SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.ID.distinct_count",
                            columns=["ID"],
                            type=ProfileStatisticType.COLUMN_DISTINCT_COUNT,
                        ),
                        TypedStatistic(
                            fq_name="SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.LABEL.distinct_count",
                            columns=["LABEL"],
                            type=ProfileStatisticType.COLUMN_DISTINCT_COUNT,
                        ),
                    ],
                    batch=BatchSpec(
                        fq_dataset_name=f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.COVID19_EXTERNAL_TABLE"
                    ),
                )
            ],
        )
        print(result)
        assert result == ProfileResponse(
            data={
                "SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.ID.distinct_count": SuccessStatisticResult(
                    value=398880
                ),
                "SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.LABEL.distinct_count": SuccessStatisticResult(
                    value=5
                ),
            },
        )

    def test_custom_statistic(self):
        profile_engine = SqlAlchemyProfileEngine()

        result = profile_engine.profile(
            datasource=self._snowflake_datasource,
            requests=[
                ProfileRequest(
                    statistics=[
                        CustomStatistic(
                            fq_name="SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.LABEL.custom_average_str_length",
                            sql="CEIL(AVG(LEN(LABEL)))",
                        ),
                    ],
                    batch=BatchSpec(
                        fq_dataset_name=f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.COVID19_EXTERNAL_TABLE"
                    ),
                )
            ],
        )
        print(result)

        assert result == ProfileResponse(
            data={
                "SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.LABEL.custom_average_str_length": SuccessStatisticResult(
                    value=7
                ),
            },
        )

    def test_sample(self):
        profile_engine = SqlAlchemyProfileEngine()

        result = profile_engine.profile(
            datasource=self._snowflake_datasource,
            requests=[
                ProfileRequest(
                    statistics=[
                        TypedStatistic(
                            fq_name="SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.ID.distinct_count",
                            columns=["ID"],
                            type=ProfileStatisticType.COLUMN_DISTINCT_COUNT,
                        ),
                    ],
                    batch=BatchSpec(
                        fq_dataset_name=f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.COVID19_EXTERNAL_TABLE",
                        sample=SampleSpec(
                            size=100,
                        ),
                    ),
                )
            ],
        )
        print(result)

        assert result == ProfileResponse(
            data={
                "SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.ID.distinct_count": SuccessStatisticResult(
                    value=100
                ),
            },
        )

    def test_different_datasets(self):
        profile_engine = SqlAlchemyProfileEngine()

        result = profile_engine.profile(
            datasource=self._snowflake_datasource,
            requests=[
                ProfileRequest(
                    statistics=[
                        TypedStatistic(
                            fq_name="SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.ID.distinct_count",
                            columns=["ID"],
                            type=ProfileStatisticType.COLUMN_DISTINCT_COUNT,
                        ),
                    ],
                    batch=BatchSpec(
                        fq_dataset_name=f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.COVID19_EXTERNAL_TABLE",
                    ),
                ),
                ProfileRequest(
                    statistics=[
                        TypedStatistic(
                            fq_name="SMOKE_TEST_DB.PUBLIC.TABLE_FROM_S3_STAGE.FILENAME.distinct_count",
                            columns=["FILENAME"],
                            type=ProfileStatisticType.COLUMN_DISTINCT_COUNT,
                        ),
                    ],
                    batch=BatchSpec(
                        fq_dataset_name=f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.TABLE_FROM_S3_STAGE",
                    ),
                ),
            ],
        )
        print(result)

        assert result == ProfileResponse(
            data={
                "SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.ID.distinct_count": SuccessStatisticResult(
                    value=398880
                ),
                "SMOKE_TEST_DB.PUBLIC.TABLE_FROM_S3_STAGE.FILENAME.distinct_count": SuccessStatisticResult(
                    value=1
                ),
            },
        )

    def test_force_partial_failure_with_invalid_sql(self):
        requests = [
            ProfileRequest(
                statistics=[
                    CustomStatistic(
                        fq_name="fq_name_1",
                        sql="some_invalid_sql_here",
                    ),
                ],
                batch=BatchSpec(
                    fq_dataset_name=f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.TABLE_FROM_S3_STAGE",
                ),
            ),
            ProfileRequest(
                statistics=[
                    CustomStatistic(
                        fq_name="fq_name_2",
                        sql="COUNT(*)",
                    ),
                ],
                batch=BatchSpec(
                    fq_dataset_name=f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.COVID19_EXTERNAL_TABLE",
                ),
            ),
        ]

        profile_engine = SqlAlchemyProfileEngine()

        response = profile_engine.profile(
            datasource=self._snowflake_datasource,
            requests=requests,
        )
        print(response)

        assert len(response.data) == 2
        assert (
            isinstance(response.data["fq_name_1"], UnsuccessfulStatisticResult)
            and response.data["fq_name_1"].type
            == UnsuccessfulStatisticResultType.FAILURE
        )
        assert isinstance(response.data["fq_name_2"], SuccessStatisticResult)

    def test_dialect_transpile_with_custom_statistic(self):
        profile_engine = SqlAlchemyProfileEngine()

        statistics = [
            CustomStatistic(
                fq_name="fq_name_1",
                sql="COUNT(UUID())",
            ),
        ]

        # snowflake

        request = ProfileRequest(
            statistics=statistics,
            batch=BatchSpec(
                fq_dataset_name=f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.COVID19_EXTERNAL_TABLE",
            ),
        )
        response = profile_engine.profile(
            datasource=self._snowflake_datasource,
            requests=[request],
        )
        print(response)
        assert len(response.data) == 1
        assert isinstance(response.data["fq_name_1"], SuccessStatisticResult)

        # observed logs
        # INFO     profile_v2.core.sqlalchemy.sqlalchemy:sqlalchemy.py:54 Generic SQL statement: SELECT COUNT(UUID()) AS fq_name_1 FROM PUBLIC.COVID19_EXTERNAL_TABLE
        # INFO     profile_v2.core.sqlalchemy.sqlalchemy:sqlalchemy.py:58 Dialect-specific SQL statement: SELECT COUNT(UUID_STRING()) AS fq_name_1 FROM PUBLIC.COVID19_EXTERNAL_TABLE

        # bigquery

        request = ProfileRequest(
            statistics=statistics,
            batch=BatchSpec(
                fq_dataset_name=f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET_CUSTOMER_DEMO}.PurchaseEvent",
            ),
        )
        response = profile_engine.profile(
            datasource=self._bigquery_datasource,
            requests=[request],
        )
        assert len(response.data) == 1
        assert isinstance(response.data["fq_name_1"], SuccessStatisticResult)

        # observed logs
        # INFO     profile_v2.core.sqlalchemy.sqlalchemy:sqlalchemy.py:54 Generic SQL statement: SELECT COUNT(UUID()) AS fq_name_1 FROM customer_demo.PurchaseEvent
        # INFO     profile_v2.core.sqlalchemy.sqlalchemy:sqlalchemy.py:58 Dialect-specific SQL statement: SELECT COUNT(GENERATE_UUID()) AS fq_name_1 FROM customer_demo.PurchaseEvent
