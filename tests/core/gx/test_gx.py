import unittest

from profile_v2.core.gx.gx import GxProfileEngine
from profile_v2.core.model import (BatchSpec, CustomStatistic, DataSource,
                                   DataSourceType, ProfileRequest,
                                   ProfileResponse, ProfileStatisticType,
                                   SampleSpec, SuccessStatisticResult,
                                   TypedStatistic, UnsuccessfulStatisticResult,
                                   UnsuccessfulStatisticResultType)
from tests.core.common import (SNOWFLAKE_CONNECTION_STRING, SNOWFLAKE_DATABASE,
                               SNOWFLAKE_SCHEMA)


class TestGxProfileEngine(unittest.TestCase):

    _datasource = DataSource(
        source=DataSourceType.SNOWFLAKE,
        connection_string=SNOWFLAKE_CONNECTION_STRING,
    )

    def test_distinct_count(self):
        profile_engine = GxProfileEngine()

        result = profile_engine.profile(
            datasource=self._datasource,
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
        profile_engine = GxProfileEngine()

        result = profile_engine.profile(
            datasource=self._datasource,
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
        profile_engine = GxProfileEngine()

        result = profile_engine.profile(
            datasource=self._datasource,
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

        assert len(result.data) == 1
        assert isinstance(
            result.data[
                "SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.LABEL.custom_average_str_length"
            ],
            UnsuccessfulStatisticResult,
        )
        assert (
            result.data[
                "SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.LABEL.custom_average_str_length"
            ].type
            == UnsuccessfulStatisticResultType.UNSUPPORTED
        )

    def test_sample(self):
        profile_engine = GxProfileEngine()

        result = profile_engine.profile(
            datasource=self._datasource,
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

        assert len(result.data) == 1

        assert isinstance(
            result.data[
                "SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.ID.distinct_count"
            ],
            UnsuccessfulStatisticResult,
        )
        assert (
            result.data[
                "SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.ID.distinct_count"
            ].type
            == UnsuccessfulStatisticResultType.UNSUPPORTED
        )

    def test_different_datasets(self):
        profile_engine = GxProfileEngine()

        result = profile_engine.profile(
            datasource=self._datasource,
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
