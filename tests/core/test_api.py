import os
import unittest
import urllib.parse

import pytest

from profile_v2.core.api import ProfileEngineValueError
from profile_v2.core.gx.gx import GxProfileEngine
from profile_v2.core.model import (BatchSpec, CustomStatistic, DataSource,
                                   FailureStatisticResult,
                                   FailureStatisticResultType, ProfileRequest,
                                   ProfileResponse, ProfileStatisticType,
                                   SampleSpec, StatisticSpec,
                                   SuccessStatisticResult, TypedStatistic)
from profile_v2.core.sqlalchemy.sqlalchemy import SqlAlchemyProfileEngine
from tests.core.common import FixedResponseEngine

SNOWFLAKE_USER = urllib.parse.quote(os.environ["SNOWFLAKE_USER"])
SNOWFLAKE_PASSWORD = urllib.parse.quote(os.environ["SNOWFLAKE_PASSWORD"])
SNOWFLAKE_ACCOUNT = "cfa31444"
SNOWFLAKE_DATABASE = "SMOKE_TEST_DB"
SNOWFLAKE_SCHEMA = "PUBLIC"
SNOWFLAKE_WAREHOUSE = "SMOKE_TEST"
SNOWFLAKE_ROLE = "datahub_role"
SNOWFLAKE_CONNECTION_STRING = f"snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}@{SNOWFLAKE_ACCOUNT}/{SNOWFLAKE_DATABASE}/{SNOWFLAKE_SCHEMA}?warehouse={SNOWFLAKE_WAREHOUSE}&role={SNOWFLAKE_ROLE}&application=datahub"


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


@pytest.mark.parametrize("engine_cls", [GxProfileEngine, SqlAlchemyProfileEngine])
def test_api_distinct_count(engine_cls):
    profile_engine = engine_cls()

    result = profile_engine.profile(
        datasource=DataSource(
            name="snowflake",
            connection_string=SNOWFLAKE_CONNECTION_STRING,
        ),
        requests=[
            ProfileRequest(
                statistics=[
                    TypedStatistic(
                        name=ProfileStatisticType.COLUMN_DISTINCT_COUNT.value,
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


@pytest.mark.parametrize("engine_cls", [GxProfileEngine, SqlAlchemyProfileEngine])
def test_api_distinct_count_multiple(engine_cls):
    profile_engine = engine_cls()

    result = profile_engine.profile(
        datasource=DataSource(
            name="snowflake",
            connection_string=SNOWFLAKE_CONNECTION_STRING,
        ),
        requests=[
            ProfileRequest(
                statistics=[
                    TypedStatistic(
                        name=ProfileStatisticType.COLUMN_DISTINCT_COUNT.value,
                        fq_name="SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.ID.distinct_count",
                        columns=["ID"],
                        type=ProfileStatisticType.COLUMN_DISTINCT_COUNT,
                    ),
                    TypedStatistic(
                        name=ProfileStatisticType.COLUMN_DISTINCT_COUNT.value,
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


@pytest.mark.parametrize("engine_cls", [GxProfileEngine, SqlAlchemyProfileEngine])
def test_api_custom_statistic(engine_cls):
    profile_engine = engine_cls()

    result = profile_engine.profile(
        datasource=DataSource(
            name="snowflake",
            connection_string=SNOWFLAKE_CONNECTION_STRING,
        ),
        requests=[
            ProfileRequest(
                statistics=[
                    CustomStatistic(
                        name="custom_average_str_length",
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

    if engine_cls == GxProfileEngine:
        assert isinstance(
            result.data[
                "SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.LABEL.custom_average_str_length"
            ],
            FailureStatisticResult,
        )
        assert (
            result.data[
                "SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.LABEL.custom_average_str_length"
            ].type
            == FailureStatisticResultType.UNSUPPORTED
        )
    elif engine_cls == SqlAlchemyProfileEngine:
        assert result == ProfileResponse(
            data={
                "SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.LABEL.custom_average_str_length": SuccessStatisticResult(
                    value=7
                ),
            },
        )
    else:
        assert False, "Unknown engine"


@pytest.mark.parametrize("engine_cls", [GxProfileEngine, SqlAlchemyProfileEngine])
def test_api_sample(engine_cls):
    profile_engine = engine_cls()

    result = profile_engine.profile(
        datasource=DataSource(
            name="snowflake",
            connection_string=SNOWFLAKE_CONNECTION_STRING,
        ),
        requests=[
            ProfileRequest(
                statistics=[
                    TypedStatistic(
                        name=ProfileStatisticType.COLUMN_DISTINCT_COUNT.value,
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

    if engine_cls == GxProfileEngine:
        assert isinstance(
            result.data[
                "SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.ID.distinct_count"
            ],
            FailureStatisticResult,
        )
        assert (
            result.data[
                "SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.ID.distinct_count"
            ].type
            == FailureStatisticResultType.UNSUPPORTED
        )
    elif engine_cls == SqlAlchemyProfileEngine:
        assert result == ProfileResponse(
            data={
                "SMOKE_TEST_DB.PUBLIC.COVID19_EXTERNAL_TABLE.ID.distinct_count": SuccessStatisticResult(
                    value=100
                ),
            },
        )
    else:
        assert False, "Unknown engine"


@pytest.mark.parametrize("engine_cls", [GxProfileEngine, SqlAlchemyProfileEngine])
def test_api_different_datasets(engine_cls):
    profile_engine = engine_cls()

    result = profile_engine.profile(
        datasource=DataSource(
            name="snowflake",
            connection_string=SNOWFLAKE_CONNECTION_STRING,
        ),
        requests=[
            ProfileRequest(
                statistics=[
                    TypedStatistic(
                        name=ProfileStatisticType.COLUMN_DISTINCT_COUNT.value,
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
                        name=ProfileStatisticType.COLUMN_DISTINCT_COUNT.value,
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

    assert len(result.data) == 2

    if engine_cls in [GxProfileEngine, SqlAlchemyProfileEngine]:
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
    else:
        assert False, "Unknown engine"
