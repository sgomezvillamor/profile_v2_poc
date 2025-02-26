import os
import urllib.parse

import pytest

from profile_v2.core.gx.gx import GxProfileEngine
from profile_v2.core.model import (BatchSpec, CustomStatistic, DataSource,
                                   FailureStatisticResult,
                                   FailureStatisticResultType, ProfileRequest,
                                   ProfileResponse, ProfileStatisticType,
                                   SampleSpec, SuccessStatisticResult,
                                   TypedStatistic)
from profile_v2.core.sqlalchemy.sqlalchemy import SqlAlchemyProfileEngine

SNOWFLAKE_USER = urllib.parse.quote(os.environ["SNOWFLAKE_USER"])
SNOWFLAKE_PASSWORD = urllib.parse.quote(os.environ["SNOWFLAKE_PASSWORD"])
SNOWFLAKE_ACCOUNT = "cfa31444"
SNOWFLAKE_DATABASE = "SMOKE_TEST_DB"
SNOWFLAKE_SCHEMA = "PUBLIC"
SNOWFLAKE_WAREHOUSE = "SMOKE_TEST"
SNOWFLAKE_ROLE = "datahub_role"
SNOWFLAKE_CONNECTION_STRING = f"snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}@{SNOWFLAKE_ACCOUNT}/{SNOWFLAKE_DATABASE}/{SNOWFLAKE_SCHEMA}?warehouse={SNOWFLAKE_WAREHOUSE}&role={SNOWFLAKE_ROLE}&application=datahub"


@pytest.mark.parametrize("engine_cls", [GxProfileEngine, SqlAlchemyProfileEngine])
def test_api_distinct_count(engine_cls):
    profile_engine = engine_cls()

    result = profile_engine.do_profile(
        datasource=DataSource(
            name="snowlake",
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

    result = profile_engine.do_profile(
        datasource=DataSource(
            name="snowlake",
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

    result = profile_engine.do_profile(
        datasource=DataSource(
            name="snowlake",
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

    result = profile_engine.do_profile(
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
