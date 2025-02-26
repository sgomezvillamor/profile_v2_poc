import unittest
from unittest.mock import MagicMock, patch

from profile_v2.core.bigquery.bigquery import (
    BigQueryInformationSchemaProfileEngine, BigQueryUtils)
from profile_v2.core.model import (BatchSpec, CustomStatistic, DataSource,
                                   FailureStatisticResult,
                                   FailureStatisticResultType, ProfileRequest,
                                   ProfileResponse, ProfileStatisticType,
                                   SuccessStatisticResult, TypedStatistic)


class TestBigQueryUtils(unittest.TestCase):

    def test_bigquerydataset_from_batch_spec(self):
        assert "dataset" == BigQueryUtils.bigquerydataset_from_batch_spec(
            BatchSpec(fq_dataset_name="project.dataset.table")
        )

    def test_bigquerytable_from_batch_spec(self):
        assert "table" == BigQueryUtils.bigquerytable_from_batch_spec(
            BatchSpec(fq_dataset_name="project.dataset.table")
        )


BIGQUERY_CREDENTIALS_PATH = "/Users/sergio/workspace/github/acryldata/connector-tests/smoke-test/credentials/smoke-test.json"
BIGQUERY_PROJECT = "acryl-staging"
BIGQUERY_DATASET = "customer_demo"
BIGQUERY_CONNECTION_STRING = f"bigquery://{BIGQUERY_PROJECT}/{BIGQUERY_DATASET}"


class TestBigQueryInformationSchemaProfileEngine(unittest.TestCase):

    def test_do_profile_with_unsupported_statistic(self):
        datasource = DataSource(
            name="bigquery",
            connection_string=BIGQUERY_CONNECTION_STRING,
            extra_config={
                "credentials_path": BIGQUERY_CREDENTIALS_PATH,
            },
        )
        requests = [
            ProfileRequest(
                statistics=[
                    CustomStatistic(
                        name="row_count",
                        fq_name="acryl-staging.customer_demo.PurchaseEvent.row_count",
                        sql="irrelevant",
                    ),
                ],
                batch=BatchSpec(
                    fq_dataset_name=f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.PurchaseEvent",
                ),
            )
        ]
        engine = BigQueryInformationSchemaProfileEngine()
        response = engine.do_profile(datasource, requests)
        print(response)

        assert len(response.data) == 1
        assert isinstance(
            response.data["acryl-staging.customer_demo.PurchaseEvent.row_count"],
            FailureStatisticResult,
        )
        assert (
            response.data["acryl-staging.customer_demo.PurchaseEvent.row_count"].type
            == FailureStatisticResultType.UNSUPPORTED
        )

    def test_do_profile_with_supported_statistic(self):
        datasource = DataSource(
            name="bigquery",
            connection_string=BIGQUERY_CONNECTION_STRING,
            extra_config={
                "credentials_path": BIGQUERY_CREDENTIALS_PATH,
            },
        )
        requests = [
            ProfileRequest(
                statistics=[
                    TypedStatistic(
                        name="row_count",
                        fq_name="acryl-staging.customer_demo.PurchaseEvent.row_count",
                        columns=[],  # Not used
                        type=ProfileStatisticType.TABLE_ROW_COUNT,
                    ),
                ],
                batch=BatchSpec(
                    fq_dataset_name=f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.PurchaseEvent",
                ),
            )
        ]
        engine = BigQueryInformationSchemaProfileEngine()
        response = engine.do_profile(datasource, requests)
        print(response)

        assert response == ProfileResponse(
            data={
                "acryl-staging.customer_demo.PurchaseEvent.row_count": SuccessStatisticResult(
                    value=68
                )
            },
        )

    def test_do_profile_with_supported_statistic_across_multiple_tables(self):
        datasource = DataSource(
            name="bigquery",
            connection_string=BIGQUERY_CONNECTION_STRING,
            extra_config={
                "credentials_path": BIGQUERY_CREDENTIALS_PATH,
            },
        )
        requests = [
            ProfileRequest(
                statistics=[
                    TypedStatistic(
                        name="row_count",
                        fq_name="acryl-staging.customer_demo.PurchaseEvent.row_count",
                        columns=[],  # Not used
                        type=ProfileStatisticType.TABLE_ROW_COUNT,
                    ),
                ],
                batch=BatchSpec(
                    fq_dataset_name=f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.PurchaseEvent",
                ),
            ),
            ProfileRequest(
                statistics=[
                    TypedStatistic(
                        name="row_count",
                        fq_name="acryl-staging.customer_demo.revenue.row_count",
                        columns=[],  # Not used
                        type=ProfileStatisticType.TABLE_ROW_COUNT,
                    ),
                ],
                batch=BatchSpec(
                    fq_dataset_name=f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.revenue",
                ),
            ),
            ProfileRequest(
                statistics=[
                    TypedStatistic(
                        name="row_count",
                        fq_name="acryl-staging.customer_demo.test_assertions.row_count",
                        columns=[],  # Not used
                        type=ProfileStatisticType.TABLE_ROW_COUNT,
                    ),
                ],
                batch=BatchSpec(
                    fq_dataset_name=f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.test_assertions",
                ),
            ),
        ]
        engine = BigQueryInformationSchemaProfileEngine()
        response = engine.do_profile(datasource, requests)
        print(response)

        assert response == ProfileResponse(
            data={
                "acryl-staging.customer_demo.PurchaseEvent.row_count": SuccessStatisticResult(
                    value=68
                ),
                "acryl-staging.customer_demo.revenue.row_count": SuccessStatisticResult(
                    value=1
                ),
                "acryl-staging.customer_demo.test_assertions.row_count": SuccessStatisticResult(
                    value=20
                ),
            },
        )
