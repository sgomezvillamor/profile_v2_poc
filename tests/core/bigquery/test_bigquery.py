import unittest

from profile_v2.core.bigquery.bigquery import (
    BigQueryInformationSchemaProfileEngine, BigQueryProfileEngine,
    BigQueryUtils)
from profile_v2.core.model import (BatchSpec, CustomStatistic, DataSource,
                                   DataSourceType, ProfileRequest,
                                   ProfileResponse, ProfileStatisticType,
                                   SuccessStatisticResult, TypedStatistic,
                                   UnsuccessfulStatisticResult,
                                   UnsuccessfulStatisticResultType)
from profile_v2.core.report import ProfileCoreReport
from tests.core.common import (BIGQUERY_CONNECTION_STRING,
                               BIGQUERY_CREDENTIALS_PATH,
                               BIGQUERY_DATASET_CUSTOMER_DEMO,
                               BIGQUERY_DATASET_DEPLOY_TEST_1K,
                               BIGQUERY_PROJECT)


class TestBigQueryUtils(unittest.TestCase):

    def test_bigquerydataset_from_batch_spec(self):
        assert "dataset" == BigQueryUtils.bigquerydataset_from_batch_spec(
            BatchSpec(fq_dataset_name="project.dataset.table")
        )

    def test_bigquerytable_from_batch_spec(self):
        assert "table" == BigQueryUtils.bigquerytable_from_batch_spec(
            BatchSpec(fq_dataset_name="project.dataset.table")
        )


class TestBigQueryInformationSchemaProfileEngine(unittest.TestCase):

    _datasource = DataSource(
        source=DataSourceType.BIGQUERY,
        connection_string=BIGQUERY_CONNECTION_STRING,
        extra_config={
            "credentials_path": BIGQUERY_CREDENTIALS_PATH,
        },
    )

    def test_profile_with_unsupported_statistic(self):

        requests = [
            ProfileRequest(
                statistics=[
                    CustomStatistic(
                        fq_name="acryl-staging.customer_demo.PurchaseEvent.row_count",
                        sql="irrelevant",
                    ),
                ],
                batch=BatchSpec(
                    fq_dataset_name=f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET_CUSTOMER_DEMO}.PurchaseEvent",
                ),
            )
        ]
        engine = BigQueryInformationSchemaProfileEngine()
        response = engine.profile(self._datasource, requests)
        print(response)

        assert len(response.data) == 1
        assert isinstance(
            response.data["acryl-staging.customer_demo.PurchaseEvent.row_count"],
            UnsuccessfulStatisticResult,
        )
        assert (
            response.data["acryl-staging.customer_demo.PurchaseEvent.row_count"].type
            == UnsuccessfulStatisticResultType.UNSUPPORTED
        )

    def test_profile_with_supported_statistic(self):
        requests = [
            ProfileRequest(
                statistics=[
                    TypedStatistic(
                        fq_name="acryl-staging.customer_demo.PurchaseEvent.row_count",
                        type=ProfileStatisticType.TABLE_ROW_COUNT,
                    ),
                ],
                batch=BatchSpec(
                    fq_dataset_name=f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET_CUSTOMER_DEMO}.PurchaseEvent",
                ),
            )
        ]
        engine = BigQueryInformationSchemaProfileEngine()
        response = engine.profile(self._datasource, requests)
        print(response)

        assert response == ProfileResponse(
            data={
                "acryl-staging.customer_demo.PurchaseEvent.row_count": SuccessStatisticResult(
                    value=68
                )
            },
        )

    def test_profile_with_supported_statistic_across_multiple_tables(self):
        requests = [
            ProfileRequest(
                statistics=[
                    TypedStatistic(
                        fq_name="acryl-staging.customer_demo.PurchaseEvent.row_count",
                        type=ProfileStatisticType.TABLE_ROW_COUNT,
                    ),
                ],
                batch=BatchSpec(
                    fq_dataset_name=f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET_CUSTOMER_DEMO}.PurchaseEvent",
                ),
            ),
            ProfileRequest(
                statistics=[
                    TypedStatistic(
                        fq_name="acryl-staging.customer_demo.revenue.row_count",
                        type=ProfileStatisticType.TABLE_ROW_COUNT,
                    ),
                ],
                batch=BatchSpec(
                    fq_dataset_name=f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET_CUSTOMER_DEMO}.revenue",
                ),
            ),
            ProfileRequest(
                statistics=[
                    TypedStatistic(
                        fq_name="acryl-staging.customer_demo.test_assertions.row_count",
                        type=ProfileStatisticType.TABLE_ROW_COUNT,
                    ),
                ],
                batch=BatchSpec(
                    fq_dataset_name=f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET_CUSTOMER_DEMO}.test_assertions",
                ),
            ),
        ]
        engine = BigQueryInformationSchemaProfileEngine()
        response = engine.profile(self._datasource, requests)
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

    def test_profile_with_supported_statistic_across_multiple_datasets(self):
        requests = [
            ProfileRequest(
                statistics=[
                    TypedStatistic(
                        fq_name="acryl-staging.customer_demo.PurchaseEvent.row_count",
                        type=ProfileStatisticType.TABLE_ROW_COUNT,
                    ),
                ],
                batch=BatchSpec(
                    fq_dataset_name=f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET_CUSTOMER_DEMO}.PurchaseEvent",
                ),
            ),
            ProfileRequest(
                statistics=[
                    TypedStatistic(
                        fq_name="acryl-staging.customer_demo.revenue.row_count",
                        type=ProfileStatisticType.TABLE_ROW_COUNT,
                    ),
                ],
                batch=BatchSpec(
                    fq_dataset_name=f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET_CUSTOMER_DEMO}.revenue",
                ),
            ),
            ProfileRequest(
                statistics=[
                    TypedStatistic(
                        fq_name="acryl-staging.deploy_test_1k.table_1.row_count",
                        type=ProfileStatisticType.TABLE_ROW_COUNT,
                    ),
                ],
                batch=BatchSpec(
                    fq_dataset_name=f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET_DEPLOY_TEST_1K}.table_1",
                ),
            ),
            ProfileRequest(
                statistics=[
                    TypedStatistic(
                        fq_name="acryl-staging.deploy_test_1k.table_10.row_count",
                        type=ProfileStatisticType.TABLE_ROW_COUNT,
                    ),
                ],
                batch=BatchSpec(
                    fq_dataset_name=f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET_DEPLOY_TEST_1K}.table_10",
                ),
            ),
        ]
        engine = BigQueryInformationSchemaProfileEngine()
        response = engine.profile(self._datasource, requests)
        print(response)

        assert response == ProfileResponse(
            data={
                "acryl-staging.customer_demo.PurchaseEvent.row_count": SuccessStatisticResult(
                    value=68
                ),
                "acryl-staging.customer_demo.revenue.row_count": SuccessStatisticResult(
                    value=1
                ),
                "acryl-staging.deploy_test_1k.table_1.row_count": SuccessStatisticResult(
                    value=0
                ),
                "acryl-staging.deploy_test_1k.table_10.row_count": SuccessStatisticResult(
                    value=0
                ),
            },
        )

    def test_profile_groups_requests_by_bigquerydataset(self):
        requests = [
            ProfileRequest(
                statistics=[
                    TypedStatistic(
                        fq_name="acryl-staging.customer_demo.PurchaseEvent.row_count",
                        type=ProfileStatisticType.TABLE_ROW_COUNT,
                    ),
                ],
                batch=BatchSpec(
                    fq_dataset_name=f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET_CUSTOMER_DEMO}.PurchaseEvent",
                ),
            ),
            ProfileRequest(
                statistics=[
                    TypedStatistic(
                        fq_name="acryl-staging.customer_demo.revenue.row_count",
                        type=ProfileStatisticType.TABLE_ROW_COUNT,
                    ),
                ],
                batch=BatchSpec(
                    fq_dataset_name=f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET_CUSTOMER_DEMO}.revenue",
                ),
            ),
            ProfileRequest(
                statistics=[
                    TypedStatistic(
                        fq_name="acryl-staging.customer_demo.test_assertions.row_count",
                        type=ProfileStatisticType.TABLE_ROW_COUNT,
                    ),
                ],
                batch=BatchSpec(
                    fq_dataset_name=f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET_CUSTOMER_DEMO}.test_assertions",
                ),
            ),
            ProfileRequest(
                statistics=[
                    TypedStatistic(
                        fq_name="acryl-staging.deploy_test_1k.table_1.row_count",
                        type=ProfileStatisticType.TABLE_ROW_COUNT,
                    ),
                ],
                batch=BatchSpec(
                    fq_dataset_name=f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET_DEPLOY_TEST_1K}.table_1",
                ),
            ),
        ]
        engine = BigQueryInformationSchemaProfileEngine(
            report=ProfileCoreReport(),  # instantiate report to avoid mutable default argument issues
        )
        _ = engine.profile(self._datasource, requests)

        assert (
            engine.report.num_issued_queries_by_engine[
                "BigQueryInformationSchemaProfileEngine"
            ]
            == 2
        )

    def test_fail_non_existing_bigquerydataset(self):
        requests = [
            ProfileRequest(
                statistics=[
                    TypedStatistic(
                        fq_name="acryl-staging.non_existing_dataset.table1.row_count",
                        type=ProfileStatisticType.TABLE_ROW_COUNT,
                    ),
                ],
                batch=BatchSpec(
                    fq_dataset_name=f"{BIGQUERY_PROJECT}.non_existing_dataset.table1",
                ),
            ),
            ProfileRequest(
                statistics=[
                    TypedStatistic(
                        fq_name="acryl-staging.non_existing_dataset.table2.row_count",
                        type=ProfileStatisticType.TABLE_ROW_COUNT,
                    ),
                ],
                batch=BatchSpec(
                    fq_dataset_name=f"{BIGQUERY_PROJECT}.non_existing_dataset.table2",
                ),
            ),
        ]
        engine = BigQueryInformationSchemaProfileEngine()
        response = engine.profile(self._datasource, requests)
        print(response)
        print(engine.report)
        assert len(response.data) == 2
        assert (
            isinstance(
                response.data["acryl-staging.non_existing_dataset.table1.row_count"],
                UnsuccessfulStatisticResult,
            )
            and response.data[
                "acryl-staging.non_existing_dataset.table1.row_count"
            ].type
            == UnsuccessfulStatisticResultType.FAILURE
        )
        assert (
            isinstance(
                response.data["acryl-staging.non_existing_dataset.table2.row_count"],
                UnsuccessfulStatisticResult,
            )
            and response.data[
                "acryl-staging.non_existing_dataset.table2.row_count"
            ].type
            == UnsuccessfulStatisticResultType.FAILURE
        )


class TestBigQueryProfileEngine(unittest.TestCase):

    _datasource = DataSource(
        source=DataSourceType.BIGQUERY,
        connection_string=BIGQUERY_CONNECTION_STRING,
        extra_config={
            "credentials_path": BIGQUERY_CREDENTIALS_PATH,
        },
    )

    _requests = [
        ProfileRequest(
            statistics=[
                TypedStatistic(
                    fq_name="acryl-staging.customer_demo.PurchaseEvent.row_count",
                    type=ProfileStatisticType.TABLE_ROW_COUNT,
                ),
            ],
            batch=BatchSpec(
                fq_dataset_name=f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET_CUSTOMER_DEMO}.PurchaseEvent",
            ),
        ),
        ProfileRequest(
            statistics=[
                TypedStatistic(
                    fq_name="acryl-staging.customer_demo.revenue.row_count",
                    type=ProfileStatisticType.TABLE_ROW_COUNT,
                ),
            ],
            batch=BatchSpec(
                fq_dataset_name=f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET_CUSTOMER_DEMO}.revenue",
            ),
        ),
        ProfileRequest(
            statistics=[
                TypedStatistic(
                    fq_name="acryl-staging.deploy_test_1k.table_1.row_count",
                    type=ProfileStatisticType.TABLE_ROW_COUNT,
                ),
            ],
            batch=BatchSpec(
                fq_dataset_name=f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET_DEPLOY_TEST_1K}.table_1",
            ),
        ),
        ProfileRequest(
            statistics=[
                TypedStatistic(
                    fq_name="acryl-staging.deploy_test_1k.table_10.row_count",
                    type=ProfileStatisticType.TABLE_ROW_COUNT,
                ),
            ],
            batch=BatchSpec(
                fq_dataset_name=f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET_DEPLOY_TEST_1K}.table_10",
            ),
        ),
        ProfileRequest(
            statistics=[
                TypedStatistic(
                    fq_name="acryl-staging.deploy_test_1k.PurchaseEvent.product_id.distinct_count",
                    columns=["product_id"],
                    type=ProfileStatisticType.COLUMN_DISTINCT_COUNT,
                ),
            ],
            batch=BatchSpec(
                fq_dataset_name=f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET_CUSTOMER_DEMO}.PurchaseEvent",
            ),
        ),
        ProfileRequest(
            statistics=[
                TypedStatistic(
                    fq_name="acryl-staging.deploy_test_1k.PurchaseEvent.user_id.distinct_count",
                    columns=["user_id"],
                    type=ProfileStatisticType.COLUMN_DISTINCT_COUNT,
                ),
            ],
            batch=BatchSpec(
                fq_dataset_name=f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET_CUSTOMER_DEMO}.PurchaseEvent",
            ),
        ),
        ProfileRequest(
            statistics=[
                CustomStatistic(
                    fq_name="acryl-staging.deploy_test_1k.PurchaseEvent.amount.custom_avg",
                    sql="CEIL(AVG(amount))",
                ),
            ],
            batch=BatchSpec(
                fq_dataset_name=f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET_CUSTOMER_DEMO}.PurchaseEvent",
            ),
        ),
        ProfileRequest(
            statistics=[
                TypedStatistic(
                    fq_name="acryl-staging.deploy_test_1k.deploy_test_1k.table_1.distinct_count",
                    columns=["column_0"],
                    type=ProfileStatisticType.COLUMN_DISTINCT_COUNT,
                ),
            ],
            batch=BatchSpec(
                fq_dataset_name=f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET_DEPLOY_TEST_1K}.table_1",
            ),
        ),
    ]

    def test_group_requests_by_bigquerydataset(self):
        batch_requests = BigQueryProfileEngine._group_requests_by_bigquerydataset(
            self._requests
        )
        print(batch_requests)
        assert len(batch_requests) == 2
        distinct_bigquery_datasets_batch_0 = set(
            BigQueryUtils.bigquerydataset_from_batch_spec(request.batch)
            for request in batch_requests[0]
        )
        distinct_bigquery_datasets_batch_1 = set(
            BigQueryUtils.bigquerydataset_from_batch_spec(request.batch)
            for request in batch_requests[1]
        )
        assert (
            len(distinct_bigquery_datasets_batch_0)
            == len(distinct_bigquery_datasets_batch_1)
            == 1
        )
        assert distinct_bigquery_datasets_batch_0 != distinct_bigquery_datasets_batch_1
        assert distinct_bigquery_datasets_batch_0.union(
            distinct_bigquery_datasets_batch_1
        ) == {
            f"{BIGQUERY_DATASET_CUSTOMER_DEMO}",
            f"{BIGQUERY_DATASET_DEPLOY_TEST_1K}",
        }

    def test_integration_test(self):
        engine = BigQueryProfileEngine()
        response = engine.profile(self._datasource, self._requests)
        print(response)
        assert response == ProfileResponse(
            data={
                "acryl-staging.customer_demo.PurchaseEvent.row_count": SuccessStatisticResult(
                    value=68
                ),
                "acryl-staging.customer_demo.revenue.row_count": SuccessStatisticResult(
                    value=1
                ),
                "acryl-staging.deploy_test_1k.table_1.row_count": SuccessStatisticResult(
                    value=0
                ),
                "acryl-staging.deploy_test_1k.table_10.row_count": SuccessStatisticResult(
                    value=0
                ),
                "acryl-staging.deploy_test_1k.PurchaseEvent.product_id.distinct_count": SuccessStatisticResult(
                    value=4
                ),
                "acryl-staging.deploy_test_1k.PurchaseEvent.user_id.distinct_count": SuccessStatisticResult(
                    value=2
                ),
                "acryl-staging.deploy_test_1k.PurchaseEvent.amount.custom_avg": SuccessStatisticResult(
                    value=20.0
                ),
                "acryl-staging.deploy_test_1k.deploy_test_1k.table_1.distinct_count": SuccessStatisticResult(
                    value=0
                ),
            },
        )
        print(engine.report)
