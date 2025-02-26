import logging
from typing import List

from sqlalchemy import create_engine, text

from profile_v2.core.api import ProfileEngine
from profile_v2.core.model import (BatchSpec, DataSource,
                                   FailureStatisticResult,
                                   FailureStatisticResultType, ProfileRequest,
                                   ProfileResponse, ProfileStatisticType,
                                   StatisticSpec, SuccessStatisticResult,
                                   TypedStatistic)
from profile_v2.core.utils import ModelCollections

logger = logging.getLogger(__name__)


class BigQueryUtils:
    @staticmethod
    def bigquerydataset_from_batch_spec(batch: BatchSpec) -> str:
        return batch.fq_dataset_name.split(".")[1]

    @staticmethod
    def bigquerytable_from_batch_spec(batch: BatchSpec) -> str:
        return batch.fq_dataset_name.split(".")[2]


class BigQueryInformationSchemaProfileEngine(ProfileEngine):
    """
    Profile engine for BigQuery using INFORMATION_SCHEMA.

    Supported statistics are:
    - TABLE_ROW_COUNT

    Everything else is unsupported.

    While requests can span for multiple bigquery tables and datasets, requests are internally processed in batches by dataset.
    """

    def do_profile(
        self, datasource: DataSource, requests: List[ProfileRequest]
    ) -> ProfileResponse:
        response = ProfileResponse()

        split = ModelCollections.split_request_by_statistics(
            requests, BigQueryInformationSchemaProfileEngine._is_statistic_supported
        )
        supported_requests = split[True]
        unsupported_requests = split[False]

        for unsupported_request in unsupported_requests:
            for statistic in unsupported_request.statistics:
                response.data[statistic.fq_name] = FailureStatisticResult(
                    type=FailureStatisticResultType.UNSUPPORTED,
                    message=f"Unsupported statistic spec: {statistic}",
                )

        supported_requests_by_dataset = ModelCollections.split_requests_by_batch(
            supported_requests,
            predicate=BigQueryUtils.bigquerydataset_from_batch_spec,
        )

        assert datasource.extra_config and datasource.extra_config.get(
            "credentials_path"
        ), "credentials_path is required for BigQuery"
        engine = create_engine(
            datasource.connection_string,
            credentials_path=datasource.extra_config["credentials_path"],
        )

        for dataset, requests in supported_requests_by_dataset.items():
            select_query = f"select table_id, row_count from {dataset}.__TABLES__"
            # TODO: add where clause with table_ids from [extract-table-id(request.batch) for request in requests]
            logger.info(text(select_query))
            with engine.connect() as conn:
                result = conn.execute(text(select_query))
                row_counts_by_table = {row[0]: row[1] for row in result}
                logger.info(row_counts_by_table)

                for request in requests:
                    for statistic in request.statistics:
                        assert BigQueryInformationSchemaProfileEngine._is_statistic_supported(
                            statistic
                        )
                        statistic_fq_name = statistic.fq_name
                        table_name = BigQueryUtils.bigquerytable_from_batch_spec(
                            request.batch
                        )
                        response.data[statistic_fq_name] = SuccessStatisticResult(
                            value=row_counts_by_table[table_name]
                        )

        return response

    @staticmethod
    def _is_statistic_supported(statistic_spec: StatisticSpec) -> bool:
        return isinstance(statistic_spec, TypedStatistic) and statistic_spec.type in [
            ProfileStatisticType.TABLE_ROW_COUNT,
        ]
