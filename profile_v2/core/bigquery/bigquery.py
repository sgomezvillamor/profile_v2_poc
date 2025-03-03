import logging
from typing import Dict, List, Tuple

from sqlalchemy import text

from profile_v2.core.api import ProfileEngine
from profile_v2.core.api_utils import ModelCollections, ParallelProfileEngine
from profile_v2.core.model import (BatchSpec, DataSource,
                                   ProfileNonFunctionalRequirements,
                                   ProfileRequest, ProfileResponse,
                                   ProfileStatisticType, StatisticSpec,
                                   SuccessStatisticResult, TypedStatistic,
                                   UnsuccessfulStatisticResult,
                                   UnsuccessfulStatisticResultType)
from profile_v2.core.report import ProfileCoreReport
from profile_v2.core.sqlalchemy.sqlalchemy import SqlAlchemyProfileEngine

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

    def __init__(self, report: ProfileCoreReport = ProfileCoreReport()):
        super().__init__(report)

    def _do_profile(
        self,
        datasource: DataSource,
        requests: List[ProfileRequest],
        non_functional_requirements: ProfileNonFunctionalRequirements = ProfileNonFunctionalRequirements(),
    ) -> ProfileResponse:
        response = ProfileResponse()

        split = ModelCollections.group_request_by_statistics_predicate(
            requests, BigQueryInformationSchemaProfileEngine._is_statistic_supported
        )
        supported_requests = split[True]
        unsupported_requests = split[False]

        for unsupported_request in unsupported_requests:
            for statistic in unsupported_request.statistics:
                response.data[statistic.fq_name] = UnsuccessfulStatisticResult(
                    type=UnsuccessfulStatisticResultType.UNSUPPORTED,
                    message=f"Unsupported statistic spec: {statistic}",
                )

        supported_requests_by_dataset = (
            ModelCollections.group_requests_by_batch_predicate(
                supported_requests,
                predicate=BigQueryUtils.bigquerydataset_from_batch_spec,
            )
        )

        engine = SqlAlchemyProfileEngine.create_engine(datasource)

        for dataset, requests in supported_requests_by_dataset.items():
            select_query = f"select table_id, row_count from {dataset}.__TABLES__"
            # TODO: add where clause with table_ids from [extract-table-id(request.batch) for request in requests]
            logger.info(text(select_query))
            with engine.connect() as conn:
                try:
                    self.report_issue_query()

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
                except Exception as e:
                    self.report_unsuccessful_query(
                        UnsuccessfulStatisticResultType.FAILURE
                    )
                    logger.error(f"Error profiling requests: {requests}")
                    logger.exception(e)
                    for request in requests:
                        failed_response_for_request = ModelCollections.failed_response_for_request(
                            request,
                            unsuccessful_result_type=UnsuccessfulStatisticResultType.FAILURE,
                            message=str(e),
                            exception=e,
                        )
                        response.update(failed_response_for_request)
                else:
                    self.report_successful_query()

        return response

    @staticmethod
    def _is_statistic_supported(statistic_spec: StatisticSpec) -> bool:
        return (
            isinstance(statistic_spec, TypedStatistic)
            and statistic_spec.type.is_table_level()
            and statistic_spec.type
            in [
                ProfileStatisticType.TABLE_ROW_COUNT,
            ]
        )


class BigQueryProfileEngine(ProfileEngine):
    """
    Profile engine for BigQuery.

    Table level statistics are solved with the BigQueryInformationSchemaProfileEngine.
    While all other statistics are solved with the SqlAlchemyProfileEngine, and running requests in parallel with
    ParallelProfileEngine.
    """

    @staticmethod
    def _separate_information_schema_requests(
        requests: List[ProfileRequest],
    ) -> Tuple[List[ProfileRequest], List[ProfileRequest]]:
        information_schema_requests: List[ProfileRequest] = []
        other_requests: List[ProfileRequest] = []

        for request in requests:
            for statistic in request.statistics:
                if BigQueryInformationSchemaProfileEngine._is_statistic_supported(
                    statistic
                ):
                    information_schema_requests.append(
                        ProfileRequest(batch=request.batch, statistics=[statistic])
                    )
                else:
                    other_requests.append(
                        ProfileRequest(batch=request.batch, statistics=[statistic])
                    )

        information_schema_requests = ModelCollections.join_statistics_by_batch(
            information_schema_requests
        )
        other_requests = ModelCollections.join_statistics_by_batch(other_requests)
        return information_schema_requests, other_requests

    @staticmethod
    def _group_requests_by_bigquerydataset(
        requests: List[ProfileRequest],
    ) -> List[List[ProfileRequest]]:
        requests_by_dataset: Dict[str, List[ProfileRequest]] = (
            ModelCollections.group_requests_by_batch_predicate(
                requests,
                predicate=BigQueryUtils.bigquerydataset_from_batch_spec,
            )
        )
        return [requests for requests in requests_by_dataset.values()]

    def __init__(
        self, report: ProfileCoreReport = ProfileCoreReport(), max_workers: int = 4
    ):
        super().__init__(report)

        self.bq_information_schema_profile_engine = (
            BigQueryInformationSchemaProfileEngine(report=self.report)
        )
        self.parallel_sqlalchemy_profile_engine = ParallelProfileEngine(
            engine=SqlAlchemyProfileEngine(report=self.report),
            max_workers=max_workers,
            batch_requests_predicate=BigQueryProfileEngine._group_requests_by_bigquerydataset,
        )

    def _do_profile(
        self,
        datasource: DataSource,
        requests: List[ProfileRequest],
        non_functional_requirements: ProfileNonFunctionalRequirements = ProfileNonFunctionalRequirements(),
    ) -> ProfileResponse:
        response = ProfileResponse()

        information_schema_requests, other_requests = (
            BigQueryProfileEngine._separate_information_schema_requests(requests)
        )
        information_schema_response = (
            self.bq_information_schema_profile_engine._do_profile(
                datasource, information_schema_requests
            )
        )
        other_requests_response = self.parallel_sqlalchemy_profile_engine._do_profile(
            datasource, other_requests
        )

        response.update(information_schema_response)
        response.update(other_requests_response)

        return response
