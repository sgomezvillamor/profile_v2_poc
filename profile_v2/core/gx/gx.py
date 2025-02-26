import logging
from typing import List

import great_expectations as gx

from profile_v2.core.api import ProfileEngine
from profile_v2.core.model import (DataSource, FailureStatisticResult,
                                   FailureStatisticResultType, ProfileRequest,
                                   ProfileResponse, ProfileStatisticType,
                                   SuccessStatisticResult, TypedStatistic)

logger = logging.getLogger(__name__)


class GxProfileEngine(ProfileEngine):

    def do_profile(
        self, datasource: DataSource, requests: List[ProfileRequest]
    ) -> ProfileResponse:
        response = ProfileResponse()
        context = gx.get_context()

        data_source = context.data_sources.add_snowflake(
            name=datasource.name,
            connection_string=datasource.connection_string,
        )
        logger.info(f"Data source added: {data_source}")

        for request in requests:
            table_data_asset = data_source.add_table_asset(
                table_name=GxProfileEngine._table_name_from_fq_name(
                    request.batch.fully_qualified_dataset_name
                ),
                name=request.batch.fully_qualified_dataset_name,
            )
            logger.info(f"Table data asset added: {table_data_asset}")
            if request.batch.sample:
                response.data.update(
                    {
                        statistic.fq_name: FailureStatisticResult(
                            type=FailureStatisticResultType.UNSUPPORTED,
                            message="Sampling not supported yet",
                        )
                        for statistic in request.statistics
                    }
                )
                return response

            table_batch_definition = table_data_asset.add_batch_definition_whole_table(
                name="FULL_TABLE"
            )

            suite = gx.ExpectationSuite(name="my_expectation_suite")

            for statistic in request.statistics:
                if isinstance(statistic, TypedStatistic):
                    if statistic.type == ProfileStatisticType.COLUMN_DISTINCT_COUNT:
                        assert len(statistic.columns) == 1
                        expectation = (
                            gx.expectations.ExpectColumnUniqueValueCountToBeBetween(
                                column=statistic.columns[0],
                                min_value=None,
                                max_value=None,
                                meta={
                                    "fq_name": statistic.fq_name
                                },  # just some meta to identify the result
                            )
                        )
                        suite.add_expectation(expectation)
                    else:
                        logger.warning(f"Unsupported typed statistic spec: {statistic}")
                        response.data[statistic.fq_name] = FailureStatisticResult(
                            type=FailureStatisticResultType.UNSUPPORTED,
                            message=f"Unsupported typed statistic spec: {statistic}",
                        )
                else:
                    logger.warning(f"Unsupported statistic spec: {statistic}")
                    response.data[statistic.fq_name] = FailureStatisticResult(
                        type=FailureStatisticResultType.UNSUPPORTED,
                        message=f"Unsupported statistic spec: {statistic}",
                    )

            context.suites.add(suite)

            validation_definition = gx.ValidationDefinition(
                data=table_batch_definition,
                suite=suite,
                name="my_validation_definition",
            )

            validation_results = validation_definition.run()
            logger.info(f"Validation results: {validation_results}")

            for result in validation_results.results:
                response.data[result.expectation_config.meta["fq_name"]] = (
                    SuccessStatisticResult(value=result.result["observed_value"])
                )
                # instead, with Metrics API
                assert (
                    result.get_metric(
                        metric_name=result.expectation_config.type
                        + ".result.observed_value",
                        column=result.expectation_config.kwargs["column"],
                    )
                    == result.result["observed_value"]
                )

        return response

    @staticmethod
    def _table_name_from_fq_name(fq_name: str) -> str:
        return fq_name.split(".")[-1]
