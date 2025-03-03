import logging
import random
from typing import List

import great_expectations as gx

from profile_v2.core.api import ProfileEngine
from profile_v2.core.model import (DataSource,
                                   ProfileNonFunctionalRequirements,
                                   ProfileRequest, ProfileResponse,
                                   ProfileStatisticType,
                                   SuccessStatisticResult, TypedStatistic,
                                   UnsuccessfulStatisticResult,
                                   UnsuccessfulStatisticResultType)
from profile_v2.core.model_utils import ModelCollections

logger = logging.getLogger(__name__)


class GxProfileEngine(ProfileEngine):
    """
    Generic profile engine using Great Expectations.

    Restrictions:
    - all requests must be for the same batch

    TODO:
    - sampling
    - other statistics different from COLUMN_DISTINCT_COUNT
    - custom statistics
    """

    def _do_profile(
        self,
        datasource: DataSource,
        requests: List[ProfileRequest],
        non_functional_requirements: ProfileNonFunctionalRequirements = ProfileNonFunctionalRequirements(),
    ) -> ProfileResponse:
        response = ProfileResponse()
        context = gx.get_context()

        data_source = context.data_sources.add_snowflake(
            name=datasource.source.value,
            connection_string=datasource.connection_string,
        )
        logger.info(f"Data source added: {data_source}")

        for request in requests:
            table_data_asset = data_source.add_table_asset(
                table_name=GxProfileEngine._table_name_from_fq_name(
                    request.batch.fq_dataset_name
                ),
                name=request.batch.fq_dataset_name,
            )
            logger.info(f"Table data asset added: {table_data_asset}")
            if request.batch.sample:
                failed_response = ModelCollections.failed_response_for_request(
                    request,
                    UnsuccessfulStatisticResultType.UNSUPPORTED,
                    "Sampling not supported yet",
                )
                response.update(failed_response)
                return response

            table_batch_definition = table_data_asset.add_batch_definition_whole_table(
                name="FULL_TABLE"
            )

            suite = gx.ExpectationSuite(
                name=GxProfileEngine._random_suite_name(),
            )

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
                        response.data[statistic.fq_name] = UnsuccessfulStatisticResult(
                            type=UnsuccessfulStatisticResultType.UNSUPPORTED,
                            message=f"Unsupported typed statistic spec: {statistic}",
                        )
                else:
                    logger.warning(f"Unsupported statistic spec: {statistic}")
                    response.data[statistic.fq_name] = UnsuccessfulStatisticResult(
                        type=UnsuccessfulStatisticResultType.UNSUPPORTED,
                        message=f"Unsupported statistic spec: {statistic}",
                    )

            context.suites.add(suite)

            validation_definition = gx.ValidationDefinition(
                data=table_batch_definition,
                suite=suite,
                name=GxProfileEngine._random_validation_definition_name(),
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

    @staticmethod
    def _random_suite_name() -> str:
        return f"suite_{random.randint(100, 999)}"

    @staticmethod
    def _random_validation_definition_name() -> str:
        return f"validation_definition_{random.randint(100, 999)}"
