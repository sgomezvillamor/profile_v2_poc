import logging
import great_expectations as gx

from profile_v2.core.model import (
    DataSource,
    ProfileRequest,
    ProfileResponse,
    ProfileStatisticType,
    TypedStatistic,
)

logger = logging.getLogger(__name__)

def do_profile_gx(datasource: DataSource, request: ProfileRequest) -> ProfileResponse:
    response = ProfileResponse()
    context = gx.get_context()

    data_source = context.data_sources.add_snowflake(
        name=datasource.name,
        connection_string=datasource.connection_string,
    )
    logger.info(f"Data source added: {data_source}")
    table_data_asset = data_source.add_table_asset(
        table_name=_table_name_from_fq_name(request.batch.fully_qualified_dataset_name),
        name=request.batch.fully_qualified_dataset_name,
    )
    logger.info(f"Table data asset added: {table_data_asset}")
    table_batch_definition = table_data_asset.add_batch_definition_whole_table(name="FULL_TABLE")

    suite = gx.ExpectationSuite(name="my_expectation_suite")

    for statistic in request.statistics:
        if isinstance(statistic, TypedStatistic):
            if statistic.statistic == ProfileStatisticType.DISTINCT_COUNT:
                assert len(statistic.columns) == 1
                expectation = gx.expectations.ExpectColumnUniqueValueCountToBeBetween(
                    column=statistic.columns[0],
                    min_value=None,
                    max_value=None,
                    meta={"fq_name": statistic.fq_name} # just some meta to identify the result
                )
                suite.add_expectation(expectation)
            else:
                raise ValueError(f"Unsupported typed statistic spec: {statistic.statistic}")
        else:
            raise ValueError(f"Unsupported statistic spec: {statistic}")

    context.suites.add(suite)

    validation_definition = gx.ValidationDefinition(
        data=table_batch_definition, suite=suite, name="my_validation_definition"
    )

    validation_results = validation_definition.run()
    logger.info(f"Validation results: {validation_results}")

    for result in validation_results.results:
        response.data[result.expectation_config.meta['fq_name']] = result.result['observed_value']

    return response


def _table_name_from_fq_name(fq_name: str) -> str:
    return fq_name.split(".")[-1]