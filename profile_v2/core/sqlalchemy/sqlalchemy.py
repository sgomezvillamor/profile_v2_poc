import logging
from typing import Any, Dict, Iterator, List, Optional, Tuple

from sqlalchemy import Engine, create_engine, text
from sqlglot.expressions import Select

from profile_v2.core.api import ProfileEngine
from profile_v2.core.model import (CustomStatistic, DataSource, DataSourceType,
                                   ExpensivenessRequirements,
                                   ProfileNonFunctionalRequirements,
                                   ProfileRequest, ProfileResponse,
                                   ProfileStatisticType,
                                   SuccessStatisticResult, TypedStatistic,
                                   UnsuccessfulStatisticResult,
                                   UnsuccessfulStatisticResultType)
from profile_v2.core.model_utils import ModelCollections
from profile_v2.core.report import ProfileCoreReport

logger = logging.getLogger(__name__)


class SqlAlchemyProfileEngine(ProfileEngine):
    """
    Generic profile engine using SQLAlchemy.

    TODO:
    - TABLE_ROW_COUNT statistic, support it depending on "expensiveness" considerations
    """

    def __init__(self, report: ProfileCoreReport = ProfileCoreReport()):
        super().__init__(report)

    @staticmethod
    def create_engine(datasource: DataSource):
        if datasource.source == DataSourceType.SNOWFLAKE:
            return create_engine(datasource.connection_string)
        elif datasource.source == DataSourceType.BIGQUERY:
            assert datasource.extra_config and datasource.extra_config.get(
                "credentials_path"
            ), "credentials_path is required for BigQuery"
            return create_engine(
                datasource.connection_string,
                credentials_path=datasource.extra_config["credentials_path"],
            )
        else:
            assert False, f"Unsupported datasource: {datasource.source}"

    def _do_profile(
        self,
        datasource: DataSource,
        requests: List[ProfileRequest],
        non_functional_requirements: ProfileNonFunctionalRequirements = ProfileNonFunctionalRequirements(),
    ) -> ProfileResponse:
        response = ProfileResponse()

        split = ModelCollections.group_request_by_statistics_predicate(
            requests,
            lambda statistic_spec: statistic_spec.is_table_level(),
        )
        table_level_requests = split[True]
        column_level_requests = split[False]

        engine = SqlAlchemyProfileEngine.create_engine(datasource)

        self._process_table_level_requests(
            table_level_requests,
            datasource,
            engine,
            non_functional_requirements,
            response,
        )
        self._process_column_level_requests(
            column_level_requests, datasource, engine, response
        )

        return response

    def _process_table_level_requests(
        self,
        table_level_requests: List[ProfileRequest],
        datasource: DataSource,
        engine: Engine,
        non_functional_requirements: ProfileNonFunctionalRequirements,
        response: ProfileResponse,
    ):
        if (
            non_functional_requirements.expensiveness
            == ExpensivenessRequirements.UNLIMITED
        ):
            for request in table_level_requests:
                for statistic in request.statistics:
                    sqlglot_friendly_table_name = (
                        SqlAlchemyProfileEngine._sqlglotfriendly_table_name(
                            request.batch.fq_dataset_name
                        )
                    )
                    select_statement = (
                        Select()
                        .select("COUNT(*) AS row_count")
                        .from_(sqlglot_friendly_table_name)
                    )
                    logger.info(f"Generic SQL statement: {select_statement}")
                    dialect_select_statement = select_statement.sql(
                        dialect=datasource.source.value
                    )
                    logger.info(
                        f"Dialect-specific SQL statement: {dialect_select_statement}"
                    )
                    self.report_issue_query()
                    try:
                        column, value = next(
                            self._execute_select(engine, dialect_select_statement)
                        )
                        response.data[statistic.fq_name] = SuccessStatisticResult(
                            value=value
                        )
                    except Exception as e:
                        self.report_unsuccessful_query(
                            UnsuccessfulStatisticResultType.FAILURE
                        )
                        logger.error(f"Error profiling request: {request}")
                        logger.exception(e)
                        failed_response_for_request = ModelCollections.failed_response_for_request(
                            request,
                            unsuccessful_result_type=UnsuccessfulStatisticResultType.FAILURE,
                            message=str(e),
                            exception=e,
                        )
                        response.update(failed_response_for_request)
                    else:
                        self.report_successful_query()
        else:
            for request in table_level_requests:
                not_matching_expensiveness_response = (
                    ModelCollections.failed_response_for_request(
                        request,
                        UnsuccessfulStatisticResultType.SKIPPED,
                        "Skipped because of expensiveness",
                    )
                )
                response.update(not_matching_expensiveness_response)

    def _process_column_level_requests(
        self,
        column_level_requests: List[ProfileRequest],
        datasource: DataSource,
        engine: Engine,
        response: ProfileResponse,
    ):
        for request in column_level_requests:
            try:
                select_statement, fq_name_mappings = self._generate_select_query(
                    request, response
                )
                if select_statement:
                    logger.info(f"Generic SQL statement: {select_statement}")
                    dialect_select_statement = select_statement.sql(
                        dialect=datasource.source.value
                    )
                    logger.info(
                        f"Dialect-specific SQL statement: {dialect_select_statement}"
                    )
                    self.report_issue_query()
                    for column, value in self._execute_select(
                        engine, dialect_select_statement
                    ):
                        fq_name = fq_name_mappings[column]
                        response.data[fq_name] = SuccessStatisticResult(value=value)
            except Exception as e:
                self.report_unsuccessful_query(UnsuccessfulStatisticResultType.FAILURE)
                logger.error(f"Error profiling request: {request}")
                logger.exception(e)
                failed_response_for_request = ModelCollections.failed_response_for_request(
                    request,
                    unsuccessful_result_type=UnsuccessfulStatisticResultType.FAILURE,
                    message=str(e),
                    exception=e,
                )
                response.update(failed_response_for_request)
            else:
                self.report_successful_query()

    def _generate_select_query(
        self, request: ProfileRequest, response: ProfileResponse
    ) -> Tuple[Optional[Select], Dict[str, str]]:
        """
        Generate a SELECT query based on the profile request.

        If the request contains multiple statistics, the query will be a single SELECT statement.
        If some statistic is not supported, the corresponding UnsuccessfulStatisticResult will be added to the response.

        If no SELECT query is needed (e.g. all unsupported), None is returned.
        """
        fq_name_mappings: Dict[str, str] = {}
        select_statement = Select()
        for statistic in request.statistics:
            fq_name = statistic.fq_name
            sqlfriendly_fq_name = SqlAlchemyProfileEngine._sqlfriendly_column_name(
                fq_name
            )
            fq_name_mappings[sqlfriendly_fq_name] = fq_name

            if isinstance(statistic, TypedStatistic):
                if statistic.type == ProfileStatisticType.COLUMN_DISTINCT_COUNT:
                    column = f"COUNT(DISTINCT {','.join([col for col in statistic.columns])}) AS {sqlfriendly_fq_name}"
                    select_statement = select_statement.select(column, append=True)
                else:
                    logger.warning(f"Unsupported statistic type: {statistic.type}")
                    response.data[fq_name] = UnsuccessfulStatisticResult(
                        type=UnsuccessfulStatisticResultType.UNSUPPORTED,
                        message=f"Unsupported statistic type: {statistic.type}",
                    )
            elif isinstance(statistic, CustomStatistic):
                column = f"{statistic.sql} AS {sqlfriendly_fq_name}"
                select_statement = select_statement.select(column, append=True)
            else:
                logger.warning(f"Unsupported statistic spec: {statistic}")
                response.data[fq_name] = UnsuccessfulStatisticResult(
                    type=UnsuccessfulStatisticResultType.UNSUPPORTED,
                    message=f"Unsupported statistic spec: {statistic}",
                )

        if len(select_statement.expressions) > 0:
            sqlglot_friendly_table_name = (
                SqlAlchemyProfileEngine._sqlglotfriendly_table_name(
                    request.batch.fq_dataset_name
                )
            )
            select_statement = (
                select_statement.from_(
                    f"{sqlglot_friendly_table_name} TABLESAMPLE ({request.batch.sample.size})"
                )
                if request.batch.sample
                else select_statement.from_(sqlglot_friendly_table_name)
            )

            return select_statement, fq_name_mappings

        return None, fq_name_mappings

    def _execute_select(self, engine, select_query) -> Iterator[Tuple[str, Any]]:
        with engine.connect() as conn:
            result = conn.execute(text(select_query))
            # TODO: what if there are multiple rows? raise error?
            row = result.fetchone()
            logger.info(row)
            if row:
                for column, value in zip(row._fields, row._data):
                    column = column.strip("`")
                    yield column, value

    @staticmethod
    def _sqlglotfriendly_table_name(table_name: str) -> str:
        parts = table_name.split(".")
        return ".".join(parts[-2:])

    @staticmethod
    def _sqlfriendly_column_name(column_name: str) -> str:
        # lower because eg snowflake returns column names in uppercase when fetching results
        return column_name.replace(".", "_").replace(" ", "_").replace("-", "_").lower()
