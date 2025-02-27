import logging
from typing import List

from sqlalchemy import create_engine, text

from profile_v2.core.api import ProfileEngine
from profile_v2.core.model import (CustomStatistic, DataSource,
                                   FailureStatisticResult,
                                   FailureStatisticResultType, ProfileRequest,
                                   ProfileResponse, ProfileStatisticType,
                                   StatisticSpec, SuccessStatisticResult,
                                   TypedStatistic)

logger = logging.getLogger(__name__)


class SqlAlchemyProfileEngine(ProfileEngine):
    """
    Generic profile engine using SQLAlchemy.

    Restrictions:
    - all requests must be for the same batch
    """

    def do_profile(
        self, datasource: DataSource, requests: List[ProfileRequest]
    ) -> ProfileResponse:
        response = ProfileResponse()
        engine = create_engine(datasource.connection_string)

        for request in requests:
            select_columns = []
            for statistic in request.statistics:
                fq_name = statistic.fq_name
                if isinstance(statistic, TypedStatistic):
                    if statistic.type == ProfileStatisticType.COLUMN_DISTINCT_COUNT:
                        column = f"COUNT(DISTINCT {','.join([col for col in statistic.columns])}) AS `{fq_name}`"
                        select_columns.append(column)
                elif isinstance(statistic, CustomStatistic):
                    column = f"{statistic.sql} AS `{fq_name}`"
                    select_columns.append(column)
                else:
                    logger.warning(f"Unsupported statistic spec: {statistic}")
                    response.data[fq_name] = FailureStatisticResult(
                        type=FailureStatisticResultType.UNSUPPORTED,
                        message=f"Unsupported statistic spec: {statistic}",
                    )

            if select_columns:
                from_statement = f"FROM {request.batch.fq_dataset_name}"
                if request.batch.sample:
                    from_statement += f" TABLESAMPLE ({request.batch.sample.size} ROWS)"
                select_query = f"SELECT {', '.join(select_columns)} {from_statement}"
                logger.info(text(select_query))
                with engine.connect() as conn:
                    result = conn.execute(text(select_query))
                    row = result.fetchone()
                    logger.info(row)
                    if row:
                        for column, value in zip(row._fields, row._data):
                            column = column.strip("`")
                            fq_name = SqlAlchemyProfileEngine._find_fq_name_to_preserve_casing(
                                request.statistics, column
                            )
                            response.data[fq_name] = SuccessStatisticResult(value=value)

        return response

    @staticmethod
    def _find_fq_name_to_preserve_casing(
        statistic_specs: List[StatisticSpec], column_name: str
    ) -> str:
        # snowflake or sqlalchemy uppercase column names when fetching results
        for statistic_spec in statistic_specs:
            if statistic_spec.fq_name.casefold() == column_name.casefold():
                return statistic_spec.fq_name
        return column_name
