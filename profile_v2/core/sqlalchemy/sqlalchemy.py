import logging
from typing import Dict, List

from sqlalchemy import create_engine, text

from profile_v2.core.api import ProfileEngine
from profile_v2.core.model import (CustomStatistic, DataSource, DataSourceType,
                                   ProfileRequest, ProfileResponse,
                                   ProfileStatisticType,
                                   SuccessStatisticResult, TypedStatistic,
                                   UnsuccessfulStatisticResult,
                                   UnsuccessfulStatisticResultType)

logger = logging.getLogger(__name__)


class SqlAlchemyProfileEngine(ProfileEngine):
    """
    Generic profile engine using SQLAlchemy.

    TODO:
    - TABLE_ROW_COUNT statistic
    """

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
        self, datasource: DataSource, requests: List[ProfileRequest]
    ) -> ProfileResponse:
        response = ProfileResponse()

        engine = SqlAlchemyProfileEngine.create_engine(datasource)
        fq_name_mappings: Dict[str, str] = {}
        for request in requests:
            select_columns = []
            for statistic in request.statistics:
                fq_name = statistic.fq_name
                sqlfriendly_fq_name = SqlAlchemyProfileEngine._sqlfriendly_column_name(
                    fq_name
                )
                fq_name_mappings[sqlfriendly_fq_name] = fq_name
                if isinstance(statistic, TypedStatistic):
                    if statistic.type == ProfileStatisticType.COLUMN_DISTINCT_COUNT:
                        column = f"COUNT(DISTINCT {','.join([col for col in statistic.columns])}) AS {sqlfriendly_fq_name}"
                        select_columns.append(column)
                    else:
                        logger.warning(f"Unsupported statistic type: {statistic.type}")
                        response.data[fq_name] = UnsuccessfulStatisticResult(
                            type=UnsuccessfulStatisticResultType.UNSUPPORTED,
                            message=f"Unsupported statistic type: {statistic.type}",
                        )
                elif isinstance(statistic, CustomStatistic):
                    column = f"{statistic.sql} AS {sqlfriendly_fq_name}"
                    select_columns.append(column)
                else:
                    logger.warning(f"Unsupported statistic spec: {statistic}")
                    response.data[fq_name] = UnsuccessfulStatisticResult(
                        type=UnsuccessfulStatisticResultType.UNSUPPORTED,
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
                            fq_name = fq_name_mappings[column]
                            response.data[fq_name] = SuccessStatisticResult(value=value)

        return response

    @staticmethod
    def _sqlfriendly_column_name(column_name: str) -> str:
        # lower because eg snowflake returns column names in uppercase when fetching results
        return column_name.replace(".", "_").replace(" ", "_").replace("-", "_").lower()
