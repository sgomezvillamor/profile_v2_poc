import logging
from sqlalchemy import (
    create_engine,
    text,
)
from profile_v2.core.model import (
    CustomStatistic,
    DataSource,
    ProfileRequest,
    ProfileResponse,
    ProfileStatisticType,
    StatisticFQName,
    TypedStatistic,
)

logger = logging.getLogger(__name__)

def do_profile_sqlalchemy(datasource: DataSource, request: ProfileRequest) -> ProfileResponse:
    response = ProfileResponse(data={}, errors=[])
    engine = create_engine(datasource.connection_string)

    table_name = request.batch.fully_qualified_dataset_name

    select_columns = []
    for statistic in request.statistics:
        fq_name = statistic.fq_name
        if isinstance(statistic, TypedStatistic):
            if statistic.statistic == ProfileStatisticType.DISTINCT_COUNT:
                column = f"COUNT(DISTINCT {','.join([col for col in statistic.columns])}) AS `{fq_name}`"
                select_columns.append(column)
        elif isinstance(statistic, CustomStatistic):
            column = f"{statistic.sql} AS `{fq_name}`"
            select_columns.append(column)
        else:
            raise ValueError(f"Unsupported statistic spec: {statistic}")

    if select_columns:
        select_query = f"SELECT {', '.join(select_columns)} FROM {request.batch.fully_qualified_dataset_name}"
        logger.info(select_query)
        with engine.connect() as conn:
            result = conn.execute(text(select_query))
            row = result.fetchone()
            logger.info(row)
            if row:
                for column, value in zip(row._fields, row._data):
                    column = column.strip('`')
                    response.data[column] = value

    return response
