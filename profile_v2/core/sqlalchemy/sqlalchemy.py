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

    select_queries = []
    for statistic in request.statistics:
        fq_name = statistic.fq_name
        if isinstance(statistic, TypedStatistic):
            if statistic.statistic == ProfileStatisticType.DISTINCT_COUNT:
                columns = f"COUNT(DISTINCT {','.join([col for col in statistic.columns])})"
                select_query = f"SELECT '{fq_name}' AS fq_name,  {columns} AS value FROM {table_name}"
                select_queries.append(select_query)
        elif isinstance(statistic, CustomStatistic):
            select_query = f"SELECT '{fq_name}' AS fq_name, {statistic.sql} AS value FROM {table_name}"
            select_queries.append(select_query)
        else:
            raise ValueError(f"Unsupported statistic spec: {statistic}")

    if select_queries:
        union_all_query = " UNION ALL ".join(select_queries)
        logger.info(union_all_query)
        with engine.connect() as conn:
            result = conn.execute(text(union_all_query))
            rows = result.fetchall()
            for row in rows:
                logger.info(row)
                fq_name = row[0]
                response.data[fq_name] = row[1]

    return response
