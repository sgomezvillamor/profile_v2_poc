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
    TypedStatistic,
)

def do_profile_sqlalchemy(datasource: DataSource, request: ProfileRequest) -> ProfileResponse:
    response = ProfileResponse(data={}, errors=[])
    engine = create_engine(datasource.connection_string)
    with engine.connect() as conn:
        for statistic in request.statistics:
            if isinstance(statistic, TypedStatistic):
                if statistic.statistic == ProfileStatisticType.DISTINCT_COUNT:
                    result = conn.execute(text(f"SELECT COUNT(DISTINCT {','.join(statistic.columns)}) FROM {request.batch.fully_qualified_dataset_name}"))
                    distinct_count = result.scalar()
                    response.data[statistic.name] = distinct_count
            elif isinstance(statistic, CustomStatistic):
                result = conn.execute(text(f"SELECT {statistic.sql}) FROM {request.batch.fully_qualified_dataset_name}"))
                custom_stat = result.scalar()
                response.data[statistic.name] = custom_stat
            else:
                raise ValueError(f"Unsupported statistic spec: {statistic}")

    return response