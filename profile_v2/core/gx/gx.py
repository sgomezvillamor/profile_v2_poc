import great_expectations as gx

from profile_v2.core.model import (
    DataSource,
    ProfileRequest,
    ProfileResponse,
)

def do_profile_gx(datasource: DataSource, request: ProfileRequest) -> ProfileResponse:
    context = gx.get_context()
    data_source = context.data_sources.add_snowflake(
        name=datasource.name,
        connection_string=datasource.connection_string,
    )
    data_asset = data_source.get_asset(request.batch.fully_qualified_dataset_name)

    # TODO: implement batch spec if any, full table for the moment
    batch_definition = data_asset.add_batch_definition_whole_table(
        name="FULL_TABLE"
    )
    batch_definition.head()  # validate the batch definition

    return ProfileResponse()


