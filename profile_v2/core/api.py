from profile_v2.core.model import (
    DataSource,
    ProfileRequest,
    ProfileResponse,
)
from profile_v2.core.gx.gx import do_profile_gx

def do_profile(datasource: DataSource, request: ProfileRequest) -> ProfileResponse:
    # TODO: some logic to choose one engine or another or mixed?
    return do_profile_gx(datasource=datasource, request=request)

