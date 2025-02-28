import logging
import threading
import time
from datetime import datetime
from typing import List, Optional

from profile_v2.core.api import ProfileEngine
from profile_v2.core.model import (DataSource, ProfileRequest, ProfileResponse,
                                   SuccessStatisticResult)

logger = logging.getLogger(__name__)


class FixedResponseEngine(ProfileEngine):
    def __init__(self, response: ProfileResponse):
        self.response = response

    def _do_profile(
        self, datasource: DataSource, requests: List[ProfileRequest]
    ) -> ProfileResponse:
        return self.response


class SuccessResponseEngine(ProfileEngine):
    def __init__(
        self, success_value: int = 0, elapsed_time_millis: Optional[int] = None
    ):
        self.success_value = success_value
        self.elapsed_time_millis = elapsed_time_millis

    def _do_profile(
        self, datasource: DataSource, requests: List[ProfileRequest]
    ) -> ProfileResponse:
        response = ProfileResponse()
        if self.elapsed_time_millis:
            logger.info(
                f"[{threading.get_ident()} {datetime.now()}] Sleeping for {self.elapsed_time_millis / 1000} seconds..."
            )
            time.sleep(self.elapsed_time_millis / 1000)

        for request in requests:
            for statistic in request.statistics:
                response.data[statistic.fq_name] = SuccessStatisticResult(
                    value=self.success_value
                )
        logger.info(f"[{threading.get_ident()} {datetime.now()}] Done!")
        return response
