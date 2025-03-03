import unittest
from threading import Thread

from profile_v2.core.model import UnsuccessfulStatisticResultType
from profile_v2.core.report import ProfileCoreReport


class TestProfileCoreReport(unittest.TestCase):

    def test_issue_query_increments_count(self):
        report = ProfileCoreReport()
        report.issue_query("engine1")
        report.issue_query("engine1")
        report.issue_query("engine2")

        assert report.num_issued_queries_by_engine["engine1"] == 2
        assert report.num_issued_queries_by_engine["engine2"] == 1
        assert report.num_issued_queries_by_engine["engine0"] == 0

    def test_successful_query_increments_count(self):
        report = ProfileCoreReport()
        report.successful_query("engine1")
        report.successful_query("engine1")
        report.successful_query("engine2")

        assert report.num_successful_queries_by_engine["engine1"] == 2
        assert report.num_successful_queries_by_engine["engine2"] == 1
        assert report.num_successful_queries_by_engine["engine0"] == 0

    def test_unsuccessful_query_increments_count(self):
        report = ProfileCoreReport()
        status = UnsuccessfulStatisticResultType.FAILURE
        report.unsuccessful_query("engine1", status)
        report.unsuccessful_query("engine1", status)
        report.unsuccessful_query("engine2", status)

        assert (
            report.num_unsuccessful_queries_by_engine_and_status[("engine1", status)]
            == 2
        )
        assert (
            report.num_unsuccessful_queries_by_engine_and_status[("engine2", status)]
            == 1
        )
        assert (
            report.num_unsuccessful_queries_by_engine_and_status[
                ("engine1", UnsuccessfulStatisticResultType.UNSUPPORTED)
            ]
            == 0
        )

    def test_concurrent_queries_increment_counts_correctly(self):
        report = ProfileCoreReport()
        status = UnsuccessfulStatisticResultType.SKIPPED

        def workload():
            for i in range(1000):
                report.issue_query("engine1")
                if i % 10 == 0:
                    report.unsuccessful_query("engine1", status)
                else:
                    report.successful_query("engine1")

        threads = [Thread(target=workload) for _ in range(10)]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        assert report.num_issued_queries_by_engine["engine1"], 10000
        assert report.num_successful_queries_by_engine["engine1"] == 9000
        assert (
            report.num_unsuccessful_queries_by_engine_and_status[("engine1", status)]
            == 1000
        )

    def test_string_representation(self):
        report = ProfileCoreReport()
        report.issue_query("engine1")
        report.successful_query("engine1")
        report.issue_query("engine1")
        report.successful_query("engine1")
        report.issue_query("engine2")
        report.unsuccessful_query("engine2", UnsuccessfulStatisticResultType.FAILURE)

        assert (
            repr(report)
            == "ProfileCoreReport(num_issued_queries_by_engine={'engine1': 2, 'engine2': 1}, "
            "num_successful_queries_by_engine={'engine1': 2}, "
            "num_unsuccessful_queries_by_engine_and_status={('engine2', 'failure'): 1})"
        )
