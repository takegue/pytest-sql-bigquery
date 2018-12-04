from dataclasses import dataclass, field

import pytest

from google.cloud import bigquery
from tabulate import tabulate

import sqlchecker as lib

def bytes_to_human_readable(size) -> (float, str):
    power = 2 ** 10
    n = 0
    readable_units = {
        0: "Bytes",
        1: "KB",
        2: "MB",
        3: "GB",
        4: "TB",
    }
    while size >= power:
        size /= power
        n += 1

    return float(size), readable_units[n]


@dataclass
class JobInfo:
    job: bigquery.QueryJob
    query: str
    total_bytes_processed: int

@dataclass(frozen=True)
class TestResult:
    label: str = field(repr=False, default="unknown")
    status: str = field(repr=False, default="unknown")
    errors: int = field(repr=False, default=-1)


def pytest_collect_file(parent, path):
    if path.ext == ".sql":
        return SQLReaderForChecking(path, parent)

class SQLReaderForChecking(pytest.File):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from google.cloud import bigquery
        self.client = bigquery.Client()

    def collect(self):
        yield from self._collect()

    def _collect(self):
        for name, check_stmt in lib.generate_sql_for_checks_from_file(self.fspath):
            # query = str(check_stmt)
            # dryrun_job = self.client.query(
            #     query,
            #     job_config=bigquery.job.QueryJobConfig(
            #         dry_run=True,
            #         use_query_cache=False,
            #     )
            # )
            # job = self.client.query(query, job_id_prefix="retty-sql-test")
            info = JobInfo(
                job=None,
                # total_bytes_processed=dryrun_job.total_bytes_processed,
                total_bytes_processed=5,
                query='hoge'
            )
            yield SQLTestItem(name, info, parent=self)


class SQLTestItem(pytest.Item):
    """Pytest Item for Scrapy Spider
    """
    def __init__(self, name, info, **kwargs):
        super().__init__(name, **kwargs)
        self.info = info

    def runtest(self, *args, **kwargs):
        return

        job_result = self.info.job.result()
        report = {}
        for row in job_result:
            part = dict(row)
            part["status"] = "PASSED" if part["errors"] == 0 else "FAILED"
            result = TestResult(**part)
            report[result.label] = result

        for label, result in report.items():
            if result.status == "FAILED":
                raise SQLTestItemException(self.name, result, report)

    def repr_failure(self, excinfo):
        """ called when self.runtest() raises an exception. """
        excinfo.traceback = excinfo.traceback[-1:]
        return self._repr_failure_py(excinfo, style="short")

    def reportinfo(self):
        name = self.name
        parent_fspath = self.parent.fspath
        tbp = self.info.total_bytes_processed
        size, unit = bytes_to_human_readable(tbp or 0)
        return f"BigQuery SQL ({size:5.2f} {unit})", None, f"{name}({parent_fspath}) [{size:5.2f} {unit}]"


class SQLTestItemException(Exception):

    def __init__(self, name, result: TestResult, overall, *args, **kwargs):
        self.label = name
        self.result = result
        self.overall = overall
        super().__init__(*args, **kwargs)

    def __str__(self):
        records = [
            (r.label if r.status == "PASSED" else f'*{r.label}', r.status, r.errors)
            for name, r in self.overall.items()
        ]
        table = tabulate(records, headers=["label", "status", "error"])
        return (
            f"some {self.label}'s results is wrong\n"
            f'{table}'
        )
