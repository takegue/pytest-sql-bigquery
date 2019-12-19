from dataclasses import dataclass, field

import pytest

from google.cloud import bigquery
from tabulate import tabulate

import sqlchecker.converter as lib
import warnings

CLIENT = bigquery.Client()

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


class SQLReaderForChecking(pytest.File):

    def __init__(self, *args, client=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = client if client else CLIENT

    def collect(self):
        yield from self._collect()

    def _collect(self):
        for name, check_stmt in lib.generate_sql_for_checks_from_file(self.fspath):
            query = str(check_stmt)
            yield SQLTestItem(name, self.client, query, parent=self)


class SQLTestItem(pytest.Item):
    """Pytest Item for Scrapy Spider
    """
    def __init__(self, name, client, query, **kwargs):
        super().__init__(name, **kwargs)
        self.query = query

        try:
            self.info = self.run_query_if_job_meets_limit(client, query)
        except Exception as e:
            self.info = None
            self._error = e


    def run_query_if_job_meets_limit(
        self,
        client,
        query,
        bytes_limit=50 * (10 ** 9)
    ):
        prefix = "retty-sql-test"
        dryrun_job = client.query(
            query,
            job_id_prefix=prefix,
            job_config=bigquery.job.QueryJobConfig(
                dry_run=True,
                use_query_cache=False,
            )
        )
        job = client.query(
            query,
            job_id_prefix=prefix,
            job_config=bigquery.job.QueryJobConfig(
                maximum_bytes_billed=bytes_limit,
            )
        )
        info = JobInfo(
            job=job,
            total_bytes_processed=dryrun_job.total_bytes_processed,
            query=query
        )
        return info


    def runtest(self):
        # When something error is caused
        if self.info is None:
            msg = self._error.message.split(None, 2)[2:][0]

            line, col = 0, 0
            try:
                line, col = map(int, msg.split()[-1].strip('][').split(':'))
            except ValueError:
                pass

            lines = self.query.splitlines()[max(0,line-10):]
            s = max(0, line-10)
            query = '\n'.join(f'{ix}:\t{line}' for ix, line in enumerate(lines, s))

            error = f"in {self.parent.fspath}\n{query}\n\n---\nMessage: {msg}"
            raise Exception(error)

        job = self.info.job
        job_result = job.result()
        report = {}

        elpased_seconds = (job.ended - job.created).total_seconds()
        quota_seconds = 60
        if elpased_seconds > 60:
            warnings.warn(
                f"Detected slow test over {quota_seconds}s"
                f"(path={self.parent.fspath}:{self.name} seconds={elpased_seconds}s)"
            )

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

        if self.info is None:
            return "BigQuery Error", None, f"{self.name}"

        tbp = self.info.total_bytes_processed
        size, unit = bytes_to_human_readable(tbp or 0)
        return f"BigQuery SQL ({size:5.2f} {unit})", None, f"{name}({parent_fspath}) [{size:5.2f} {unit}]"


class SQLExceedScanLimitException(Exception):
    pass

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
