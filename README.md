
# Pytest plugin for Bigquery SQL

pytest-sql-bigquery is pytest-plugin which provides a sql-unitest-framework for BigQuery.
This plugin adopts an end-to-end approch that runnning SQL test on SQL engines.

# Get Started

## Requirements

- Python >= 3.7
- sqlparse
- google-cloud-bigquery (For BigQuery integration)

- BigQuery (Google Cloud Project)


## Install

```
pip install pytest-bigquery-sql
```

Then, set up `confidist.py` for pytest settings.

```python
import pytest

from sqlchecker.integrations.pytest import SQLReaderForChecking

class ChainPytestFile(pytest.File):

    def __init__(self, path, parent, chains, **kwargs):
        super().__init__(path, parent, **kwargs)
        self.chains = chains

    def collect(self):
        for interpreter in self.chains:
            yield from interpreter.collect()

def pytest_collect_file(parent, path):
    if path.ext == ".sql":
        return ChainPytestFile(
            path, parent,
            [
                SQLReaderForChecking(path, parent),
            ])
```

Run test for `examples/sql` directory

```
pytest run -vv examples/sql 
```


## SQL Test Examples

```sql
with dataset as (
    select 1
    union all select 2
    union all select 3
)
, __check_sample as (
    select 'test' as label, count(1) as actual, 2 as expected from dataset 
)

select * from dataset
```

will execute `__check_sample` by sql substitution 
and compare columns between `actual` and `expected`
