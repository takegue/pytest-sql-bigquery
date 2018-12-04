import pytest
import sqlparse as sp

import sqlchecker as lib

@pytest.fixture
def testable_stmt():
    sql = """
    WITH test as (
        select 1 as label
        from `project.dataset.table`
        group by label
        order by 1 desc
    )
    , __check as (
        select "test" as label, count(1) as actual, 1 as expected from test
    )
    SELECT * FROM report
    """.strip()
    return sp.parse(sql)[0]

@pytest.fixture
def testable_stmt_with_mock():
    sql = """
    WITH
    __mock__source as (
        select v as label from unnest(range(10)) as v
    )
    , __mock__project__dataset__table as (
        select v as label from unnest(range(10)) as v
    )
    , source as (
        select * from `project.dataset.table`
    )
    , test as (
        select 1 as label
        from source
        group by label
        order by 1 desc
    )
    , test2 as (
        select 1 as source
        from source
        join `project.dataset.table` as hoge using(label)
        group by label
        order by 1 desc
    )
    , __check as (
        select "test" as label, count(1) as actual, 1 as expected from test
    )
    SELECT * FROM report
    """.strip()
    return sp.parse(sql)[0]

@pytest.fixture
def non_testable_stmt():
    sql = """
    WITH test as (
        select 1
    )

    SELECT * FROM report
    """
    return sp.parse(sql)[0]

@pytest.fixture
def sample_query():
    sql = open("../../dist/bigquery/mindful-rhythm-113423/DM_raiten/v_raiten_index_monthly/query.sql").read()
    return sp.parse(sql)[0]

def test_iter_test_items(testable_stmt):
    items = list(lib.iter_prefixed_items(testable_stmt, '__check'))
    assert len(items) == 1

def test_generate_sql_for_checks(testable_stmt):
    actual = list(lib.generate_sql_for_checks(testable_stmt))

    assert len(actual) == 1

    actual_name, actual_stmt = actual[0]
    expected_stmt = """
    WITH test as (
        select 1 as label
        from `project.dataset.table`
        group by label
        order by 1 desc
    )
    , __check as (
        select "test" as label, count(1) as actual, 1 as expected from test
    )
    select
      label, countif(actual != expected) as errors
    from __check
    group by label
    """.strip()
    assert str(actual_stmt) == expected_stmt


def test_generate_sql_with_mock_for_checks(testable_stmt_with_mock):
    breakpoint
    actual = list(lib.generate_sql_for_checks(testable_stmt_with_mock))

    assert len(actual) == 1

    actual_name, actual_stmt = actual[0]
    expected_stmt = """
    WITH
    __mock__source as (
        select v as label from unnest(range(10)) as v
    )
    , __mock__project__dataset__table as (
        select v as label from unnest(range(10)) as v
    )
    , source as (
        select * from __mock__project__dataset__table
    )
    , test as (
        select 1 as label
        from __mock__source
        group by label
        order by 1 desc
    )
    , test2 as (
        select 1 as source
        from __mock__source
        join __mock__project__dataset__table as hoge using(label)
        group by label
        order by 1 desc
    )
    , __check as (
        select "test" as label, count(1) as actual, 1 as expected from test
    )
    select
      label, countif(actual != expected) as errors
    from __check
    group by label
    """.strip()
    assert str(actual_stmt) == expected_stmt


def test_replace_table_with(testable_stmt):
    actual_stmt = lib.replace_table_with(
        testable_stmt,
        "project.dataset.table",
        "fixture_table",
    )
    actual_stmt = lib.replace_table_with(
        actual_stmt,
        "test",
        "fuga",
    )

    expected_stmt = """
    WITH test as (
        select 1 as label
        from fixture_table
        group by label
        order by 1 desc
    )
    , __check as (
        select "test" as label, count(1) as actual, 1 as expected from fuga
    )
    SELECT * FROM report
    """.strip()
    assert str(actual_stmt) == expected_stmt


if __name__ == '__main__':
    pytest.main()
