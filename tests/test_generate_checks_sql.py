import pytest
import sqlparse as sp

import pytest_sql_bigquery.converter as lib

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
def mockable_stmt():
    sql = """
    WITH
    __mock___project___dataset___table as (
        select "test" as label, 1 as actual, 1 as expected from test
    ),
    test as (
        select 1 as label
        from `project.dataset.table`
        group by label
        order by 1 desc
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
def no_stmt():
    sql = """
    """
    return sp.parse(sql)[0]

@pytest.fixture
def sample_query():
    sql = open("../../dist/bigquery/mindful-rhythm-113423/DM_raiten/v_raiten_index_monthly/query.sql").read()
    return sp.parse(sql)[0]

def test_has_test_item(testable_stmt):
    assert lib.has_test_item(testable_stmt) is True

def test_iter_test_items_for_no_sql_file(no_stmt):
    items = list(lib.iter_test_items(no_stmt))
    assert len(items) == 0

def test_generate_sql_for_checks_for_no_sql_file(no_stmt):
    print(lib.__file__)
    items = list(lib.generate_sql_for_checks(no_stmt))
    assert len(items) == 0

def test_has_test_item_for_non_testable(non_testable_stmt):
    assert lib.has_test_item(non_testable_stmt) is False


def test_iter_test_items(testable_stmt):
    items = list(lib.iter_test_items(testable_stmt))
    assert len(items) == 1


def test_generate_mocked_sql(mockable_stmt):
    actual = lib.mocking_tables(mockable_stmt)
    expected_stmt = """
    WITH
    __mock___project___dataset___table as (
        select "test" as label, 1 as actual, 1 as expected from test
    ),
    test as (
        select 1 as label
        from __mock___project___dataset___table
        group by label
        order by 1 desc
    )
    SELECT * FROM report
    """.strip()
    assert str(actual) == expected_stmt


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
      label, count(1) as errors
    from __check
    where actual != expected
    group by label
    limit 100
    """.strip()
    assert str(actual_stmt) == expected_stmt

def test_replace_table_with(testable_stmt):
    actual_stmt = lib.replace_table_with(
        testable_stmt,
        "project.dataset.table",
        "fixture_table",
    )

    expected_stmt = """
    WITH test as (
        select 1 as label
        from fixture_table
        group by label
        order by 1 desc
    )
    , __check as (
        select "test" as label, count(1) as actual, 1 as expected from test
    )
    SELECT * FROM report
    """.strip()
    assert str(actual_stmt) == expected_stmt


if __name__ == '__main__':
    pytest.main()
