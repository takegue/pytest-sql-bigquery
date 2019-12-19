
import sqlparse as sp
from sqlparse.tokens import Token as T

def iter_identifier(token_list):
    for token in token_list.tokens:
        if isinstance(token, sp.sql.Identifier):
            yield token
        elif isinstance(token, sp.sql.IdentifierList):
            yield from iter_identifier(token)

def iter_test_items(stmt):
    yield from (
        identifier
        for identifier in iter_identifier(stmt)
        if identifier.get_name() and identifier.get_name().startswith("__check")
            and not identifier.is_child_of(stmt)
    )

def has_test_item(stmt):
    return sum( 1 for _ in list(iter_test_items(stmt))) > 0

def generate_sql_for_checks(stmt):
    stmt = mocking_tables(stmt)

    root_dml = stmt.token_matching(
        lambda t: t.ttype == T.Keyword.DML,
        0
    )
    if root_dml is None:
        return

    idx_root_dml = stmt.token_index(root_dml)

    template = """
    select
      label, count(1) as errors
    from {table_name}
    where actual != expected
    group by label
    limit 100
    """.strip()

    # FIXME: sqlparse doens't implement tree objects copying
    for test_item in list(iter_test_items(stmt)):
        table_name = test_item.get_name().strip()
        sql = template.format(table_name=table_name)
        repl_stmt = sp.parse(sql)[0]
        new_stmt = sp.sql.Statement(stmt.tokens[:])
        new_stmt.tokens[idx_root_dml:] = repl_stmt.tokens[:]
        yield table_name.lstrip("_"), new_stmt


def generate_sql_for_checks_from_file(fpath):
    with open(fpath) as fp:
        ret = sp.parse(fp.read())
        if not ret:
            return

        stmt = ret[0]
        yield from generate_sql_for_checks(stmt)


def mocking_tables(stmt):
    stmt = clone(stmt)
    mocks = [ t for t in stmt.flatten() if t.value.startswith("__mock") ]
    sep = "___"

    for mock in mocks:
        target = mock.value.replace("mock", "").strip("_").replace(sep, ".")
        stmt = replace_table_with(stmt, target, mock.value)

    return stmt


def clone(stmt):
    return sp.parse(str(stmt))[0]

def replace_table_with(stmt, target, replacement):
    # clone objects
    stmt = sp.parse(str(stmt))[0]
    tokens = (
        token for token in stmt.flatten()
        if token.ttype == T.Name
        and token.value.strip("`") == target
    )
    for token in tokens:
        token.value = replacement

    return stmt

if __name__ == '__main__':
    pass
