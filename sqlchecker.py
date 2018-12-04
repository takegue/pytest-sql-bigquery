
import sqlparse as sp
from sqlparse.tokens import Token as T

def iter_identifier(token_list):
    for token in token_list.tokens:
        if isinstance(token, sp.sql.Identifier):
            yield token
        elif isinstance(token, sp.sql.IdentifierList):
            yield from iter_identifier(token)


def iter_prefixed_items(stmt, prefix):
    yield from (
        identifier
        for identifier in iter_identifier(stmt)
        if identifier.get_name() and identifier.get_name().startswith(prefix)
            and not identifier.is_child_of(stmt)
    )


def generate_sql_for_checks(
    stmt,
    prefix_testcase='__check',
    prefix_mock='__mock__'
):
    for identifier in iter_prefixed_items(stmt, prefix_mock):
        table_name = identifier.get_name().strip()
        target = table_name.replace(prefix_mock, '').strip('_').replace("__", ".")
        stmt = replace_table_with(stmt, target, table_name)

    root_dml = stmt.token_matching(
        lambda t: t.ttype == T.Keyword.DML,
        0
    )
    idx_root_dml = stmt.token_index(root_dml)
    template = """
    select
      label, countif(actual != expected) as errors
    from {table_name}
    group by label
    """.strip()

    # FIXME: sqlparse doens't implement tree objects copying
    for test_item in list(iter_prefixed_items(stmt, prefix_testcase)):
        table_name = test_item.get_name().strip()
        sql = template.format(table_name=table_name)

        repl_stmt = sp.parse(sql)[0]
        new_stmt = sp.sql.Statement(stmt.tokens[:])
        new_stmt.tokens[idx_root_dml:] = repl_stmt.tokens[:]
        yield table_name.lstrip("_"), new_stmt


def generate_sql_for_checks_from_file(fpath):
    with open(fpath) as fp:
        ret = sp.parse(fp.read())
        stmt = ret[0]
        yield from generate_sql_for_checks(stmt)


def replace_table_with(stmt, target, replacement):
    # clone objects
    stmt = sp.parse(str(stmt))[0]

    def pred(token):
        if not token.value.strip("`") == target:
            return False

        if isinstance(token.parent, sp.sql.IdentifierList):
            return False

        now = token
        prev_token = None
        while prev_token is None and now.parent is not None:
            parent = now.parent
            idx = parent.token_index(now)
            prev_token = parent.token_prev(idx)[1]
            now = parent

        now = token
        next_token = None
        while next_token is None and now.parent is not None:
            parent = now.parent
            idx = parent.token_index(now)
            next_token = parent.token_next(idx)[1]
            now = parent

        if prev_token.value not in ('from', 'join'):
            return False

        return True

    tokens = (token for token in stmt.flatten() if pred(token))

    for token in tokens:
        token.value = replacement

    return stmt


if __name__ == '__main__':
    pass
