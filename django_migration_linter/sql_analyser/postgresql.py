import re

from .base import BaseAnalyser


def has_create_index(sql_statements, **kwargs):
    regex_result = None
    for sql in sql_statements:
        regex_result = re.search(r"CREATE (UNIQUE )?INDEX.*ON (.*) \(", sql)
        if re.search("INDEX CONCURRENTLY", sql):
            regex_result = None
        elif regex_result:
            break
    if not regex_result:
        return False

    concerned_table = regex_result.group(2)
    table_is_added_in_transaction = any(
        sql.startswith("CREATE TABLE {}".format(concerned_table))
        for sql in sql_statements
    )
    return not table_is_added_in_transaction


def has_add_unique_column(sql_statements, **kwargs):
    regex_result = None
    for sql in sql_statements:
        regex_result = re.search(
            "ALTER TABLE (.*) ADD COLUMN .*UNIQUE CONSTRAINT.*",
            sql
        )
        if regex_result:
            break
    if not regex_result:
        return False

    concerned_table = regex_result.group(1)
    table_is_added_in_transaction = any(
        sql.startswith("CREATE TABLE {}".format(concerned_table))
        for sql in sql_statements
    )
    return not table_is_added_in_transaction


class PostgresqlAnalyser(BaseAnalyser):
    migration_tests = [
        {
            "code": "CREATE_INDEX",
            "fn": has_create_index,
            "msg": "CREATE INDEX locks table",
            "mode": "transaction",
            "type": "warning",
        },
        {
            "code": "DROP_INDEX",
            "fn": lambda sql, **kw: re.search("DROP INDEX", sql)
            and not re.search("INDEX CONCURRENTLY", sql),
            "msg": "DROP INDEX locks table",
            "mode": "one_liner",
            "type": "warning",
        },
        {
            "code": "REINDEX",
            "fn": lambda sql, **kw: sql.startswith("REINDEX"),
            "msg": "REINDEX locks table",
            "mode": "one_liner",
            "type": "warning",
        },
        {
            "code": "ADD_UNIQUE_COLUMN",
            "fn": has_add_unique_column,
            "msg": "Adding a column with UNIQUE CONSTRAINT locks table",
            "mode": "transaction",
            "type": "error",
        },
    ]
