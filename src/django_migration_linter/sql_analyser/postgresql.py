from __future__ import annotations

import re

from .base import BaseAnalyser, Check, CheckMode, CheckType


def has_create_index_in_transaction(sql_statements: list[str], **kwargs) -> bool:
    """Return if a migration opens a transaction, acquires EXCLUSIVE lock, then indexes.

    Any locks that are obtained after a transaction is opened will not be released
    until that transaction either commits or rolls back.

    Accordingly, it's extremely important that a migration avoid any long-running
    queries while exclusive locks are held.

    Building an index is generally a long-running operation. A standard index
    creation (that is, avoiding use of `CONCURRENTLY`) can be safe to run on
    tables which are small in size and/or read-heavy. (`CREATE INDEX` only
    prevents writes to a table; reads are allowed during index creation).

    This check is a stricter version of `CREATE_INDEX` -- if a team wishes
    to build indices nonconcurrently, it's imperative to be mindful of locks.
    """
    if not (sql_statements and sql_statements[0].startswith("BEGIN")):
        return False

    for i, sql in enumerate(sql_statements):
        # If any statements acquire an exclusive lock, complain about index creation
        # later.
        # Nearly every single `ALTER TABLE` command requires an exclusive lock:
        #     https://www.postgresql.org/docs/current/sql-altertable.html
        # (Most common example is `ALTER TABLE... ADD COLUMN`, then later
        # `CREATE INDEX`)
        if sql.startswith("ALTER TABLE"):
            return has_create_index(sql_statements, ignore_concurrently=False)
    return False


def has_create_index(
    sql_statements: list[str], ignore_concurrently: bool = True, **kwargs
) -> bool:
    regex_result = None
    for i, sql in enumerate(sql_statements):
        regex_result = re.search(r"CREATE (UNIQUE )?INDEX.*ON (.*) \(", sql)
        if ignore_concurrently and re.search("INDEX CONCURRENTLY", sql):
            regex_result = None
        if regex_result:
            break
    if not regex_result:
        return False

    preceding_sql_statements = sql_statements[:i]
    concerned_table = regex_result.group(2)
    table_is_added_in_transaction = any(
        sql.startswith(f"CREATE TABLE {concerned_table}")
        for sql in preceding_sql_statements
    )
    return not table_is_added_in_transaction


def has_add_unique_column(sql_statements, **kwargs):
    regex_result = None
    for sql in sql_statements:
        regex_result = re.search(
            r"ALTER TABLE (.*) ADD COLUMN .*UNIQUE CONSTRAINT.*", sql
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


def multiple_table_locks(sql_statements, **kwargs):
    """Returns true if there are more than 1 table locks

    go/created-at-migration-postportem
    """
    tables = set()

    for sql in sql_statements:
        result = re.search(r'ALTER TABLE "([\w]+)".*', sql)

        if result:
            table_name = result.group(1)
            tables.add(table_name)

    return len(tables) > 1


class PostgresqlAnalyser(BaseAnalyser):
    migration_checks: list[Check] = [
        Check(
            code="CREATE_INDEX",
            fn=has_create_index,
            message="CREATE INDEX locks table",
            mode=CheckMode.TRANSACTION,
            type=CheckType.WARNING,
        ),
        Check(
            code="CREATE_INDEX_EXCLUSIVE",
            fn=has_create_index_in_transaction,
            message="CREATE INDEX prolongs transaction, delaying lock release",
            mode=CheckMode.TRANSACTION,
            type=CheckType.WARNING,
        ),
        Check(
            code="DROP_INDEX",
            fn=lambda sql, **kw: re.search("DROP INDEX", sql)
            and not re.search("INDEX CONCURRENTLY", sql),
            message="DROP INDEX locks table",
            mode=CheckMode.ONE_LINER,
            type=CheckType.WARNING,
        ),
        Check(
            code="REINDEX",
            fn=lambda sql, **kw: sql.startswith("REINDEX"),
            message="REINDEX locks table",
            mode=CheckMode.ONE_LINER,
            type=CheckType.WARNING,
        ),
        Check(
            code="ADD_UNIQUE_COLUMN",
            fn=has_add_unique_column,
            message="Adding a column with UNIQUE CONSTRAINT locks table",
            mode=CheckMode.TRANSACTION,
            type=CheckType.ERROR,
        ),
        Check(
            code="MULTIPLE_TABLE_LOCKS",
            fn=multiple_table_locks,
            message=(
                "Do not lock more than one table at a time in the same "
                "transaction to minimize the chance of deadlocks and "
                "production issues. go/created-at-migration-postportem"
            ),
            mode=CheckMode.TRANSACTION,
            type=CheckType.ERROR,
        ),
    ]
