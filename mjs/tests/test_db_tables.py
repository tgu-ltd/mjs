import os
import sqlite3
from mjs.dbtables import DbTables


def get_to_db():
    return 'mjs/tests/test_data/test_solar.db'


def get_from_table():
    return 'solar'


def test_extract_solar_table():
    ''' Test extracting a table from a db and adding it to another db '''

    # Get the row from the old db we are extracting the table from
    from_db = 'mjs/tests/test_data/only_solar.db'
    con = sqlite3.connect(from_db)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("SELECT * FROM {0}".format(get_from_table()))
    old_db_row = dict(cur.fetchone())
    cur.close()
    con.close()

    # Extract the table and test that the to be created table is deleted
    dbt = DbTables(
        from_table=get_from_table(),
        to_db=get_to_db(),
        from_db=from_db,
        rm_to_db=True,
    )
    assert(os.path.exists(get_to_db()) is False)
    dbt.extract()

    # Get the row from the new db and table
    con = sqlite3.connect(get_to_db())
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("SELECT * FROM {0}".format(get_from_table()))
    new_db_row = dict(cur.fetchone())
    cur.close()
    con.close()
    print(new_db_row.get('_ts'))
    print(old_db_row.get('_ts'))
    assert(new_db_row.get('_ts') == old_db_row.get('_ts'))


def test_append_solar_table():
    # Get the row from the old db we are extracting the table from
    rows = []
    from_db = 'mjs/tests/test_data/gps_and_solar.db'
    con = sqlite3.connect(from_db)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("SELECT * FROM {0}".format(get_from_table()))
    old_db_row = dict(cur.fetchone())
    cur.close()
    con.close()

    # Append data from old db and table to the other db and table
    dbt = DbTables(
        from_table=get_from_table(),
        to_db=get_to_db(),
        from_db=from_db,
    )
    dbt.append()

    # Get the row from the new db and table
    con = sqlite3.connect(get_to_db())
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("SELECT * FROM {0} order by _ts desc limit 1".format(get_from_table()))
    new_db_row = dict(cur.fetchone())
    cur.execute("SELECT * FROM {0}".format(get_from_table()))
    for row in cur.fetchall():
        rows.append(dict(row))
    cur.close()
    con.close()
    assert(new_db_row.get('_ts') == old_db_row.get('_ts'))
    assert(len(rows) > 1)
