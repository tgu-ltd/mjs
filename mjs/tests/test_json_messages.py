import os
import time
import json
import sqlite3


def publish(mjs, msg, topic):
    server = mjs.config.get('server')
    port = mjs.config.get('port')
    os.system("mosquitto_pub -h {0} -p {1} -t {2} -m '{3}'".format(
        server,
        port,
        topic,
        json.dumps(msg)
    ))


def run_sql(dbc, sql):
    results = []
    time.sleep(2)  # Wait for db to get the message
    dbc.execute(sql)
    rows = dbc.fetchall()
    for result in rows:
        results.append(result[0])
    return results


def test_non_json_messages_are_not_saved(mjs, dbc):
    value = 1
    col = 'no_json'
    sql_error = False
    topic = 'test_non_json'
    publish(mjs, '{0}:{1}'.format(col, value), topic)
    sql = 'SELECT {0} FROM {1}'.format(col, topic)
    try:
        run_sql(dbc, sql)
    except sqlite3.OperationalError as e:
        if 'no such table: test_non_json' in str(e):
            sql_error = True
    assert(sql_error is True)
