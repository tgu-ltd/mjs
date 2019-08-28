import os
import time
import json


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


def test_table_adjusts_to_new_columns(mjs, dbc):
    col = 'column'
    row_one_value = 1
    row_two_value = 'z'
    row_three_value = 3
    first_message_stored = False
    second_message_stored = False
    third_message_stored = False
    topic = 'test_setup_adjustable'

    publish(mjs, {col: row_one_value}, topic)
    sql = 'SELECT {0} FROM {1}'.format(col, topic)
    first_message_stored = row_one_value in run_sql(dbc, sql)

    publish(mjs, {col: row_two_value}, topic)
    sql = 'SELECT {0} FROM {1}'.format(col, topic)
    second_message_stored = row_two_value in run_sql(dbc, sql)

    publish(mjs, {col: row_three_value}, topic)
    sql = 'SELECT {0} FROM {1}'.format(col, topic)
    third_message_stored = row_three_value in run_sql(dbc, sql)

    assert(first_message_stored is True)
    assert(second_message_stored is True)
    assert(third_message_stored is True)
