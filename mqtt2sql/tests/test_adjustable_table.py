import os
import time
import json


def publish(m2s, msg, topic):
    server = m2s.config.get('server')
    port = m2s.config.get('port')
    os.system("mosquitto_pub -h {0} -p {1} -t {2} -m '{3}'".format(
        server,
        port,
        topic,
        json.dumps(msg)
    ))


def select_col_val(dbc, col, val, table):
    found = False
    sqlval = val
    if type(val) == str:
        sqlval = "'{0}'".format(val)
    for i in range(5):
        time.sleep(1)
        dbc.execute('SELECT {0} FROM {1} WHERE {0} = {2}'.format(
            col, table, col, sqlval
        ))
        result = dbc.fetchone()[0]
        if val == result:
            found = True
            break
    return found


def test_table_adjusts_to_new_columns(m2s, dbc):
    col_one = 'col_1'
    col_two = 'col_2'
    row_one_value = 1
    row_two_value = '2'
    first_message_stored = False
    second_message_stored = False
    topic = 'test_setup_adjustable'

    publish(m2s, {col_one: row_one_value}, topic)
    first_message_stored = select_col_val(dbc, col_one, row_one_value, topic)

    publish(m2s, {col_two: row_two_value}, topic)
    second_message_stored = select_col_val(dbc, col_two, row_two_value, topic)

    time.sleep(1)
    assert(first_message_stored is True)
    assert(second_message_stored is True)
