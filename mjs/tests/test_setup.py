import os
import time
import json
import pytest
from pathlib import Path


@pytest.mark.first
def test_tests_are_working():
    assert True


@pytest.mark.second
def test_mqtt_broker_started(broker):
    assert(broker is not None)


@pytest.mark.third
def test_mjs_started(mjs):
    db_file = Path(mjs.config.get('dbfile'))
    log_file = Path(mjs.config.get('logfile'))
    assert(db_file.is_file() is True)
    assert(log_file.is_file() is True)


@pytest.mark.fourth
def test_mqtt_messages_are_stored_in_sqlite(mjs, dbc):
    row_value = 'value'
    topic = 'test_setup'
    column_name = 'column'
    msg = {column_name: row_value}
    port = mjs.config.get('port')
    server = mjs.config.get('server')
    os.system("mosquitto_pub -h {0} -p {1} -t {2} -m '{3}'".format(
        server,
        port,
        topic,
        json.dumps(msg)
    ))
    msg_stored_in_db = False

    # loop over select for a few seconds to see if entry in DB
    for i in range(5):
        time.sleep(1)
        dbc.execute('SELECT {0} FROM {1}'.format(
            column_name, topic
        ))
        result = dbc.fetchone()[0]
        if row_value in result:
            msg_stored_in_db = True
            break

    assert(msg_stored_in_db is True)
