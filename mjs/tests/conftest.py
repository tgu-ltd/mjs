import os
import pytest
import sqlite3
from mjs.mqtt import Mqtt


@pytest.fixture(scope="session")
def broker():
    result = os.system("mosquitto &")
    yield result
    # This could hurt if the user is running more than broker
    os.system("pkill -9 -f 'mosquitto'")


@pytest.fixture(scope="session")
def mjs():
    m = Mqtt(
        logfile='./testbase.log',
        dbfile='./testbase.db'
    )
    m.connect()
    m.start()
    yield m
    m.stop()
    m.join()
    os.remove(m.config.get('dbfile'))
    os.remove(m.config.get('logfile'))


@pytest.fixture(scope="session")
def dbc(mjs):
    dbfile = mjs.config.get('dbfile')
    conn = sqlite3.connect(dbfile)
    c = conn.cursor()
    yield c
    conn.commit()
    conn.close()
