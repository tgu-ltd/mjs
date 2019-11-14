from mjs.mqtt import Mqtt


def start():
    mjs = Mqtt()
    try:
        mjs.connect()
    except (KeyboardInterrupt, SystemExit):
        mjs.disconnect()
