from mjs.mqtt import Mqtt


def start():
    mjs = Mqtt()
    try:
        mjs.connect()
        mjs.start()
    except (KeyboardInterrupt, SystemExit):
        mjs.stop()
        mjs.join()
