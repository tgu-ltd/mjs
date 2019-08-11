from mqtt2sql.mqtt import Mqtt


def start():
    m2s = Mqtt()
    try:
        m2s.connect()
        m2s.start()
    except (KeyboardInterrupt, SystemExit):
        m2s.stop()
        m2s.join()
