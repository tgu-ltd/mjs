from mjs.mqtt import Mqtt
from mjs.jfts import Jfts


def start():
    # Example CLI arguments ...
    # mjs
    #   server 10.10.20.1
    #   port 1823
    #   topics "#"
    #   dbfile "./solar_gps.db"
    #   loglevel debug
    #   logtocon true
    mjs = Mqtt()
    try:
        mjs.connect()
    except (KeyboardInterrupt, SystemExit):
        mjs.disconnect()


def filestosql():
    # Example CLI arguments ...
    # jfts
    #   filespath "../data/halfords_115_amph_battery_data"
    #   filenames victron_data_averaged_
    #   use_ts_field time
    #   add_to_row battery_name:halfords_115_amph
    #   tablename solar
    #   loglevel debug
    #   logtocon true
    #   dbfile solar.db
    jfts = Jfts()
    jfts.load()
    jfts.save()
