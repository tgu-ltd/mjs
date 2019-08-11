# mjs
A python module that stores mqtt messages into sqlite3 tables

## Intended Audience
Hobbyists who wants to quickly store mqtt message into a sqlite3 database without having to muck about in hyperspace  

## Installing
~$ git clone git@github.com:tgu-ltd/mjs.git
~$ cd mjs
~$ pip setup.py install --user 
~$ mjs help

## Using
~$ mjs help
~$ mjs server 192.168.5.1 topics "#"
~$ mjs server 192.168.5.1 loglevel debug logtocon true topics "#"

## Storing and retrieving data
```bash
~$ mjs server 192.168.5.1 port 1883 topics "gps,solar" dbfile "./solar_gps.db" &
~$ sqlite ./solar_gps.db
sqlite> select _ts, V, VPV, VPV_min, VPV_max from solar;
1565383183.30413|12550|96|80|140
1565383242.29581|12550|80||
1565383301.35675|12550|76|60|80
sqlite> select _ts, V, VPV, VPV_min, VPV_max from solar where VPV > 79;
1565383183.30413|12550|96|80|140
1565383242.29581|12550|80|
```

## Detailed explanation
This example assumes that a mqtt broker is serving on 127.0.0.1 port 1883. 
If we now run this command ...

```bash
~$ mjs topics "#" &
```

A mqtt client will start listing for all topics on ip:port 127.0.0.1:1883. 
If we send a message to a mqtt server like this ...

```bash 
~$ mosquitto_pub --host 127.0.0.1 --port 1883 -t solar_panel -m '{"volts": 13.2}'
```

Then the logs will show this...

```bash
...
2019-08-10 09:46:59,701 New solar_panel message
2019-08-10 09:46:59,701 Create Table solar_panel
2019-08-10 09:46:59,702 CREATE TABLE solar_panel (_ts REAL PRIMARY KEY,volts REAL);
2019-08-10 09:46:59,712 Caching table
2019-08-10 09:46:59,712 INSERT INTO solar_panel (volts,_ts) VALUES (13.2,1565426819.7128189);
2019-08-10 09:46:59,713 solar_panel data saved
...
```

And the sqlite db will show ...

```bash 
~$ sqlite> .schema solar_panel
CREATE TABLE solar_panel (_ts REAL PRIMARY KEY,volts REAL);
~$ sqlite> select * from solar_panel;
1565426819.71282|13.2
```


## Good bits and bad bits

### Cons
* No encryption
* Needs auto documentation
* Will only work when mqtt messages are in json format
* No relationships between tables
* DBA's should turn away now. 
*.. No normalization on these tables
*.. Each table entry has a timestamped primary key. Make sure your ntp is working :) 
* Name change required to "q2q"

### Pros
* Listen to one or many mqtt topics
* Quick capture and analysis 
* Auto creates tables
* Maps mqtt topics to sqlite tables
* Auto adjusts tables for new mqtt message keys 
* Databases get reused and not overwritten if the application restarts


## Future development
* Need to use tox
* Need to use python-semantic-release
