#!/bin/bash

pkill -9 -f 'mjs server 10.10.20.1 port 1823 topics solar'
pipenv run mjs server 10.10.20.1 port 1823 topics solar dbfile ./data/solar.db
