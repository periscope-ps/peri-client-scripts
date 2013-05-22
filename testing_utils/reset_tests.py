#!/usr/bin/python
'''
This is a very specific (to my machine) script for clearing out
three unis instances running locally and re-populating them with
topologies stored in local files. It's not very useful by itself, but
it demonstrates how to do some things.
'''

import pymongo
import blipp.unis_client
import json

TOPO_LOC = "/home/jaffee/repos/peri-ps-ng/testjson/"

def drop_db(db_name):
    c = pymongo.MongoClient()
    c.drop_database(db_name)

def post_topo(unis_url, topo_file):
    topo_dict = json.loads(open(topo_file).read())
    unis = blipp.unis_client.UNISInstance({"unis_url": unis_url})
    unis.post("/topologies", data=topo_dict)


def main():
    drop_db("unis1")
    drop_db("unis2")
    drop_db("unis3")
    post_topo("http://localhost:8881", TOPO_LOC + "unis1topo.json")
    post_topo("http://localhost:8882", TOPO_LOC + "unis2topo.json")
    post_topo("http://localhost:8883", TOPO_LOC + "unis3topo.json")


if __name__=="__main__":
    main()
