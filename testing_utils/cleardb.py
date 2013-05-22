#!/usr/bin/python
'''
Usage:
cleardb.py <database_name>

Drops the mongo database named at the command line.
'''

import pymongo
import sys

c = pymongo.MongoClient()
c.drop_database(sys.argv[1])
