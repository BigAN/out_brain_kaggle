import pymongo
import const as cst
import codecs as cs
from pymongo import MongoClient

mongo_db = MongoClient(cst.ua_db_uri)[cst.ua_db]



def from_csv_to_cc(file,collection):
    with cs.open(file,"r","utf8") as f:
        headers = f.readline()

