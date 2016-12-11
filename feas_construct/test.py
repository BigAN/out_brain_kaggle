import pandas as pd
import numpy as np
import outbrain.const as cst
import dask.dataframe as dd
import os
from pymongo import MongoClient

mongo_db = MongoClient(cst.out_db_uri)[cst.out_db]
promoted_content_coll = mongo_db.promoted_content

promoted_content_coll.find_one_and_update({"ad_id":1},{"$set":{"haha":1}})

