import pandas as pd
import numpy as np
import outbrain.const as cst
import os
from pymongo import MongoClient

mongo_db = MongoClient(cst.out_db_uri)[cst.out_db]
promoted_content_coll = mongo_db.promoted_content
reg = 3  # trying anokas idea of regularization
train = pd.read_csv(os.path.join(cst.data_root, "clicks_train.csv"))  # display_id,ad_id,clicked
test = pd.read_csv(os.path.join(cst.data_root, "clicks_test.csv"))  # display_id,ad_id,clicked

promoted_content = pd.read_csv(
        os.path.join(cst.data_root, "promoted_content.csv"))  # ad_id,document_id,campaign_id,advertiser_id

train.head()

# doc_cate.rename(columns=["doc_cate_conf_level","document_id","entity_id"])
# doc_entity.rename(columns={'confidence_level': "doc_entity_conf_level","document_id":"document_id","entity_id":"entity_id"})
# doc_topics.rename(columns={'confidence_level': "doc_topics_conf_level","document_id":"document_id","entity_id":"entity_id"})

cnt = train[train.clicked == 1].ad_id.value_counts()
cntall = train.ad_id.value_counts()


def get_ad_click_rate(ad_id):
    if cnt.get(ad_id, 0) / ((cntall.get(ad_id, 0) + reg) * 1.0) > 0:
        print ad_id,cnt.get(ad_id, 0) / ((cntall.get(ad_id, 0) + reg) * 1.0)
    return cnt.get(ad_id, 0) / (cntall.get(ad_id, 0) + reg + 0.0)


promoted_content['ad_click_rate'] = promoted_content.ad_id.apply(lambda x: get_ad_click_rate(x))


def to_mongo(x):
    promoted_content_coll.find_one_and_update({"ad_id": x['ad_id']}, {"$set": {"ad_click_rate": x['ad_click_rate']}})
    # print x


promoted_content.head()
promoted_content.apply(to_mongo, axis=1)
