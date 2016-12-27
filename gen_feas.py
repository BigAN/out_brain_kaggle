import pandas as pd

import numpy as np
import outbrain.const as cst
import dask.dataframe as dd
import os

pd.merge
reg = 10  # trying anokas idea of regularization

train = dd.read_csv(os.path.join(cst.out_data_root, "clicks_train.csv"))  # display_id,ad_id,clicked
test = dd.read_csv(os.path.join(cst.out_data_root, "clicks_test.csv"))  # display_id,ad_id,clicked

doc_cate = dd.read_csv(
        os.path.join(cst.out_data_root, "documents_categories.csv"))  # document_id,category_id,confidence_level
doc_entity = dd.read_csv(
        os.path.join(cst.out_data_root, "documents_entities.csv"))  # document_id,entity_id,confidence_level
doc_topics = dd.read_csv(os.path.join(cst.out_data_root, "documents_topics.csv"))  # document_id,topic_id,confidence_level
events = dd.read_csv(os.path.join(cst.out_data_root, "events.csv"))  # document_id,topic_id,confidence_level
promoted_content = dd.read_csv(
        os.path.join(cst.out_data_root, "promoted_content.csv"))  # ad_id,document_id,campaign_id,advertiser_id

train.head()

doc_cate.rename(columns=["doc_cate_conf_level","document_id","entity_id"])
doc_entity.rename(columns={'confidence_level': "doc_entity_conf_level","document_id":"document_id","entity_id":"entity_id"})
doc_topics.rename(columns={'confidence_level': "doc_topics_conf_level","document_id":"document_id","entity_id":"entity_id"})

cnt = train[train.clicked == 1].ad_id.value_counts()
cntall = train.ad_id.value_counts()

def get_ad_click_rate(ad_id):

    return cnt.get(ad_id,0) / (cntall.get(ad_id,0) + reg)

promoted_content['ad_click_rate'] = promoted_content.ad_id.apply(lambda x:get_ad_click_rate(x))

print "cal click rate done"
def mult_merge(l, key):
    x1 = l[0]
    for x in l[1:]:
        x1 = x1.merge(x, on=key,how='outer')
    return x1
doc_info = mult_merge([doc_cate,doc_entity,doc_topics],"document_id")

print "print mult merge done"
#join ad
ad_doc_info = promoted_content.merge(doc_info,on = "document_id")
ad_doc_info.head()
ad_doc_info.to_csv("/Users/dongjian/work/kaggle/outbrain/data/ad_doc_info")

print "ad_doc_info done"
#test.join
rs = test.merge(ad_doc_info,on = "ad_id")\
    # .merge(events,on="display_id")
rs.to_csv("/Users/dongjian/data/outbrain_featrues")

print "all done"