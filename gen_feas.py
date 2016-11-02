import pandas as pd
import numpy as np
import const as cst
import os

reg = 10 # trying anokas idea of regularization

train = pd.read_csv(os.path.join(cst.data_root,"clicks_train.csv"))

train.head()
cnt = train[train.clicked==1].ad_id.value_counts()
cntall = train.ad_id.value_counts()

def get_ad_click_rate(ad_id):
    return cnt(ad_id)/cntall +

