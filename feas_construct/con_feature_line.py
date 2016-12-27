# encoding:utf8
import pandas as pd
import numpy as np
import codecs as cs
import os

import outbrain.const as cst
from pymongo import MongoClient

mongo_db = MongoClient(cst.out_db_uri, connect=False)[cst.out_db]
clicks_train = mongo_db.clicks_train
clicks_test = mongo_db.clicks_test
promoted_content_coll = mongo_db.promoted_content
doc_info_c = mongo_db.doc_info


def get_ad_info(ad_col, ad_id):
    return ad_col.find_one({"ad_id": ad_id})


def get_doc_info(doc_info, doc_id):
    return list(doc_info.find({"document_id": doc_id}))


def parse_doc_info(doc_infos):
    if doc_infos:
        cates = "#".join([str(d['category_id']) for d in doc_infos])
        entity = "#".join([str(e.get('entity_id', "")) for e in doc_infos[0]['entity']])
        topics = "#".join([str(e.get('topic_id', "")) for e in doc_infos[0]['topics']])
        return ",".join([cates, entity, topics])
    else:
        return ",".join(["","",""])


def parse_click(c):
    display_id, ad_id, clicked = c.get("display_id", ""), c.get("ad_id", ""), c.get("clicked", 0)
    ad_info = get_ad_info(promoted_content_coll, ad_id)
    if ad_info:
        doc_info_str = parse_doc_info(get_doc_info(doc_info_c, ad_info['document_id']))
        return ",".join(
                map(str,
                    [display_id, ad_id, ad_info['document_id'], ad_info['campaign_id'], ad_info['advertiser_id'],
                     ad_info.get('ad_click_rate', 0), doc_info_str, clicked]))
    else:
        return ",".join(map(str, [display_id, ad_id, "", "", "", "", "","","", 0]))


def transform_data(clicks):
    '''
    将点击行为泛华,转化为feature_line
    :param clicks:
    :return: display_id,ad_id,doc_id,compaign_id,adv_id,ad_click_rate,cates,entities,topics
    '''
    rs = []

    with cst.TimeRecord("total"):
        # with cst.TimeRecord("cs load"):
        #     cs = [c for c in clicks.find().limit(10)]
        # return p.map(parse_click, cs)
        for n, c in enumerate(clicks.find().limit(10 ** 5)):
            # with cst.TimeRecord("total"):
            if n % 100 == 1:
                print n
            rs.append(parse_click(c))
        return rs


if __name__ == "__main__":
    # print get_ad_info(promoted_content_coll, 1)
    # print parse_doc_info(get_doc_info(doc_info, 6614))
    import multiprocessing as mp

    pool = mp.Pool(8)
    STAMP = 10 ** 6

    # with cst.TimeRecord("total"):
    #     open(cst.train_out, "w").close()
    #
    #     t_cs = []
    #     for n, c in enumerate(clicks_train.find()):
    #         if n % 1000000 == 1:
    #             print "num :", n
    #
    #         if len(t_cs) <= STAMP:
    #             t_cs.append(c)
    #         else:
    #             with cst.TimeRecord("one round"):
    #                 print "process start"
    #                 rs = pool.map(parse_click, t_cs)
    #                 with cs.open(cst.train_out, "a", "utf8") as f:
    #                     f.write("\n".join(rs))
    #                 t_cs = []
    #
    #     else:
    #         rs = pool.map(parse_click, t_cs)
    #         with cs.open(cst.train_out, "a", "utf8") as f:
    #             f.write("\n".join(rs))

    # with cst.TimeRecord("sec total"):
    #     open(cst.test_out, "w").close()
    #     t_cs = []
    #     for n, c in enumerate(clicks_test.find()):
    #         if n % 1000000 == 1:
    #             print "num :", n
    #
    #         if len(t_cs) <= STAMP:
    #             t_cs.append(c)
    #         else:
    #             with cst.TimeRecord("one round"):
    #                 print "process start"
    #                 rs = pool.map(parse_click, t_cs)
    #                 with cs.open(cst.test_out, "a", "utf8") as f:
    #                     f.write("\n".join(rs))
    #                 t_cs = []
    #
    #     else:
    #         rs = pool.map(parse_click, t_cs)
    #         with cs.open(cst.test_out, "a", "utf8") as f:
    #             f.write("\n".join(rs))
    # writer = cst.gen_write_data()
    #
    # for data in cst.read_data_from_generator(clicks_test.find(), num=1000000, stamp=1000000):
    #     rs = pool.map(parse_click,data)
    #     writer("\n".join(rs) + "\n",cst.out_test_data_for_test)

    cst.pipeline(clicks_train.find().limit(10**6), cst.gen_write_data(), parse_click, cst.train_out, 10 ** 6, 10 ** 6)
    # cst.pipeline(clicks_test.find(),cst.gen_write_data(),parse_click,cst.test_out,10 ** 6,10 ** 6)
