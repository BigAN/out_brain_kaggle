from __future__ import unicode_literals
import os
import time
import codecs

out_data_root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
data_root = "/Users/dongjian/data/"
from pymongo import MongoClient

out_db_uri = 'mongodb://localhost:27017/outbrain'
out_db = 'outbrain'

train_out = os.path.join(out_data_root, "out_train_data")
test_out = os.path.join(out_data_root, "out_test_data")
out_test_data_for_test = os.path.join(out_data_root, "out_test_data_for_test")

sub = os.path.join(data_root, "submission1234.csv")


def pipeline(read, writer, func, filepath, num, stamp):
    import multiprocessing as mp
    p = mp.Pool(8)
    writer = gen_write_data()

    for data in read_data_from_generator(read, num, stamp):
        rs = p.map(func, data)
        writer("\n".join(rs) + "\n", filepath)


def read_data_from_generator(gen, num, stamp=100000, time_record=False):
    data = []
    scope = num
    for n, l in enumerate(gen, start=0):
        if n % stamp == 0:
            print n
        if n != scope:
            data.append(l)
        else:
            yield data
            scope += num
            data = []
            data.append(l)
    else:
        yield data


def gen_write_data():
    d = {"n": 1}

    def write_data(data, file_path):
        write_way = "w" if d['n'] == 1 else "a"
        with codecs.open(file_path, write_way, "utf8") as f:
            f.write(data)
        d['n'] += 1

    return write_data


def mult_merge(l, key):
    x1 = l[0]
    for x in l[1:]:
        x1 = x1.merge(x, on=key, how='outer')
    return x1


class TimeRecord(object):
    t = None

    def __init__(self, name):
        self.t = time.time()
        self.name = name

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        print "{name}   cost {time} seconds".format(**{"name": self.name, "time": time.time() - self.t})
