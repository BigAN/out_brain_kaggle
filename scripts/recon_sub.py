import os
import codecs as cs
# import arrow
import itertools as it

out_data_root = "/Users/dongjian/work/kaggle/outbrain/data"
data_root = "/Users/dongjian/data/"

sub = os.path.join(data_root, "submission1234.csv")

click_test = os.path.join(out_data_root, "clicks_test.csv")
sub_format = os.path.join(out_data_root, "submittion" + "20161227" + ".csv")

with cs.open(sub, "r", "utf8") as sub, \
        cs.open(click_test, "r", "utf8") as clt, \
        cs.open(sub_format, "w", "utf8") as sf:
    header = clt.readline()
    sf.write(header )
    tmp_key = None
    rs = []
    for n, x in enumerate(it.izip(sub, clt)):
        if n % 10 ** 6 == 1:
            print "proces line {0}".format(n)


        def extract(inp):
            af_stp = map(lambda x: x.strip(), x)
            rate = af_stp[0].split(",")[1]
            dis_id, ad_id = af_stp[1].split(",")
            return dis_id, ad_id, rate


        dis_id, ad_id, rate = extract(x)
        if not tmp_key:
            tmp_key = dis_id
            rs.append((ad_id, rate))
        if tmp_key and dis_id == tmp_key:
            rs.append((ad_id, rate))
        elif tmp_key and dis_id != tmp_key:
            srs = sorted(rs, key=lambda x: -float(x[1]))
            # print "{0},{1}".format(tmp_key, map(lambda x: x[0], srs))
            sf.write("{0},{1}".format(tmp_key," ".join(map(lambda x: x[0], srs)) + "\n"))

            tmp_key = dis_id
            rs = []
            rs.append((ad_id, rate))
        else:
            raise Exception("not this way , logic error")
    else:
        srs = sorted(rs, key=lambda x: -float(x[1]))
        sf.write("{0},{1}".format(tmp_key, " ".join(map(lambda x: x[0], srs)) + "\n"))