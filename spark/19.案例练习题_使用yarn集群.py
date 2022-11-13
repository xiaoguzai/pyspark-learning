r"""
{"id":1,"timestamp":"2019-05-08T01:03.00Z","category":"平板电脑","areaName":"北京","money":"1450"}|{"id":2,"timestamp":"2019-05-08T01:01.00Z","category":"手机","areaName":"北京","money":"1450"}|{"id":3,"timestamp":"2019-05-08T01:03.00Z","category":"手机","areaName":"北京","money":"8412"}
{"id":4,"timestamp":"2019-05-08T05:01.00Z","category":"电脑","areaName":"上海","money":"1513"}|{"id":5,"timestamp":"2019-05-08T01:03.00Z","category":"家电","areaName":"北京","money":"1550"}|{"id":6,"timestamp":"2019-05-08T01:01.00Z","category":"电脑","areaName":"杭州","money":"1550"}
{"id":7,"timestamp":"2019-05-08T01:03.00Z","category":"电脑","areaName":"北京","money":"5611"}|{"id":8,"timestamp":"2019-05-08T03:01.00Z","category":"家电","areaName":"北京","money":"4410"}|{"id":9,"timestamp":"2019-05-08T01:03.00Z","category":"家具","areaName":"郑州","money":"1120"}
{"id":10,"timestamp":"2019-05-08T01:01.00Z","category":"家具","areaName":"北京","money":"6661"}|{"id":11,"timestamp":"2019-05-08T05:03.00Z","category":"家具","areaName":"杭州","money":"1230"}|{"id":12,"timestamp":"2019-05-08T01:01.00Z","category":"书籍","areaName":"北京","money":"5550"}
{"id":13,"timestamp":"2019-05-08T01:03.00Z","category":"书籍","areaName":"北京","money":"5550"}|{"id":14,"timestamp":"2019-05-08T01:01.00Z","category":"电脑","areaName":"北京","money":"1261"}|{"id":15,"timestamp":"2019-05-08T03:03.00Z","category":"电脑","areaName":"杭州","money":"6660"}
{"id":16,"timestamp":"2019-05-08T01:01.00Z","category":"电脑","areaName":"天津","money":"6660"}|{"id":17,"timestamp":"2019-05-08T01:03.00Z","category":"书籍","areaName":"北京","money":"9000"}|{"id":18,"timestamp":"2019-05-08T05:01.00Z","category":"书籍","areaName":"北京","money":"1230"}
{"id":19,"timestamp":"2019-05-08T01:03.00Z","category":"电脑","areaName":"杭州","money":"5551"}|{"id":20,"timestamp":"2019-05-08T01:01.00Z","category":"电脑","areaName":"北京","money":"2450"}
{"id":21,"timestamp":"2019-05-08T01:03.00Z","category":"食品","areaName":"北京","money":"5520"}|{"id":22,"timestamp":"2019-05-08T01:01.00Z","category":"食品","areaName":"北京","money":"6650"}
{"id":23,"timestamp":"2019-05-08T01:03.00Z","category":"服饰","areaName":"杭州","money":"1240"}|{"id":24,"timestamp":"2019-05-08T01:01.00Z","category":"食品","areaName":"天津","money":"5600"}
{"id":25,"timestamp":"2019-05-08T01:03.00Z","category":"食品","areaName":"北京","money":"7801"}|{"id":26,"timestamp":"2019-05-08T01:01.00Z","category":"服饰","areaName":"北京","money":"9000"}
{"id":27,"timestamp":"2019-05-08T01:03.00Z","category":"服饰","areaName":"杭州","money":"5600"}|{"id":28,"timestamp":"2019-05-08T01:01.00Z","category":"食品","areaName":"北京","money":"8000"}|{"id":29,"timestamp":"2019-05-08T02:03.00Z","category":"服饰","areaName":"杭州","money":"7000"}
读取data文件夹中的order.text文件，提取北京的数据，组合北京和商品类别进行输出，
同时对结果集进行去重，得到北京售卖的商品类别信息
"""
# coding:utf8

from pyspark import SparkConf,SparkContext
from defs_19 import city_with_category
import json
import os
os.environ["HADOOP_CONF_DIR"] = "/export/server/hadoop/etc/hadoop"


if __name__ == '__main__':
    # 提交到yarn集群，这里设置为yarn
    # 如果这里不设置为yarn，可以通过/export/server/bin/spark-submit --master yarn --py-files ./defs_19.py /home/hadoop/main.py
    # 成功去提交
    conf = SparkConf().setAppName("test").setMaster("yarn")
    # 如果提交到集群运行，除了主代码以外，还依赖了其他的代码文件
    # 需要设置一个参数来告知spark还有依赖文件要同步上传到集群中
    # 参数叫做spark.submit.pyFiles
    # 参数值可以是单个.py文件，也可以是.zip压缩包(有多个依赖文件的时候
    # 可以用zip压缩上传)
    conf.set("spark.submit.pyFiles","defs_19.py")
    sc = SparkContext(conf=conf)


    file_rdd = sc.textFile("hdfs://node1:8020/input/order.text")
    print(file_rdd)

    print(file_rdd.collect())
    #['{"id":1,"timestamp":"2019-05-08T01:03.00Z","category":"平板电脑","areaName":"北京","money":"1450"}|{"id":2,"timestamp":"2019-05-08T01:01.00Z","category":"手机","areaName":"北京","money":"1450"}]
    file_rdd = file_rdd.flatMap(lambda x:x.split("|"))
    rdd = file_rdd.map(lambda data:json.loads(data))
    rdd = rdd.filter(lambda data:data["areaName"]=="北京")
    result_rdd = rdd.map(lambda x:x['areaName']+'_'+x['category'])
    #注意rdd.distinct().map会报错
    result_rdd = rdd.map(city_with_category)
    print(result_rdd.collect())