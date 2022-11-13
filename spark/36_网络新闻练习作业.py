from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel
from defs_35 import context_jieba, filter_words, append_words, extract_user_and_word
from operator import add
import os
os.environ["HADOOP_CONF_DIR"] = "/export/server/hadoop/etc/hadoop"

r"""
86.149.9.216 10001 17/05/2015:10:05:30 GET /presentations/logstash-monitorama-2013/images/github-contributions.png
83.149.9.216 10002 17/05/2015:10:06:53 GET /presentations/logstash-monitorama-2013/css/print/paper.css
83.149.9.216 10002 17/05/2015:10:06:53 GET /presentations/logstash-monitorama-2013/css/print/paper.css
"""
if __name__ == '__main__':
    # 0. 初始化执行环境 构建SparkContext对象
    conf = SparkConf().setAppName("test").setMaster("yarn")
    sc = SparkContext(conf=conf)

    # 1. 读取数据文件
    file_rdd = sc.textFile("hdfs://node1:8020/input/apache.log")
    # 2. 对数据进行切分 \t
    split_rdd = file_rdd.map(lambda x: x.split(" "))
    print(split_rdd.collect())

    # 3. 因为要做多个需求, split_rdd 作为基础的rdd 会被多次使用.
    # 多次使用想到使用缓存或者checkpoint来解决
    # 这里想到简单的，使用缓存来解决
    split_rdd.persist(StorageLevel.DISK_ONLY)

    #任务1：计算当前网站被访问次数并排序
    result1 = split_rdd.map(lambda x:(x[4],1))
    result1 = result1.reduceByKey(lambda a,b:a+b). \
        sortBy(lambda x:x[1],ascending=False, numPartitions=1). \
        take(5)
    print(result1)

    #任务2：计算当前访问的用户数并排序
    result2 = split_rdd.map(lambda x:(x[1],1))
    result2 = result2.reduceByKey(lambda a,b:a+b). \
        sortBy(lambda x:x[1],ascending=False, numPartitions=1). \
        take(5)
    print(result2)

    #任务3：有哪些ip访问了本网站
    r"""
    rdd = sc.parallelize([('a',1),('a',1),('b',1),('b',1),('b',1)])
    grouped_rdd = rdd.groupByKey()
    print(grouped_rdd.map(lambda x:(x[0],list(x[1]))).collect())
    """
    result3 = split_rdd.map(lambda x:(x[0]+"_"+x[1],1)). \
        sortBy(lambda x:x[1],ascending=False, numPartitions=1). \
        take(5)
    grouped_rdd = split_rdd.map(lambda x:(x[0],x[1]))
    grouped_rdd = grouped_rdd.groupByKey()
    print(grouped_rdd.map(lambda x:(x[0],list(x[1]))).collect())