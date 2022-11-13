# coding:utf8
# 演示spark的accumulator累加器

from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':
    #构建SparkConf对象
    conf = SparkConf().setAppName("helloworld").setMaster("local[*]")
    #构建SparkContext执行环境入口对象
    sc = SparkContext(conf = conf)

    #10条数据，2个分区
    rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10],2)

    acmlt = sc.accumulator(0)

    def map_func(data):
        global acmlt
        acmlt += 1
        print(acmlt)

    rdd.map(map_func).collect()

    print(acmlt)
    #结果为10
