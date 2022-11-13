# coding:utf8

from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a',1),('a',1),('b',1),('b',1),('a',1)])

    #reduceByKey对相同key的数值执行聚合相加
    print(rdd.reduceByKey(lambda a,b:a+b).collect())

    rdd = sc.parallelize([('a',1),('a',11),('b',3),('b',5)])
    #rdd.map(lamba x:(x[0],x[1]*10)).collect()
    print(rdd.mapValues(lambda value:value * 10).collect())