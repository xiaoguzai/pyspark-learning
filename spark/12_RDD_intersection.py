from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("create rdd"). \
        setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd1 = sc.parallelize([('a',1),('b',1)])
    rdd2 = sc.parallelize([('a',1),('c',1)])
    union_rdd = rdd1.intersection(rdd2)
    print(union_rdd.collect())
    #运行结果[('a',1)]