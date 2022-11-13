from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("create rdd"). \
        setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([1,1,3,3])
    rdd2 = sc.parallelize([5,5,6,6,9,9])

    union_rdd = rdd1.union(rdd2)

    print(union_rdd.collect())
    #运行结果[1,1,3,3,5,5,6,6,9,9]
