from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    # 构建Spark运行环境
    conf = SparkConf().setAppName("create rdd"). \
        setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a',1),('a',11),('a',6),('b',3),('b',5)])
    print(rdd.mapValues(lambda x:x*10).collect())
    #[('a', 10), ('a', 110), ('a', 60), ('b', 30), ('b', 50)]