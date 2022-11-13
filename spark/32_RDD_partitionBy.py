from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    # 构建Spark运行环境
    conf = SparkConf().setAppName("create rdd"). \
        setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('hadoop',1),('spark',1),('hello',1),('flink',1),('hadoop',1),('spark',1)])

    def partition_self(key):
        if 'hadoop' == key: return 0
        if ('spark' == key or 'flink' == key): return 1
        return 2

    print(rdd.partitionBy(3,partition_self).glom().collect())
    #!!!注意!!!分区号不要超标，你设置3个分区，分区号只能是0,1,2

    rdd2 = rdd.repartition(5)
    print(rdd2.glom().collect())
    #结果[[],[('hadoop',1)],[('flink',1),('hadoop',1),('spark',1)]
    #[('spark',1),('hello',1)],[]]
    rdd3 = rdd.repartition(1)
    print(rdd3.glom().collect())
    #[[('hadoop',1),('spark',1),('hello',1),('flink',1),('hadoop',1),('spark',1)]]

    #rdd.coalesce(参数1,参数2)
    #参数1：分区数，参数2：True or False
    #True表示允许shuffle，可以加分区，False表示不允许shuffle
    #不允许加分区，

    print(rdd.coalesce(5,shuffle=True).glom().collect())
    #[[], [('hadoop', 1)], [('flink', 1), ('hadoop', 1), ('spark', 1)], [('spark', 1), ('hello', 1)], []]

    print(rdd.coalesce(5).glom().collect())
    #[[('hadoop', 1)], [('spark', 1), ('hello', 1)], [('flink', 1)], [('hadoop', 1), ('spark', 1)]]