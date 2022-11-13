from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    # 构建Spark执行环境
    conf = SparkConf().setAppName("create rdd"). \
        setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1,1,3,3,5,5,6,6,9,9])

    print(rdd.distinct().collect())
    #输出结果[1,5,9,6,3]