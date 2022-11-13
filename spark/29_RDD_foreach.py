from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test"). \
        setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1,3,2,4,7,9,6],1)
    r = rdd.foreach(lambda x:print(x*10))
    print(r)
    r"""
    输出结果：
    10
    30
    20
    40
    70
    90
    60
    """