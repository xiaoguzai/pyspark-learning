from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test"). \
        setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1,3,2,4,7,9,6],3)

    def ride10(data):
        result = list()
        for i in data:
            result.append(i*10)
        print(result)

    print(rdd.foreachPartition(ride10))
    r"""
    运行结果：
    [10,30]
    [20,40]
    [70,90,60]
    """