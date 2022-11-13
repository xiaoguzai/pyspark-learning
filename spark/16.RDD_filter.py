from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("create rdd"). \
        setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1,2,3,4,5])
    print(rdd.filter(lambda x:x%2 == 1).collect())