from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("create rdd"). \
        setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([('a',1),('a',1),('b',1),('b',1),('b',1)])
    grouped_rdd = rdd.groupByKey()
    print(grouped_rdd.map(lambda x:(x[0],list(x[1]))).collect())
    #运行结果：[('b',[1,1,1]),('a',[1,1])]