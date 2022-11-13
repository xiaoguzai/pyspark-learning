from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test"). \
        setMaster("local[*]")
    sc = SparkContext(conf=conf)

    #saveAsTextFile:将RDD数据写入文本文件中
    #支持本地写出或者写出hdfs文件系统
    rdd = sc.parallelize([1,3,2,4,7,9,6],3)
    rdd.saveAsTextFile("hdfs://node1:8020/output/11111")
    r"""
    注意点：foreach和saveAsTextFile这两个算子是
    分区(Executor)直接执行的，跳过Driver由分区
    所在的Executor直接执行
    其余Action算子都会将结果发送至Driver
    """