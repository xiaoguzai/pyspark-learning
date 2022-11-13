from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test"). \
        setMaster("local[*]")
    #！！！这上面不能够加入os.environ["HADOOP_CONF_DIR"] = "/export/server/hadoop/etc/hadoop"
    #原因在于加入之后rdd就会从hdfs根目录去读取文件，而不是从node1的根目录去读取文件
    sc = SparkContext(conf=conf)

    #rdd.takeOrdered(参数1,参数2)
    #参数1：要几个数据，参数2：对排序的数据进行更改(不会更改数据本身，
    # 只是在排序的时候换个样子)
    rdd = sc.parallelize([1,3,2,4,7,9,6],1)
    print(rdd.takeOrdered(3))
    #输出结果[1,2,3]

    print(rdd.takeOrdered(3,lambda x:-x))
    #排序的时候按照-x进行排序，显示的时候仍然显示x