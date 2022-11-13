from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test"). \
        setMaster("local[*]")
    #！！！这上面不能够加入os.environ["HADOOP_CONF_DIR"] = "/export/server/hadoop/etc/hadoop"
    #原因在于加入之后rdd就会从hdfs根目录去读取文件，而不是从node1的根目录去读取文件
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1,1,1,1,1],1)
    print(rdd.takeSample(True,8))
    #参数1，True表示取同一个数据，False表示不允许取同一个数据
    #参数2：抽样要几个
    #参数3：随机数种子，传入一个数字即可，随便给