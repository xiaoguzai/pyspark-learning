from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test"). \
        setMaster("local[*]")
    #！！！这上面不能够加入os.environ["HADOOP_CONF_DIR"] = "/export/server/hadoop/etc/hadoop"
    #原因在于加入之后rdd就会从hdfs根目录去读取文件，而不是从node1的根目录去读取文件
    sc = SparkContext(conf=conf)

    print(sc.parallelize([3,2,1,4,5,6]).count())
    #6,总共6个数