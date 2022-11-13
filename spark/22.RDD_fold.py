from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test"). \
        setMaster("local[*]")
    #！！！这上面不能够加入os.environ["HADOOP_CONF_DIR"] = "/export/server/hadoop/etc/hadoop"
    #原因在于加入之后rdd就会从hdfs根目录去读取文件，而不是从node1的根目录去读取文件
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize(range(1,10),3)
    print(rdd.fold(10,lambda a,b:a+b))