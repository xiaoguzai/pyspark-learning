from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    # 构建SparkContext对象
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 通过textFile API读取数据

    # 读取本地文件数据
    file_rdd1 = sc.textFile("../data/words.txt")
    print("默认读取分区数:",file_rdd1.getNumPartitions())
    # 默认分区数跟cpu无关，主要跟文件的大小有关，如果
    # 读取hdfs文件的时候，主要跟它的block块有关
    print("file_rdd1 内容:",file_rdd1.collect())

    # 加最小分区数参数的测试
    file_rdd2 = sc.textFile("../data/words.txt",3)
    file_rdd3 = sc.textFile("../data/words.txt",100)
    print("file_rdd2 分区数:",file_rdd2.getNumPartitions())
    print("file_rdd3 分区数:",file_rdd3.getNumPartitions())
    # file_rdd2分区数为3，file_rdd3分区数为70，最小分区数为
    # 参考值，spark有自己的判断，给的太大spark不会理会

    # 读取HDFS文件数据测试
    hdfs_rdd = sc.textFile("hdfs://node1:8020/input/words.txt")
    print("hdfs_rdd 内容:",hdfs_rdd.collect())

    #wholeTextFile适合读取一些小文件，这个API是小文件读取专用
