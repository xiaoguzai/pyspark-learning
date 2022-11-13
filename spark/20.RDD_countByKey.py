import json
from pyspark import SparkConf,SparkContext
#import os
#os.environ["HADOOP_CONF_DIR"] = "/export/server/hadoop/etc/hadoop"


if __name__ == '__main__':
    conf = SparkConf().setAppName("test"). \
        setMaster("local[*]")
    #！！！这上面不能够加入os.environ["HADOOP_CONF_DIR"] = "/export/server/hadoop/etc/hadoop"
    #原因在于加入之后rdd就会从hdfs根目录去读取文件，而不是从node1的根目录去读取文件
    sc = SparkContext(conf=conf)

    rdd1 = sc.textFile("../data/words.txt")
    rdd2 = rdd1.flatMap(lambda x:x.split(' '))
    rdd3 = rdd2.map(lambda x:(x,1))
    result = rdd3.countByKey()
    print('result = ')
    print(result)
    #defaultdict(<class 'int'>, {'hadoop': 7, 'spark': 3})