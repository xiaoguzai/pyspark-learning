#coding utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 读取小文件文件夹
    rdd = sc.wholeTextFiles("../data/tiny_files")
    print(rdd.collect())
    # 读取出来的内容为各种元组，key为文件名，value为文件内容
    print(rdd.map(lambda x:x[1]).collect())