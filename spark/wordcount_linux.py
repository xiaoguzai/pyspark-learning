# coding:utf8
from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("WordCountHelloWorld")
    sc = SparkContext(conf=conf)
    #通过SparkConf对象构建SparkContext对象

    # 需求：wordcount单词计数，读取HDFS上的words.txt文件，
    # 对其内部的单词统计出现的数量
    file_rdd = sc.textFile("../data/words.txt")

    # 将单词进行切割，得到一个存储
    words_rdd = file_rdd.flatMap(lambda  line:line.split(" "))

    # 将单词转换为元组对象，key为单词，value为数字1
    words_with_one_rdd = words_rdd.map(lambda x:(x,1))

    # 将元组的value按照key分组，对所有的value执行聚合操作
    result_rdd = words_with_one_rdd.reduceByKey(lambda a,b:a+b)

    # 通过collect方法收集RDD的数据输出结果
    print(result_rdd.collect())
