# coding:utf8

from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(["hadoop spark hadoop","spark hadoop hadoop","hadoop flink spark"])
    # 得到所有单词，组成RDD
    rdd2 = rdd.map(lambda line:line.split(" "))
    print(rdd2.collect())
    #结果[['hadoop', 'spark', 'hadoop'], ['spark', 'hadoop', 'hadoop'], ['hadoop', 'flink', 'spark']]
    #问题在于这个数据结构属于嵌套数据结构

    #flatMap传入的参数和map一直，就是给map逻辑用的，解除嵌套无需逻辑传参
    rdd2 = rdd.flatMap(lambda line:line.split(" "))
    print(rdd2.collect())
    #结果['hadoop','spark','hadoop','spark','hadoop','hadoop','hadoop','flink','spark']
