# coding:utf8

from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1,2,3,4,5,6],3)

    #定义方法，作为算子的传入函数体
    def add(data):
        return data*10

    print(rdd.map(add).collect())

    #更简单的方式 是定义lambda表达式来写匿名函数
    print(rdd.map(lambda data: data * 10).collect())
    #lambda表达式仅适用于一行代码就搞定的函数体