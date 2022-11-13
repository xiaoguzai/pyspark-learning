# coding:utf8
# 演示spark的accumulator累加器
from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':
    #构建SparkConf对象
    conf = SparkConf().setAppName("helloworld").setMaster("local[*]")
    #构建SparkContext执行环境入口对象
    sc = SparkContext(conf = conf)
    rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10],2)
    acmlt = sc.accumulator(0)
    def map_func(data):
        global acmlt
        acmlt += 1
        print(acmlt)
    rdd2 = rdd.map(map_func)
    rdd2.collect() # 从内存中回收rdd2的变量
    rdd3 = rdd2.map(lambda x:x)
    rdd3.collect()
    print(acmlt)
    r"""
    第一次rdd2被action之后，累加器值为10，然后rdd2就没有数据了
    当rdd3构建出来时，是依赖rdd2的，rdd2没数据，就需要重新生成
    重新生成就导致累加器累加数据的代码再次被执行
    所以代码结果是20
    
    解决方法，让rdd2存在于内存之中，把rdd2.collect()注释掉
    """
