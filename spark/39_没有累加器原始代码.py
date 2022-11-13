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
    count = 0
    def map_func(data):
        global count
        count += 1
        print(count)
    r"""
    首先count打印出来的内容为
    1 2 3 4 5 1 2 3 4 5
    最后一个count为0
    这是因为对于[1,2,3,4,5,6,7,8,9,10]被分成了两份
    每一份有5个数值，所以打印出来的内容依次为
    1 2 3 4 5,最后最终的输出count=0
    """
    rdd.map(map_func).collect()
    print(count)
    r"""
    最后的count为0的原因
                    executor:count(1,2,3,4,5)
    driver:count
                    executor:count(1,2,3,4,5)
    两个executor的内容为复制出来的，因此driver中的count
    仍然为0
    """