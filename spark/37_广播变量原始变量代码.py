# coding:utf8
# 演示spark的广播变量
import time

from pyspark import SparkConf,SparkContext
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':
    #构建SparkConf对象
    conf = SparkConf().setAppName("helloworld").setMaster("local[*]")
    #构建SparkContext执行环境入口对象
    sc = SparkContext(conf = conf)

    stu_info_list = [(1, '张大仙', 11),
                     (2, '王晓晓', 13),
                     (3, '张甜甜', 11),
                     (4, '王大力', 11)]

    score_info_rdd = sc.parallelize([
        (1,'语文',99),
        (2,'数学',99),
        (3,'英语',99),
        (4,'编程',99),
        (1,'语文',99),
        (2,'编程',99),
        (3,'语文',99),
        (4,'英文',99),
        (1,'语文',99),
        (3,'英语',99),
        (2,'编程',99)
    ])
    def map_func(data):
        id = data[0]
        name = ''
        for i in stu_info_list:
            if id == i[0]:
                name = i[1]
        return (name,data[1],data[2])
    #运行结果：[('张大仙', '语文', 99), ('王晓晓', '数学', 99),
    # ('张甜甜', '英语', 99), ('王大力', '编程', 99),
    # ('张大仙', '语文', 99), ('王晓晓', '编程', 99),
    # ('张甜甜', '语文', 99), ('王大力', '英文', 99),
    # ('张大仙', '语文', 99), ('张甜甜', '英语', 99),
    # ('王晓晓', '编程', 99)]

    print(score_info_rdd.map(map_func).collect())