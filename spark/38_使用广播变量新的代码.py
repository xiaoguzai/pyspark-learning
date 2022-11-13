# coding:utf8
# 演示spark的广播变量
import time
from pyspark import SparkConf,SparkContext
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':
    # 构建SparkConf对象
    conf = SparkConf().setAppName("helloworld").setMaster("local[*]")
    # 构建SparkContext执行环境入口对象
    sc = SparkContext(conf=conf)

    stu_info_list = [(1, '张大仙', 11),
                     (2, '王晓晓', 13),
                     (3, '张甜甜', 11),
                     (4, '王大力', 11)]

    # 1.将本地list标记成为广播变量
    broadcast = sc.broadcast(stu_info_list)

    score_info_rdd = sc.parallelize([
        (1, '语文', 99),
        (2, '数学', 99),
        (3, '英语', 99),
        (4, '编程', 99),
        (1, '语文', 99),
        (2, '编程', 99),
        (3, '语文', 99),
        (4, '英语', 99),
        (1, '语文', 99),
        (3, '英语', 99),
        (2, '编程', 99)
    ])

    def map_func(data):
        id = data[0]
        name = ''
        # 2.使用广播变量，从broadcast对象中本地list对象即可
        value = broadcast.value
        for i in value:
            if id == i[0]:
                name = i[1]
        return (name,data[1],data[2])

    print(score_info_rdd.map(map_func).collect())
    #运行结果
    #[('张大仙', '语文', 99), ('王晓晓', '数学', 99), ('张甜甜', '英语', 99),
    # ('王大力', '编程', 99), ('张大仙', '语文', 99), ('王晓晓', '编程', 99),
    # ('张甜甜', '语文', 99), ('王大力', '英语', 99), ('张大仙', '语文', 99),
    # ('张甜甜', '英语', 99), ('王晓晓', '编程', 99)]