r"""
总结：我的问题
问题1：没有想到将特殊字符注册成广播变量进行传播
问题2：对于字符串处理没有想到使用strip的方法进行去除
问题3：对字符串使用空格切分
"""
from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel
from operator import add
import re

if __name__ == '__main__':
    #构建SparkConf对象
    conf = SparkConf().setAppName("broadcast_accumulator").setMaster("local[*]")
    #构建SparkContext执行环境入口对象
    sc = SparkContext(conf = conf)

    #1. 读取文件
    file_rdd = sc.textFile("../data/accumulator_broadcast_data.txt")
    # 特殊字符的list集合，注册成广播变量节约内存和网络IO的开销
    abnormal_char = [",",".","!","#","$","%"]
    broadcast = sc.broadcast(abnormal_char)
    #注册累加器用于对特殊字符+1操作
    acmlt = sc.accumulator(0)

    #2. 过滤空行
    lines_rdd = file_rdd.filter(lambda line:line.strip())
    #3. 前后空格去除
    data_rdd = lines_rdd.map(lambda x:x.strip())
    #data_rdd = ['hadoop spark # hadoop spark spark', 'mapreduce ! spark spark hive !', 'hive spark hadoop mapreduce spark %', 'spark hive sql sql spark hive , hive spark !', '!  hdfs hdfs mapreduce mapreduce spark hive', '#']
    #4. 对于字符串使用空格切分
    words_rdd = data_rdd.flatMap(lambda x:re.split("\s+",x))
    #words_rdd = ['hadoop', 'spark', '#', 'hadoop', 'spark', 'spark', 'mapreduce', '!', 'spark', 'spark', 'hive', '!', 'hive', 'spark', 'hadoop', 'mapreduce', 'spark', '%', 'spark', 'hive', 'sql', 'sql', 'spark', 'hive', ',', 'hive', 'spark', '!', '!', 'hdfs', 'hdfs', 'mapreduce', 'mapreduce', 'spark', 'hive', '#']
    def filter_func(data):
        global acmlt
        abnormal_char = broadcast.value
        if data in abnormal_char:
            acmlt += 1
            return False
        else:
            return True

    #4. 过滤出来正常的单词
    normal_words_rdd = words_rdd.filter(filter_func)
    #5. 正常单词计数
    result_rdd = normal_words_rdd.map(lambda x:(x,1)). \
        reduceByKey(add)
    #result_rdd = [('hadoop', 3), ('hive', 6), ('hdfs', 2), ('spark', 11), ('mapreduce', 4), ('sql', 2)]

    print("正常单词计数结果：",result_rdd.collect())
    print("特殊字符数量：",acmlt)