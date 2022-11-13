# coding:utf8
import string

import pandas as pd
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
from pyspark.sql import functions as F
#官方代码的思路是把每一个单词当成一个数据库元素，然后建表，进行统一处理
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType,StringType,IntegerType,ArrayType
if __name__ == '__main__':
    # 0.构建执行环境入口对象SparkSession
    # 0.构建执行环境入口对象SparkSession
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        config("spark.sql.shuffle.partitions",2).\
        getOrCreate()
    sc = spark.sparkContext

    rdd = sc.parallelize([[1],[2],[3]])
    df = rdd.toDF(["num"])
    # TODO 方式1注册，将数字乘以10
    def split_line(num):
        return {"num":num, "letter_str":string.ascii_letters[num]}
    struct_type = StructType().add("num", IntegerType(), nullable=True).\
        add("letter_str",StringType(),nullable=True)
    #返回值用于DSL风格 内部注册名称用于SQL(字符串表达式)风格

    udf2 = spark.udf.register("udf1", split_line, struct_type)
    df.select(udf2(df['num'])).show()
    r"""
    +---------+
    |udf1(num)|
    +---------+
    |   {1, b}|
    |   {2, c}|
    |   {3, d}|
    +---------+
    """

    #select udf1(num)
    df.selectExpr("udf1(num)").show()
    r"""
    +---------+
    |udf1(num)|
    +---------+
    |   {1, b}|
    |   {2, c}|
    |   {3, d}|
    +---------+
    """

    # TODO 方式2注册，仅能用DSL风格
    udf3 = F.udf(split_line, struct_type)
    df.select(udf3(df['num'])).show(truncate=False)
    r"""
    +---------+
    |udf1(num)|
    +---------+
    |   {1, b}|
    |   {2, c}|
    |   {3, d}|
    +---------+
    """

    # StructType是一个普通Spark支持的结构化类型
    # 只可用于DF中描述Schema和UDF中描述返回值是字典的数据