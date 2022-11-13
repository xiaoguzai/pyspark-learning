# coding:utf8
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

    rdd = sc.parallelize([["hadoop spark flink"],["hadoop flink java"]])
    df = rdd.toDF(["line"])
    # TODO 方式1注册
    # 注册UDF功能：将数字都乘10
    def split_line(line):
        return line.split(" ")
    # 返回值用于DSL风格，内部注册名称用于SQL风格
    udf2 = spark.udf.register("udf1", split_line, ArrayType(StringType()))
    df.select(udf2(df['line'])).show()
    #select udf1(num)
    r"""
    +--------------------+
    |          udf1(line)|
    +--------------------+
    |[hadoop, spark, f...|
    |[hadoop, flink, j...|
    +--------------------+
    """
    df.selectExpr("udf1(line)").show()
    r"""
    +--------------------+
    |          udf1(line)|
    +--------------------+
    |[hadoop, spark, f...|
    |[hadoop, flink, j...|
    +--------------------+
    """


    df.createTempView("lines")
    spark.sql("select udf1(line) from lines").show(truncate=False)
    r"""
    +----------------------+
    |udf1(line)            |
    +----------------------+
    |[hadoop, spark, flink]|
    |[hadoop, flink, java] |
    +----------------------+
    """

    # TODO 方式2注册，仅能用DSL风格
    udf3 = F.udf(split_line, ArrayType(StringType()))
    df.select(udf3(df['line'])).show(truncate=False)
    r"""
    +----------------------+
    |split_line(line)      |
    +----------------------+
    |[hadoop, spark, flink]|
    |[hadoop, flink, java] |
    +----------------------+
    """