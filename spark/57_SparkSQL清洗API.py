# coding:utf8
import pandas as pd
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
from pyspark.sql import functions as F
#官方代码的思路是把每一个单词当成一个数据库元素，然后建表，进行统一处理
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType,StringType,IntegerType
if __name__ == '__main__':
    # 0.构建执行环境入口对象SparkSession
    spark = SparkSession.builder.\
        appName("create df").\
        master("local[*]").\
        config("spark.sql.shuffle.partitions",10).\
        getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions",10)
    sc = spark.sparkContext

    #读取数据
    df = spark.read.format("csv").\
        option("sep",";").\
        option("header",True).\
        load("../data/sql/people.csv")

    #数据清洗：数据去重
    #dropDuplicate是DataFrame的API，可以完成数据去重
    #无参数使用，对全部数据联合比较，无重复值只保留一条
    df.dropDuplicates().show()

    #有参数使用，针对特定的字段进行去重
    df.dropDuplicates(['age','job']).show()

    #数据清洗：缺失处理
    #dropna api是可以对缺失值的数据进行删除
    #无参数使用，只要列中有null 就删除这一行数据
    df.dropna().show()
    df.dropna()
    #点到dropna可以查看原始的函数
    r"""
        how : str, optional
        'any' or 'all'.
        If 'any', drop a row if it contains any nulls.
        If 'all', drop a row only if all its values are null.
    thresh: int, optional
        default None
        If specified, drop rows that have less than `thresh` non-null values.
        This overwrites the `how` parameter.
    subset : str, tuple or list, optional
        optional list of column names to consider.
    """
    #thresh：最少满足3个有效列，不满足就删除数据
    df.dropna(thresh=3)

    df.dropna(thresh=2, subset=['name','age']).show()

    #缺失值处理也可以完成对缺失值进行填充
    #DataFrame的fillna对缺失的列进行填充
    df.fillna("loss").show()

    #指定列进行填充
    df.fillna("N/A",subset=['job']).show()

    #设定一个字典，对所有的列提供填充
    df.fillna({"name":"未知姓名","age":1,"job":"worker"}).show()