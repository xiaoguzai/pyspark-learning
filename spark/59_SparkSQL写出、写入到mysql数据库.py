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
    # 0.构建执行环境入口对象SparkSession
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        config("spark.sql.shuffle.partitions",2).\
        getOrCreate()
    sc = spark.sparkContext

    #1.读取数据集
    schema = StructType().add("user_id",StringType(),nullable=True).\
        add("movie_id",IntegerType(),nullable=True).\
        add("rank",IntegerType(),nullable=True).\
        add("ts",StringType(),nullable=True)
    df = spark.read.format("csv").\
        option("sep","\t").\
        option("header",False).\
        option("encoding","utf-8").\
        schema(schema=schema).\
        load("../data/sql/u.data")

    #db_name = "bigdata"
    #spark.sql(f"create database if not exists {db_name}")

    # option("user","root").\
    # option("passwd","123456").\

    r"""
    这里创建数据库在主机上创建：
    mysql -u root -p
    create database bigdata;
    """

    #写出df到mysql数据库
    df.write.mode("overwrite").\
        format("jdbc").\
        option("url","jdbc:mysql://node1:3306/bigdata?useSSL=false&useUnicode=true").\
        option("dbtable","movie_data").\
        save()