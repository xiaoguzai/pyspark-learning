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

    r"""
    df.write.mode().format().option(K,V).save(PATH)
    mode:传入模式字符串可选：append追加，overwrite覆盖，ignore忽略，error重复就报异常(默认的)
    format:传入格式字符串可选：text，csv，json
    注意text源只支持单列df写出
    option设置属性，如：.option("sep",","):r
    save写出的路径，支持本地文件和HDFS
    """

    # Write text写出，只能写出一个列的数据，需要将df转换为单列df
    # F.concat_ws就是将数据转换为单列的过程

    df.select(F.concat_ws("---","user_id","movie_id","rank","ts")).\
        write.\
        mode("overwrite").\
        format("text").\
        save("../data/sql/text")

    # Write csv
    df.write.mode("overwrite").\
        format("csv").\
        option("sep",";").\
        option("header",True).\
        save("../data/sql/csv")

    # Write Json
    df.write.mode("overwrite").\
        format("json").\
        save("../data/sql/json")

    # Write Parquet写出
    df.write.mode("overwrite").\
        format("parquet").\
        save("../data/sql/parquet")

    # 不给format，默认以parquet写出
    df.write.mode("overwrite").save("../data/sql/default")

    r"""
    这里运行完如果在服务器上需要同步一下
    选择对应文件夹，Deployment,Download from root@node1:22 passwd
    """

