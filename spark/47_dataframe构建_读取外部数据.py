# coding:utf-8
# 演示将Panda的DataFrame转换成Spark的DataFrame
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
if __name__ == '__main__':
    # 0.构建执行环境入口对象SparkSession
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    sc = spark.sparkContext

    # 构建StructType，text数据源，读取数据的特点是
    # 将一整行只作为一个列读取，默认列明是value，类型是String
    schema = StructType().add("data",StringType(),nullable=True)
    df = spark.read.format("text").\
        schema(schema=schema).\
        load("../data/sql/people.txt")

    df.printSchema()
    r"""
    root
    |-- data: string (nullable = true)
    """
    df.show()
    r"""
    +-----------+
    |       data|
    +-----------+
    |Michael, 29|
    |   Andy, 30|
    | Justin, 19|
    +-----------+
    """