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

    # 读取csv文件
    df = spark.read.format("csv").\
        option("sep",";").\
        option("header",True).\
        option("encoding","utf-8").\
        schema("name STRING, age INT, job STRING").\
        load("../data/sql/people.csv")
    # 注意写法：类型必须大写

    df.printSchema()
    r"""
    root
    |-- name: string (nullable = true)
    |-- age: integer (nullable = true)
    |-- job: string (nullable = true)
    """
    df.show()
    r"""
    +-----+----+---------+
    | name| age|      job|
    +-----+----+---------+
    |Jorge|  30|Developer|
    |  Bob|  32|Developer|
    |  Ani|  11|Developer|
    | Lily|  11|  Manager|
    |  Put|  11|Developer|
    |Alice|   9|  Manager|
    |Alice|   9|  Manager|
    |Alice|   9|  Manager|
    |Alice|   9|  Manager|
    |Alice|null|  Manager|
    |Alice|   9|     null|
    +-----+----+---------+
    """