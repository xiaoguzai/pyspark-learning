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

    # JSON类型自带有Schema信息
    df = spark.read.format("json").load("../data/sql/people.json")
    df.printSchema()
    r"""
    root
    |-- age: long (nullable = true)
    |-- name: string (nullable = true)
    """
    df.show()
    r"""
    +----+-------+
    | age|   name|
    +----+-------+
    |null|Michael|
    |  30|   Andy|
    |  19| Justin|
    +----+-------+
    """