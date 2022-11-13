import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
if __name__ == '__main__':
    #parquet是spark中常用的一种列示存储文件格式
    #和hive中的orc差不多，他俩都是列存储格式

    #parquet内置schema：列明、列类型、是否为空
    #存储以列作为存储格式
    #序列化存储在文件中，压缩属性体积小
    #可以通过avro and parquet view查看

    # 0.构建执行环境入口对象SparkSession
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    sc = spark.sparkContext

    #读取parquet类型的文件
    df = spark.read.format("parquet").load("../data/sql/users.parquet")
    df.printSchema()
    r"""
    root
    |-- name: string (nullable = true)
    |-- favorite_color: string (nullable = true)
    |-- favorite_numbers: array (nullable = true)
    |    |-- element: integer (containsNull = true)
    array类型是个嵌套，里面嵌套element
    """
    df.show()
