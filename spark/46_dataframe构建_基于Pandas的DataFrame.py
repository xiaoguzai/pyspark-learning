# coding:utf-8
# 演示将Panda的DataFrame转换成Spark的DataFrame
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
if __name__ == '__main__':
    spark = SparkSession.builder.\
        appName("create df").\
        master("local[*]").\
        getOrCreate()
    sc = spark.sparkContext
    #构建Pandas的DF
    pdf = pd.DataFrame({
        "id":[1,2,3],
        "name":["张大仙","王晓晓","王大锤"],
        "age":[11,11,11]
    })
    #将pandas的DF对象转换成Spark的DF
    df = spark.createDataFrame(pdf)
    df.printSchema()
    df.show()
    r"""
    +---+------+---+
    | id|  name|age|
    +---+------+---+
    |  1|张大仙| 11|
    |  2|王晓晓| 11|
    |  3|王大锤| 11|
    +---+------+---+
    """