import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
if __name__ == '__main__':
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    sc = spark.sparkContext

    df = spark.read.format("csv").\
        schema("id INT,subject STRING, score INT").\
        load("../data/sql/stu_score.txt")

    #注册成临时表
    df.createTempView("score") # 注册临时视图(表）
    df.createOrReplaceTempView("score_2") # 注册或者替换临时视图
    df.createGlobalTempView("score_3") # 注册全局临时视图，全局临时视图在使用给的时候
    #需要在前面带上global_temp.前缀

    #可以通过SparkSession对象的sql api来完成sql语句的执行
    spark.sql("SELECT subject,COUNT(*) AS cnt FROM score GROUP BY subject").show()
    r"""
    +-------+---+
    |subject|cnt|
    +-------+---+
    |   英语| 30|
    |   语文| 30|
    |   数学| 30|
    +-------+---+
    """
    spark.sql("SELECT subject,COUNT(*) AS cnt FROM score_2 GROUP BY subject").show()
    r"""
    +-------+---+
    |subject|cnt|
    +-------+---+
    |   英语| 30|
    |   语文| 30|
    |   数学| 30|
    +-------+---+
    """
    spark.sql("SELECT subject,COUNT(*) AS cnt FROM global_temp.score_3 GROUP BY subject").show()
    r"""
    +-------+---+
    |subject|cnt|
    +-------+---+
    |   英语| 30|
    |   语文| 30|
    |   数学| 30|
    +-------+---+
    """