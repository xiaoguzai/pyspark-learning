import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
from pyspark.sql import functions as F
#官方代码的思路是把每一个单词当成一个数据库元素，然后建表，进行统一处理
if __name__ == '__main__':
    # 0.构建执行环境入口对象SparkSession
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    sc = spark.sparkContext

    #方法1：以RDD为基础数据加载
    rdd = sc.textFile("../data/words.txt").\
        flatMap(lambda x:x.split(" ")).\
        map(lambda x:[x])
    #转换RDD到df
    df = rdd.toDF(["word"])
    #注册df为表格
    df.createTempView("words")
    #使用sql语句处理df注册表
    spark.sql("""
        select word,count(*) as cnt from words group by 
        word order by cnt desc
    """).show()

    #方法2：纯sparksql api进行数据加载
    df = spark.read.format("text").load("../data/words.txt")
    r"""
    得到的数据表
    +--------------------+
    |               value|
    +--------------------+
    | hadoop hadoop spark|
    | hadoop hadoop spark|
    |hadoop hadoop had...|
    +--------------------+
    """
    df2 = df.withColumn("value",F.explode(F.split(df["value"]," ")))
    #通过withColumn对一个列进行操作
    #对每个value，使用空格切分，然后重新定义数据表名称为value

    df2.groupBy("value").\
        count().\
        withColumnRenamed("count","cnt").\
        orderBy("cnt",ascending=False).\
        show()
    #按照"value"进行划分统计，将"count"改名为"cnt"，
    #并且按照"cnt"升序排列