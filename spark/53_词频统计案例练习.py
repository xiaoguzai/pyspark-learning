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

    #rdd风格获取词频内容
    rdd = sc.textFile("../data/words.txt").\
        flatMap(lambda x:x.split(' '))
    rdd = rdd.map(lambda x: (x, 1))
    rdd = rdd.reduceByKey(lambda a, b: a + b)
    print(rdd.collect())

    df = rdd.toDF(['word','time'])
    df.printSchema()
    df.show()