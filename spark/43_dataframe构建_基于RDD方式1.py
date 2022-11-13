# coding:utf8
# 演示DataFrame创建的三种方式

from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark = SparkSession.builder.\
        appName("create df").\
        master("local[*]").\
        getOrCreate()

    sc = spark.sparkContext
    #首先构建一个RDD rdd[(name,age),()]
    r"""
    people.txt:
    Michael, 29
    Andy, 30
    Justin, 19
    """
    rdd = sc.textFile("../data/sql/people.txt").\
        map(lambda x:x.split(',')).\
        map(lambda x:[x[0],int(x[1])])
    #这里需要做类型转换，因为类型能够从RDD中探测的到
    #本身是string类型，这里x[1]需要转换为int类型

    #构建DF方式1
    df = spark.createDataFrame(rdd,schema=['name','age'])
    #打印表结构
    r"""
    root
    |-- name: string (nullable = true)
    |-- age: long (nullable = true)
    """
    df.printSchema()
    #打印20行数据
    r"""
    +-------+---+
    |   name|age|
    +-------+---+
    |Michael| 29|
    |   Andy| 30|
    | Justin| 19|
    +-------+---+
    """
    df.show()
    r"""
    +-------+---+
    |   name|age|
    +-------+---+
    |Michael| 29|
    | Justin| 19|
    +-------+---+
    """
    df.createTempView("ttt")
    spark.sql("select * from ttt where age<30").show()
