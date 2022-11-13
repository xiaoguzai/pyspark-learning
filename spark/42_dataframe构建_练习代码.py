# coding:utf8
# SparkSQL的入口对象是SparkSession对象
from pyspark.sql import SparkSession
if __name__ == '__main__':
    #构建SparkSession对象，这个对象是构建器模式，通过builder方法来构建
    spark = SparkSession.builder.\
        appName("local[*]").\
        config("spark.sql.shuffle.partitions","4").\
        getOrCreate()
    #appName设置程序名称，config设置一些常用属性
    #最后通过getOrCreate()方法创建SparkSession()对象

    r"""
    stu_score.txt中的内容
    1,语文,99  2,语文,99
    3,语文,99  4,语文,99
    5,语文,99  6,语文,99
    7,语文,99  8,语文,99
    9,语文,99  10,语文,99
    """
    df = spark.read.csv('../data/sql/stu_score.txt',sep=',',header=False)
    df2 = df.toDF('id','name','score')
    df2.printSchema()
    r"""
    root
    |-- id: string (nullable = true)
    |-- name: string (nullable = true)
    |-- score: string (nullable = true)
    """
    df2.show()
    r"""
    +---+----+-----+
    | id|name|score|
    +---+----+-----+
    |  1|语文|   99|
    |  2|语文|   99|
    |  3|语文|   99|
    |  4|语文|   99|
    |  5|语文|   99|
    |  6|语文|   99|
    """
    df2.createTempView("score")

    #SQL风格
    spark.sql("""
        SELECT * FROM score WHERE name='语文' LIMIT 5
    """).show()
    r"""
    +---+----+-----+
    | id|name|score|
    +---+----+-----+
    |  1|语文|   99|
    |  2|语文|   99|
    |  3|语文|   99|
    |  4|语文|   99|
    |  5|语文|   99|
    +---+----+-----+
    """
