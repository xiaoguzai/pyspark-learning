# coding:utf8
# SparkSQL中的入口是SparkSession对象
from pyspark.sql import SparkSession

if __name__ == '__main__':
    #构建SparkSession对象，这个对象是构建器模式通过builder方法来构建的
    spark = SparkSession.builder.\
        appName("local[*]").\
        config("spark.sql.shuffle.partitions","4").\
        getOrCreate()
    #appName设置程序名称，config设置一些常用属性
    #最后通过getOrCreate()方法创建SparkSession对象

    df = spark.read.csv('../data/sql/stu_score.txt',sep=',',header=False)
    df2 = df.toDF('id','name','score')
    print("df2Schema = ")
    df2.printSchema()
    print("df2show = ")
    df2.show()
    df2.createTempView("score")

    print("df2sql = ")
    #SQL风格
    spark.sql("""
        SELECT * FROM score WHERE name='语文' LIMIT 5
    """).show()

