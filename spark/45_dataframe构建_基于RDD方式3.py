# coding:utf8
# 需求：使用toDF方法将RDD转换为DF
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
if __name__ == '__main__':
    spark = SparkSession.builder.\
        appName("create_df").\
        config("spark.sql.shuffle.partitions","4").\
        getOrCreate()
    #SparkSession对象也可以获取SparkContext
    sc = spark.sparkContext
    #创建DF，首先创建RDD，将RDD转DF
    r"""
    1,语文,99
    2,语文,99
    3,语文,99
    4,语文,99
    5,语文,99
    """
    rdd = sc.textFile("../data/sql/stu_score.txt").\
        map(lambda x:x.split(',')).\
        map(lambda x:(int(x[0]),x[1],int(x[2])))
    #StructType类，这个类可以定义整个DataFrame中的Schema
    schema = StructType().\
        add("id",IntegerType(),nullable=False).\
        add("name",StringType(),nullable=True).\
        add("score",IntegerType(),nullable=False)
    #一个add方法定义一个列的信息，如果有3个列，就写三个add
    #add方法：参数1：列名称，参数2：列类型，参数3：是否允许为空
    #方式1：只传列名，类型靠推断，是否允许为空是true
    df = rdd.toDF(['id','subject','score'])
    df.printSchema()
    df.show()
    #方式2：传入完整的Schema描述对象StructType
    df = rdd.toDF(schema)
    df.printSchema()
    df.show()