# coding:utf8
# 需求：基于StructType的方式构建DataFrame 同样是RDD转DF
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
if __name__ == '__main__':
    spark = SparkSession.builder.\
        appName("create_df").\
        config("spark.sql.shuffle.partitions","4").\
        getOrCreate()
    #SparkSession对象也可以获取SparkContext
    sc = spark.sparkContext
    #创建DF，首先创建RDD将RDD转DF
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
    r"""
    root
     |-- id: integer (nullable = false)
     |-- name: string (nullable = true)
     |-- score: integer (nullable = false)
    """

    #StructType类
    #这个类可以定义整个DataFrame中的Schema
    schema = StructType().\
        add("id",IntegerType(),nullable=False).\
        add("name",StringType(),nullable=True).\
        add("score",IntegerType(),nullable=False)
    #一个add方法定义一个列的信息，如果有3个列，就写三个add
    #add方法：参数1：列名称，参数2：列类型，参数3：是否允许为空
    df = spark.createDataFrame(rdd,schema)
    df.printSchema()
    df.show()
    r"""
    +---+----+-----+
    | id|name|score|
    +---+----+-----+
    |  1|语文|   99|
    |  2|语文|   99|
    |  3|语文|   99|
    |  4|语文|   99|
    |  5|语文|   99|
    """
