# coding:utf8
import time

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
from pyspark.sql import functions as F
#官方代码的思路是把每一个单词当成一个数据库元素，然后建表，进行统一处理
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
if __name__ == '__main__':
    # 0.构建执行环境入口对象SparkSession
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]"). \
        config("spark.sql.shuffle.partitions", 1).\
        getOrCreate()
    sc = spark.sparkContext

    #rdd风格获取词频内容
    rdd = sc.textFile("../data/sql/u.data").\
        map(lambda x:x.split('\t')).\
        map(lambda x:(int(x[0]),int(x[1]),int(x[2]),int(x[3])))
    #print(rdd.collect())

    schema = StructType().\
        add("id",IntegerType(),nullable=False).\
        add("movie",IntegerType(),nullable=False).\
        add("score",IntegerType(),nullable=False).\
        add("time",IntegerType(),nullable=False)

    df = spark.createDataFrame(rdd,schema)
    df.printSchema()
    df.show()
    r"""
    +---+-----+-----+---------+
    | id|movie|score|     time|
    +---+-----+-----+---------+
    |196|  242|    3|881250949|
    |186|  302|    3|891717742|
    | 22|  377|    1|878887116|
    """

    r"""
    需求：
    1.查询用户平均分
    2.查询电影平均分
    3.查询大于平均分的电影的数量
    4.查询高分电影中(>3)打分次数最多的用户，并求出此人打的平均分
    5.查询每个用户的平均打分、最低打分、最高打分
    6.查询被评分超过100次的电影的平均分排名TOP10
    """

    df.createTempView("table")

    #SQL风格
    #1.查询用户平均分
    print("1.查询用户平均分")
    spark.sql("""
        select id,avg(score) from table group by id
    """).show()

    #2.查询电影平均分
    print("2.查询电影平均分")
    spark.sql("""
        select movie,avg(score) from table group by movie
    """).show()

    print("3.查询所有电影平均分")
    spark.sql("""
        select avg(score) from table
    """).show()

    #3.查询大于平均分的电影的数量
    print("4.查询大于平均分的电影的数量")
    spark.sql("""
        select movie,count(movie) from table group by movie
        having avg(score) > (select avg(score) from table)
    """).show()

    #4.查询高分电影中( > 3)打分次数最多的用户，并求出此人打的平均分

    print("5.查询高分电影>3打分次数最多的用户，并求出此人打的平均分")
    #synax error发现是换行没有加上\的符号
    r"""
    pyspark.sql.utils.AnalysisException: grouping expressions sequence is empty, 
    and 'table.id' is not an aggregate function. Wrap '(avg(table.score) AS `avg(score)`)' 
    in windowing function(s) or wrap 'table.id' in first() (or first_value) 
    if you don't care which value you get.;
    group by有where的写在where后面，有having的写在having前面
    """

    #spark.sql("""
    #    select id,avg(score) from table \
    #    where id in \
    #    (select id from table \
    #    where movie in \
    #    (select movie from table group by movie \
    #    having avg(score) > 3))
    #    group by id \
    #""").show()
    #!!!这里如果返回多个的话，不能写成movie等于，而应该写成movie in
    #Syntax error at or near 'where'(line 2, pos 52)
    #这个报错是由于in后面没有加入括号
    #!!!注意，有where的写在group后面，有having的写在group by前面

    spark.sql("""
        select id,avg(score) from table \
        where movie in \
        (select movie from table group by movie \
        having avg(score) > 3) \
        group by id \
    """).show()
    #有些嵌套可以用连接替换，有些嵌套不能用连接替换



    #5.查询每个用户的平均打分、最高打分、最低打分
    print("6.查询每个用户的平均打分、最高打分、最低打分")
    spark.sql("""
        select id,avg(score),max(score),min(score) from table \
        group by id \
    """).show()

    #6.查询被评分超过100次的电影的平均分排名top10
    time.sleep(10000)
