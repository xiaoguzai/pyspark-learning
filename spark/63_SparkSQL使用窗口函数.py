# coding:utf8
import string

import pandas as pd
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
from pyspark.sql import functions as F
#官方代码的思路是把每一个单词当成一个数据库元素，然后建表，进行统一处理
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType,StringType,IntegerType,ArrayType
if __name__ == '__main__':
    # 0.构建执行环境入口对象SparkSession
    # 0.构建执行环境入口对象SparkSession
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        config("spark.sql.shuffle.partitions",2).\
        getOrCreate()
    sc = spark.sparkContext

    rdd = sc.parallelize([
        ('张三','class_1',99),
        ('王五','class_2',35),
        ('王三','class_3',57),
        ('王久','class_4',12),
        ('王丽','class_5',99),
        ('王娟','class_1',90),
        ('王军','class_2',91),
        ('王俊','class_3',33),
        ('王君','class_4',55),
        ('王珺','class_5',66),
        ('郑颖','class_1',11),
        ('郑辉','class_2',33),
        ('张丽','class_3',36),
        ('张张','class_4',79),
        ('黄开','class_5',90),
        ('黄恺','class_1',90),
        ('王凯','class_3',11),
        ('王凯杰','class_1',11),
        ('王开杰','class_2',3),
        ('王景亮','class_3',99)
    ])

    schema = StructType().add("name",StringType()).\
        add("class",StringType()).\
        add("score",IntegerType())
    df = rdd.toDF(schema)

    # 窗口函数只用于SQL风格, 所以注册表先
    df.createTempView("stu")
    # TODO 聚合窗口
    spark.sql("""
        SELECT *, AVG(score) OVER() AS avg_score FROM stu
    """).show()

    r"""
    本身聚合函数avg是很多条数据聚合成为一条
    由于我们带上了over，所以当前结果可以在
    每一条数据后面都带上avg(score)结果形成新的列
    +------+-------+-----+---------+
    |  name|  class|score|avg_score|
    +------+-------+-----+---------+
    |  张三|class_1|   99|     55.0|
    |  王五|class_2|   35|     55.0|
    |  王三|class_3|   57|     55.0|
    |  王久|class_4|   12|     55.0|
    |  王丽|class_5|   99|     55.0|
    |  王娟|class_1|   90|     55.0|
    |  王军|class_2|   91|     55.0|
    |  王俊|class_3|   33|     55.0|
    |  王君|class_4|   55|     55.0|
    |  王珺|class_5|   66|     55.0|
    |  郑颖|class_1|   11|     55.0|
    |  郑辉|class_2|   33|     55.0|
    |  张丽|class_3|   36|     55.0|
    |  张张|class_4|   79|     55.0|
    |  黄开|class_5|   90|     55.0|
    |  黄恺|class_1|   90|     55.0|
    |  王凯|class_3|   11|     55.0|
    |王凯杰|class_1|   11|     55.0|
    |王开杰|class_2|    3|     55.0|
    |王景亮|class_3|   99|     55.0|
    +------+-------+-----+---------+
    """

    # SELECT *, AVG(score) OVER() AS avg_score FROM stu 等同于
    # SELECT * FROM stu
    # SELECT AVG(score) FROM stu
    # 两个SQL的结果集进行整合而来
    spark.sql("""
        SELECT *, AVG(score) OVER(PARTITION BY class) AS avg_score FROM stu
    """).show()

    r"""
    排序相关的窗口函数计算
    RANK over,DENSE_RANK over,ROW_NUMBER over
    
    +------+-------+-----+------------------+
    |  name|  class|score|         avg_score|
    +------+-------+-----+------------------+
    |  张三|class_1|   99|              60.2|
    |  王娟|class_1|   90|              60.2|
    |  郑颖|class_1|   11|              60.2|
    |  黄恺|class_1|   90|              60.2|
    |王凯杰|class_1|   11|              60.2|
    |  王五|class_2|   35|              40.5|
    |  王军|class_2|   91|              40.5|
    |  郑辉|class_2|   33|              40.5|
    |王开杰|class_2|    3|              40.5|
    |  王三|class_3|   57|              47.2|
    |  王俊|class_3|   33|              47.2|
    |  张丽|class_3|   36|              47.2|
    |  王凯|class_3|   11|              47.2|
    |王景亮|class_3|   99|              47.2|
    |  王久|class_4|   12|48.666666666666664|
    |  王君|class_4|   55|48.666666666666664|
    |  张张|class_4|   79|48.666666666666664|
    |  王丽|class_5|   99|              85.0|
    |  王珺|class_5|   66|              85.0|
    |  黄开|class_5|   90|              85.0|
    +------+-------+-----+------------------+
    """

    # SELECT *, AVG(score) OVER(PARTITION BY class) AS avg_score FROM stu 等同于# SELECT * FROM stu
    # SELECT AVG(score) FROM stu GROUP BY class
    # 两个SQL的结果集进行整合而来
    # TODO 排序窗口
    spark.sql("""
        SELECT *, ROW_NUMBER() OVER(ORDER BY score DESC) AS row_number_rank, 
        DENSE_RANK() OVER(PARTITION BY class ORDER BY score DESC) AS dense_rank, 
        RANK() OVER(ORDER BY score) AS rank
        FROM stu
    """).show()

    r"""
    over(order by score desc) as row_number_rank:整体进行分数排名
    dense_rank() over(partition by class order by score desc):按照班级分区进行排名
    rank() over(order by score) as rank:按照分数进行升序排列
    
    +------+-------+-----+---------------+----------+----+
    |  name|  class|score|row_number_rank|dense_rank|rank|
    +------+-------+-----+---------------+----------+----+
    |  张三|class_1|   99|              1|         1|  18|
    |  王娟|class_1|   90|              5|         2|  14|
    |  黄恺|class_1|   90|              7|         2|  14|
    |  郑颖|class_1|   11|             17|         3|   2|
    |王凯杰|class_1|   11|             19|         3|   2|
    |  王军|class_2|   91|              4|         1|  17|
    |  王五|class_2|   35|             13|         2|   8|
    |  郑辉|class_2|   33|             15|         3|   6|
    |王开杰|class_2|    3|             20|         4|   1|
    |王景亮|class_3|   99|              3|         1|  18|
    |  王三|class_3|   57|             10|         2|  11|
    |  张丽|class_3|   36|             12|         3|   9|
    |  王俊|class_3|   33|             14|         4|   6|
    |  王凯|class_3|   11|             18|         5|   2|
    |  张张|class_4|   79|              8|         1|  13|
    |  王君|class_4|   55|             11|         2|  10|
    |  王久|class_4|   12|             16|         3|   5|
    |  王丽|class_5|   99|              2|         1|  18|
    |  黄开|class_5|   90|              6|         2|  14|
    |  王珺|class_5|   66|              9|         3|  12|
    +------+-------+-----+---------------+----------+----+
    """

    # TODO NTILE
    spark.sql("""
        select *,ntile(6) over(order by score desc) from stu
    """).show()

    r"""
    ntile(6)指的是做了一个6分区，将整个数据划分成了6个分区
    
    +------+-------+-----+-----------------------------------------------------------------------------------------------+
    |  name|  class|score|ntile(6) OVER (ORDER BY score DESC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)|
    +------+-------+-----+-----------------------------------------------------------------------------------------------+
    |  张三|class_1|   99|                                                                                              1|
    |  王丽|class_5|   99|                                                                                              1|
    |王景亮|class_3|   99|                                                                                              1|
    |  王军|class_2|   91|                                                                                              1|
    |  王娟|class_1|   90|                                                                                              2|
    |  黄开|class_5|   90|                                                                                              2|
    |  黄恺|class_1|   90|                                                                                              2|
    |  张张|class_4|   79|                                                                                              2|
    |  王珺|class_5|   66|                                                                                              3|
    |  王三|class_3|   57|                                                                                              3|
    |  王君|class_4|   55|                                                                                              3|
    |  张丽|class_3|   36|                                                                                              4|
    |  王五|class_2|   35|                                                                                              4|
    |  王俊|class_3|   33|                                                                                              4|
    |  郑辉|class_2|   33|                                                                                              5|
    |  王久|class_4|   12|                                                                                              5|
    |  郑颖|class_1|   11|                                                                                              5|
    |  王凯|class_3|   11|                                                                                              6|
    |王凯杰|class_1|   11|                                                                                              6|
    |王开杰|class_2|    3|                                                                                              6|
    +------+-------+-----+-----------------------------------------------------------------------------------------------+
    """
