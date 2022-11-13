import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
if __name__ == '__main__':
    #parquet是spark中常用的一种列示存储文件格式
    #和hive中的orc差不多，他俩都是列存储格式

    #parquet内置schema：列明、列类型、是否为空
    #存储以列作为存储格式
    #序列化存储在文件中，压缩属性体积小
    #可以通过avro and parquet view查看

    # 0.构建执行环境入口对象SparkSession
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    sc = spark.sparkContext

    df = spark.read.format("csv").\
        schema("id INT,subject STRING, score INT").\
        load("../data/sql/stu_score.txt")

    # Column对象的获取
    id_column = df['id']
    subject_column = df['subject']

    # DSL风格演示
    df.select(["id","subject"]).show()
    r"""
    +---+-------+
    | id|subject|
    +---+-------+
    |  1|   语文|
    |  2|   语文|
    |  3|   语文|
    |  4|   语文|
    |  5|   语文|
    """
    df.select("id","subject").show()
    r"""
    +---+-------+
    | id|subject|
    +---+-------+
    |  1|   语文|
    |  2|   语文|
    |  3|   语文|
    |  4|   语文|
    |  5|   语文|
    """
    df.select(id_column,subject_column).show()
    r"""
    +---+-------+
    | id|subject|
    +---+-------+
    |  1|   语文|
    |  2|   语文|
    |  3|   语文|
    |  4|   语文|
    |  5|   语文|
    """

    #filter API
    df.filter("score < 99").show()
    df.filter(df['score'] < 99).show()
    r"""
    筛选出得分小于99的部分
    | id|subject|score|
    +---+-------+-----+
    |  1|   数学|   96|
    |  2|   数学|   96|
    |  3|   数学|   96|
    |  4|   数学|   96|
    |  5|   数学|   96|
    
    | id|subject|score|
    +---+-------+-----+
    |  1|   数学|   96|
    |  2|   数学|   96|
    |  3|   数学|   96|
    |  4|   数学|   96|
    |  5|   数学|   96|
    """

    #where API
    df.where("score < 99").show()
    df.where(df['score'] < 99).show()
    r"""
    +---+-------+-----+
    | id|subject|score|
    +---+-------+-----+
    |  1|   数学|   96|
    |  2|   数学|   96|
    |  3|   数学|   96|
    |  4|   数学|   96|
    |  5|   数学|   96|
    
    +---+-------+-----+
    | id|subject|score|
    +---+-------+-----+
    |  1|   数学|   96|
    |  2|   数学|   96|
    |  3|   数学|   96|
    |  4|   数学|   96|
    |  5|   数学|   96|
    """

    #group by API
    df.groupby("subject").count().show()
    df.groupby(df['subject']).count().show()
    r"""
    +-------+-----+
    |subject|count|
    +-------+-----+
    |   英语|   30|
    |   语文|   30|
    |   数学|   30|
    +-------+-----+
    
    +-------+-----+
    |subject|count|
    +-------+-----+
    |   英语|   30|
    |   语文|   30|
    |   数学|   30|
    +-------+-----+
    """

    #df.groupBy API的返回值 GroupData
    #GroupData对象不是DataFrame
    #它是一个有分组关系的数据结构，有一些API供我们对分组聚合
    #SQL:group by 后接上聚合：sum avg count min max
    #GroupData类似于SQL分组后的数据结构，同样有上述5种聚合方法
    #GroupData调用聚合方法后，返回值依旧是DataFrame
    #GroupData只是一个中转对象，最终还是获得DataFrame的结果
    r = df.groupby("subject")
    print(type(r))
