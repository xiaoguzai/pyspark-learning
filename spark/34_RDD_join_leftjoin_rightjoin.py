from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    # 构建Spark运行环境
    conf = SparkConf().setAppName("create rdd"). \
        setMaster("local[*]")
    sc = SparkContext(conf=conf)

    #部门ID和员工姓名
    x = sc.parallelize([(1001,"zhangsan"),(1002,"lisi"),(1003,"wangwu"),(1004,"zhangLiu")])
    #部门ID和部门名称
    y = sc.parallelize([(1001,"sales"),(1002,"tech")])

    #join为内连接
    print(x.join(y).collect())
    #[(1001, ('zhangsan', 'sales')), (1002, ('lisi', 'tech'))]

    #leftOuterJoin是左外连接，同理还有一个rightOuterJoin右外连接
    print(x.leftOuterJoin(y).collect())
    #[(1001, ('zhangsan', 'sales')), (1002, ('lisi', 'tech')), (1003, ('wangwu', None)), (1004, ('zhangLiu', None))]
    print(x.rightOuterJoin(y).collect())
    #[(1001, ('zhangsan', 'sales')), (1002, ('lisi', 'tech'))]