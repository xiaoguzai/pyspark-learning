from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("create rdd"). \
        setMaster("local[*]")
    sc = SparkContext(conf=conf)
    # 部门ID和员工姓名
    x = sc.parallelize([(1001,"zhangsan"),(1002,"lisi"),(1003,"wangwu"),(1004,"zhangliu")])
    # 部门ID和部门名称
    y = sc.parallelize([(1001,"sales"),(1002,"tech")])

    #join为内连接
    print(x.join(y).collect())
    #运行结果[(1001,('zhangsan','sales')),(1002,('lisi','tech'))]

    #leftouterjoin是左外连接，rightouterjoin是右外连接
    print(x.leftOuterJoin(y).collect())
    #运行结果[(1001,('zhangsan','sales')),(1002,('lisi','tech')),
    #(1003,('wangwu',None)),(1004,('zhangliu',None))]

    print(x.rightOuterJoin(y).collect())
    #运行结果[(1001,('zhangsan','sales')),(1002,('lisi','tech'))]
