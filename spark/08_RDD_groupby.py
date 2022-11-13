from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("create rdd"). \
        setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1,2,3,4,5])

    #分组，将数字分层，偶数和技术2个组
    rdd2 = rdd.groupBy(lambda num: 'even' if (num%2 == 0) else 'odd')

    #将rdd2的元素的value转换成list，这样可以输出内容
    print(rdd2.map(lambda x:(x[0],list(x[1]))).collect())
    #运行结果 [('even',[2,4]),('odd',[1,3,5])]