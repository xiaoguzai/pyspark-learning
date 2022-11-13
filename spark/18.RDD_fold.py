from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("create rdd"). \
        setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(range(1,10),3)
    print(rdd.fold(10,lambda a,b:a+b))
    #分区1：123聚合带上10得到16
    #分区2：456聚合带上10得到25
    #分区3：789聚合带上10得到34
    #3个分区结果聚合带上10得到10+16+25+34=85