from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':
    #构建SparkConf对象
    conf = SparkConf().setAppName("helloworld").setMaster("local[*]")
    #构建SparkContext执行环境入口对象
    sc = SparkContext(conf = conf)

    file_rdd = sc.textFile("../data/accumulator_broadcast_data.txt")
    split_rdd = file_rdd.map(lambda x: x.split("\t"))
    print('###split_rdd = ###')
    print(split_rdd.collect())
    print('##################')
    rdd = sc.parallelize(split_rdd.collect(), 2)

    acmlt = sc.accumulator(0)
    def map_func(data):
        global acmlt
        abnormal_char = [",",".","!","#","$","%"]
        print('data = ')
        print(data)
        for data1 in data:
            for data2 in data1:
                print('data2 = ')
                print(data2)
                if data2 in abnormal_char:
                    acmlt += 1
        print('acmlt = ')
        print(acmlt)
        print('========')

    print('acmlt = ')
    print(acmlt)
    rdd.map(map_func).collect()
    print(acmlt)

