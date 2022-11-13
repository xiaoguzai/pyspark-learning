# coding:utf-8
from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("create rdd"). \
        setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a',1),('E',1),('C',1),('D',1),('b',1),
                          ('g',1),('f',1),('y',1),('u',1),('i',1),
                          ('o',1),('p',1),('m',1),('n',1),('j',1),
                          ('k',1),('l',1)],3)

    print(rdd.sortByKey().collect()) #默认按照key进行升序排序
    #[('C', 1), ('D', 1), ('E', 1), ('a', 1), ('b', 1), ('f', 1), ('g', 1), ('i', 1), ('j', 1), ('k', 1), ('l', 1), ('m', 1), ('n', 1), ('o', 1), ('p', 1), ('u', 1), ('y', 1)]

    # 如果要确保全局有序，排序分区数要给1，不是1的话只能确保各个分区排好序，整体不保证
    print(rdd.sortByKey(ascending=False,numPartitions=5).collect())
    #[('y', 1), ('u', 1), ('p', 1), ('o', 1), ('n', 1), ('m', 1), ('l', 1), ('k', 1), ('j', 1), ('i', 1), ('g', 1), ('f', 1), ('b', 1), ('a', 1), ('E', 1), ('D', 1), ('C', 1)]

    # 对排序的key进行处理，在排序前处理一下key，让key以你处理的样子进行排序
    print(rdd.sortByKey(ascending=True,numPartitions=1,keyfunc=lambda key:str(key).lower()).collect())
    #[('a', 1), ('b', 1), ('C', 1), ('D', 1), ('E', 1), ('f', 1), ('g', 1), ('i', 1), ('j', 1), ('k', 1), ('l', 1), ('m', 1), ('n', 1), ('o', 1), ('p', 1), ('u', 1), ('y', 1)]
