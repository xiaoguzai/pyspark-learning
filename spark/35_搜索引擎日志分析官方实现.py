# coding:utf8

# 导入Spark的相关包
import time

from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel
from defs_35 import context_jieba, filter_words, append_words, extract_user_and_word
from operator import add
r"""
00:00:00	2982199073774412	传智播客	8	3	http://www.itcast.cn
00:00:00	07594220010824798	黑马程序员	1	1	http://www.itcast.cn
00:00:00	5228056822071097	传智播客	14	5	http://www.itcast.cn
00:00:00	6140463203615646	博学谷	62	36	http://www.itcast.cn
任务1：查询词，结巴分词
任务2：字段：用户ID和查询词，分组、统计
用户id：[1],查询词:[2]
任务3：字段：访问时间，分组、统计、排序
"""
if __name__ == '__main__':
    # 0. 初始化执行环境 构建SparkContext对象
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 1. 读取数据文件
    file_rdd = sc.textFile("hdfs://node1:8020/input/SogouQ.txt")

    # 2. 对数据进行切分 \t
    split_rdd = file_rdd.map(lambda x: x.split("\t"))

    # 3. 因为要做多个需求, split_rdd 作为基础的rdd 会被多次使用.
    # 多次使用想到使用缓存或者checkpoint来解决
    # 这里想到简单的，使用缓存来解决
    split_rdd.persist(StorageLevel.DISK_ONLY)

    #print(split_rdd.collect())
    r"""
    split_rdd = 
    [
     ['20:00:07', '5750662932822584', '酷丁鱼', '3', '2', 'http://www.itcast.cn'], 
     ['20:00:07', '11515839301781111', 'spark', '4', '3', 'http://www.itcast.cn'],
     ......
    ]
    """

    # TODO: 需求1: 用户搜索的关键`词`分析
    # 主要分析热点词
    # 将所有的搜索内容取出
    # print(split_rdd.takeSample(True, 3))
    context_rdd = split_rdd.map(lambda x: x[2])

    # 对搜索的内容进行分词分析
    words_rdd = context_rdd.flatMap(context_jieba)
    r"""
    def context_jieba(data):
        seg = jieba.cut_for_search(data)
        l = list()
        for word in seg:
            l.append(word)
        return l
    """
    r"""
    ['传智播', '客', '黑马', '程序', '程序员', '传智播', '客', '博学', 
     ......
     '语言', 'IDEA', 'itheima', '博学', '谷', 'bigdata', 'DataLake']
    """

    # print(words_rdd.collect())
    # 院校 帮 -> 院校帮
    # 博学 谷 -> 博学谷
    # 传智播 客 -> 传智播客
    filtered_rdd = words_rdd.filter(filter_words)
    # 将关键词转换: 穿直播 -> 传智播客
    # ['传智播', '黑马', '程序', '程序员', '传智播', '博学',
    # ......
    final_words_rdd = filtered_rdd.map(append_words)
    # 对单词进行 分组 聚合 排序 求出前5名
    result1 = final_words_rdd.reduceByKey(lambda a, b: a + b).\
        sortBy(lambda x: x[1], ascending=False, numPartitions=1).\
        take(5)

    print("需求1结果: ", result1)
    #需求1结果:  [('scala', 2310), ('hadoop', 2268), ('博学谷', 2002), ('传智汇', 1918), ('itheima', 1680)]

    # TODO: 需求2: 用户和关键词组合分析
    # 1, 我喜欢传智播客
    # 1+我  1+喜欢 1+传智播客
    user_content_rdd = split_rdd.map(lambda x: (x[1], x[2]))
    # 对用户的搜索内容进行分词, 分词后和用户ID再次组合
    user_word_with_one_rdd = user_content_rdd.flatMap(extract_user_and_word)
    # 对内容进行 分组 聚合 排序 求前5
    # 返回的user_word_with_one_rdd =
    result2 = user_word_with_one_rdd.reduceByKey(lambda a, b: a + b).\
        sortBy(lambda x: x[1], ascending=False, numPartitions=1).\
        take(5)

    print("需求2结果: ", result2)
    #需求2结果:  [('6185822016522959_scala', 2016), ('41641664258866384_博学谷', 1372), ('44801909258572364_hadoop', 1260), ('7044693659960919_数据', 1120), ('7044693659960919_仓库', 1120)]
    r"""
    user_word_with_one_rdd = 
    [('2982199073774412_传智播客', 1), ('07594220010824798_黑马', 1), ('07594220010824798_程序', 1),
     ......
     ('09826806962227619_IDEA', 1), ('6625608472079179_itheima', 1), ('7954902679225404_博学谷', 1), ('9717831746543397_bigdata', 1), ('6248455356965147_DataLake', 1)]
    """
    # TODO: 需求3: 热门搜索时间段分析
    # 取出来所有的时间
    time_rdd = split_rdd.map(lambda x: x[0])
    # 对时间进行处理, 只保留小时精度即可
    hour_with_one_rdd = time_rdd.map(lambda x: (x.split(":")[0], 1))
    # 分组 聚合 排序
    result3 = hour_with_one_rdd.reduceByKey(add).\
        sortBy(lambda x: x[1], ascending=False, numPartitions=1).\
        collect()

    print("需求3结果: ", result3)

    #官方提交运行指令  /export/server/spark/bin/spark-submit --master yarn --py-files /tmp/pycharm_project_570/01_RDD/defs_35.py /tmp/pycharm_project_570/01_RDD/35_搜索引擎日志分析官方实现.py
    #这里必须要指定--py-files
    #如果指定内核等内容
    r"""
    运行命令
    /export/server/spark/bin/spark-submit --master yarn --py-files /tmp/pycharm_project_570/01_RDD/defs_35.py --executor-memory 4g --executor-cores 4 --num-executors 3 /tmp/pycharm_project_570/01_RDD/35_搜索引擎日志分析官方实现.py
    首先使用cat /proc/cpuinfo查看有几个虚拟器，cpu之中列出了对应的虚拟器
        processor       : 0
    vendor_id       : AuthenticAMD
    cpu family      : 25
    model           : 80
    model name      : AMD Ryzen 7 5800H with Radeon Graphics
    stepping        : 0
    microcode       : 0xa50000c
    cpu MHz         : 3194.002
    cache size      : 512 KB
    physical id     : 0
    siblings        : 2
    core id         : 0
    cpu cores       : 2
    apicid          : 0
    initial apicid  : 0
    fpu             : yes
    fpu_exception   : yes
    cpuid level     : 16
    wp              : yes
    flags           : fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx fxsr sse sse2 ht syscall nx mmxext fxsr_opt pdpe1gb rdtscp lm constant_tsc art rep_good nopl tsc_reliable nonstop_tsc extd_apicid eagerfpu pni pclmulqdq ssse3 fma cx16 sse4_1 sse4_2 x2apic movbe popcnt aes xsave avx f16c rdrand hypervisor lahf_lm cmp_legacy extapic cr8_legacy abm sse4a misalignsse 3dnowprefetch osvw retpoline_amd ibpb vmmcall fsgsbase bmi1 avx2 smep bmi2 erms invpcid rdseed adx smap clflushopt clwb sha_ni xsaveopt xsavec clzero arat pku ospke overflow_recov succor
    bogomips        : 6388.00
    TLB size        : 2560 4K pages
    clflush size    : 64
    cache_alignment : 64
    address sizes   : 43 bits physical, 48 bits virtual
    power management:
    
    processor       : 1
    vendor_id       : AuthenticAMD
    cpu family      : 25
    model           : 80
    model name      : AMD Ryzen 7 5800H with Radeon Graphics
    stepping        : 0
    microcode       : 0xa50000c
    cpu MHz         : 3194.002
    cache size      : 512 KB
    physical id     : 0
    siblings        : 2
    core id         : 1
    cpu cores       : 2
    apicid          : 1
    initial apicid  : 1
    fpu             : yes
    fpu_exception   : yes
    cpuid level     : 16
    wp              : yes
    flags           : fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx fxsr sse sse2 ht syscall nx mmxext fxsr_opt pdpe1gb rdtscp lm constant_tsc art rep_good nopl tsc_reliable nonstop_tsc extd_apicid eagerfpu pni pclmulqdq ssse3 fma cx16 sse4_1 sse4_2 x2apic movbe popcnt aes xsave avx f16c rdrand hypervisor lahf_lm cmp_legacy extapic cr8_legacy abm sse4a misalignsse 3dnowprefetch osvw retpoline_amd ibpb vmmcall fsgsbase bmi1 avx2 smep bmi2 erms invpcid rdseed adx smap clflushopt clwb sha_ni xsaveopt xsavec clzero arat pku ospke overflow_recov succor
    bogomips        : 6388.00
    TLB size        : 2560 4K pages
    clflush size    : 64
    cache_alignment : 64
    address sizes   : 43 bits physical, 48 bits virtual
    power management:
    
    processor       : 2
    vendor_id       : AuthenticAMD
    cpu family      : 25
    model           : 80
    model name      : AMD Ryzen 7 5800H with Radeon Graphics
    stepping        : 0
    microcode       : 0xa50000c
    cpu MHz         : 3194.002
    cache size      : 512 KB
    physical id     : 1
    siblings        : 2
    core id         : 0
    cpu cores       : 2
    apicid          : 2
    initial apicid  : 2
    fpu             : yes
    fpu_exception   : yes
    cpuid level     : 16
    wp              : yes
    flags           : fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx fxsr sse sse2 ht syscall nx mmxext fxsr_opt pdpe1gb rdtscp lm constant_tsc art rep_good nopl tsc_reliable nonstop_tsc extd_apicid eagerfpu pni pclmulqdq ssse3 fma cx16 sse4_1 sse4_2 x2apic movbe popcnt aes xsave avx f16c rdrand hypervisor lahf_lm cmp_legacy extapic cr8_legacy abm sse4a misalignsse 3dnowprefetch osvw retpoline_amd ibpb vmmcall fsgsbase bmi1 avx2 smep bmi2 erms invpcid rdseed adx smap clflushopt clwb sha_ni xsaveopt xsavec clzero arat pku ospke overflow_recov succor
    bogomips        : 6388.00
    TLB size        : 2560 4K pages
    clflush size    : 64
    cache_alignment : 64
    address sizes   : 43 bits physical, 48 bits virtual
    power management:
    
    processor       : 3
    vendor_id       : AuthenticAMD
    cpu family      : 25
    model           : 80
    model name      : AMD Ryzen 7 5800H with Radeon Graphics
    stepping        : 0
    microcode       : 0xa50000c
    cpu MHz         : 3194.002
    cache size      : 512 KB
    physical id     : 1
    siblings        : 2
    core id         : 1
    cpu cores       : 2
    apicid          : 3
    initial apicid  : 3
    fpu             : yes
    fpu_exception   : yes
    cpuid level     : 16
    wp              : yes
    flags           : fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx fxsr sse sse2 ht syscall nx mmxext fxsr_opt pdpe1gb rdtscp lm constant_tsc art rep_good nopl tsc_reliable nonstop_tsc extd_apicid eagerfpu pni pclmulqdq ssse3 fma cx16 sse4_1 sse4_2 x2apic movbe popcnt aes xsave avx f16c rdrand hypervisor lahf_lm cmp_legacy extapic cr8_legacy abm sse4a misalignsse 3dnowprefetch osvw retpoline_amd ibpb vmmcall fsgsbase bmi1 avx2 smep bmi2 erms invpcid rdseed adx smap clflushopt clwb sha_ni xsaveopt xsavec clzero arat pku ospke overflow_recov succor
    bogomips        : 6388.00
    TLB size        : 2560 4K pages
    clflush size    : 64
    cache_alignment : 64
    address sizes   : 43 bits physical, 48 bits virtual
    power management:

    
    这里能够看出总共有4个虚拟器
    接下来使用free -g查看内存，total为对应内存
     free -g
                    total        used        free      shared  buff/cache   available
    Mem:              7           3           2           0           1           4
    Swap:             3           0           3

    """