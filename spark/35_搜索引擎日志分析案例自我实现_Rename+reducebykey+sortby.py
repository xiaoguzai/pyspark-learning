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
from pyspark import SparkConf,SparkContext
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':
    # 构建Spark运行环境
    conf = SparkConf().setAppName("create rdd")
    sc = SparkContext(conf=conf)

    conf.set("spark.submit.pyFiles", "defs_35.py")

    file_rdd = sc.textFile("hdfs://node1:8020/input/SogouQ.txt")
    file_rdd = file_rdd.map(lambda line: line.split(" "))
    def getstring(data):
        return data[0].split("\t")
    file_rdd = file_rdd.map(getstring)

    #需要反复运行的内容放到内存中去
    file_rdd.persist(StorageLevel.DISK_ONLY)

    result1 = file_rdd.map(lambda x:(x[2],1))
    result1 = result1.reduceByKey(lambda a,b:a+b)

    #对于上述代码，构建yarn的命令
    #/export/server/spark/bin/spark-submit --master yarn /tmp/pycharm_project_570/01_RDD/35_搜索引擎日志分析案例自我实现_Rename+reducebykey+sortby.py



    result1 = result1.reduceByKey(lambda a,b:a+b). \
        sortBy(lambda x:x[1],ascending=False, numPartitions=1). \
        take(5)

    print("任务1：中文关键词的数量为：")
    print(result1)
    #任务1：中文关键词的数量top5为：
    #[('scala', 2310), ('hadoop', 2268), ('博学谷', 2002), ('传智汇', 1918), ('itheima', 1680)]


    rdd2 = file_rdd.map(lambda x:(x[1]+"_"+x[2],1))
    result2 = rdd2.reduceByKey(lambda a,b:a+b). \
        sortBy(lambda x:x[1],ascending=False,numPartitions=1). \
        take(5)
    print("任务2：id+关键词的数量为：")
    print(result2)

    # 访问时间[0]，分组、统计、排序
    rdd3 = file_rdd.map(lambda x:(x[0],1))
    result3 = rdd3.reduceByKey(lambda a,b:a+b)
    result3 = result3.reduceByKey(lambda a,b:a+b). \
        sortBy(lambda x:x[1],ascending=False,numPartitions=1). \
        take(5)
    print('任务3：访问时间的结果为：')
    print(result3)

    r"""
    运行我方程序
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
    
    运行命令
    /export/server/spark/bin/spark-submit --master yarn --executor-memory 4g --executor-cores 4 --num-executors 3 /tmp/pycharm_project_570/01_RDD/35_搜索引擎日志分析案例自我实现_Rename+reducebykey+sortby.py
    """