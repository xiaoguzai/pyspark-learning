# Spark Local环境部署

## 下载地址

https://dlcdn.apache.org/spark/spark-3.2.0/spark-3.2.0-bin-hadoop3.2.tgz

## 错误排查-ping不通百度

发现虚拟网络编辑器之中的nat模式子网ip为192.168.198.0，其实node1为192.168.88.151,node2为192.168.88.152,node3为192.168.88.153，所以这里的子网ip应该为192.168.88.0

## 问题：finalshell连接经常掉线

在连接的过程中将加速那个选项取消掉即可

![捕获](C:\Users\47455\Pictures\捕获.PNG)

出现这个问题主要不是finalshell的问题，主要问题来自于内存分配的太大的问题，此时我将每一个虚拟机的内存

分配为3g。

## 问题：zookeeper启动不起来

报错内容为

Unable to start AdminServer, exiting abnormally
org.apache.zookeeper.server.admin.AdminServer$AdminServerException: Problem starting AdminServer on address 0.0.0.0, port 8080 and command URL /commands

最主要原因为8080占用了端口，此时我们首先查看原先8080端口的进程

```
netstat -alnp | grep 8080
```

显示出下面的内容

```
tcp6       0      0 :::8080                 :::*                    LISTEN      2877/java  
```

接下来我们需要将对应的进程去除

```
kill -9 2877
```

然后就能够成功启动新的进程了

**特别注意，如果zookeeper占用了8080端口，此时spark就不再是8080端口了，可能是8081、8082等相关端口，又因为worker的端口为8081，所以此时spark就不是8081，如果先启动spark再启动zookeeper的时候，zookeeper就会报错**

如果不想发生上述的情况，就需要修改相应的端口，在zoo.cfg中增加admin.serverPort=没有被占用的端口号（比如2284）

**先运行的抢占为资源机，不一定管理的一定为node1，抢占的那个机子为alive，其余的机子为standby**

## 条件


- PYTHON 推荐3.8
- JDK 1.8

(每个机器上都安装)

中途突然退出，在windows上使用chkdsk e:/f对整个e盘进行修复

遇到紧急模式了采用https://blog.csdn.net/whandgdh/article/details/103302516

## 0.修改主机名称

```
nmcli general hostname node1
```

将主机名称修改为node1，然后使用语法

```
vim /etc/hosts
```

修改对应ip地址信息（修改主机名之后这个文件可能被重置）

```
192.168.88.151 node1 node1.itcast.cn
192.168.88.152 node2 node2.itcast.cn
192.168.88.153 node3 node3.itcast.cn
```

这里如果ip地址没修改过来，再进一步修改配置文件

```
vim /etc/sysconfig/network-scripts/ifcfg-ens33
```

将其中的ip地址内容修改，然后重启

如果出现node3.itcast.cn:Permission denied(publickey,password)

说明node2无法连接到node3，对ndoe2和node3进行一次ssh连接

```
root@node2:~# ssh root@node3
root@node3's password: 
```



## 1.Anaconda On Linux 安装
本次课程的Python环境需要安装到Linux(虚拟机)和Windows(本机)上

参见最下方, 附: Anaconda On Linux 安装

**linux下面安装python3可以效仿这篇文章内容https://blog.csdn.net/weixin_41887201/article/details/124206113**

安装python的命令

[root@yuer ~]# wget https://www.python.org/ftp/python/3.9.1/Python-3.9.1.tgz

如果采用拖动文件的方式可能会缺失某些文件

## 2.解压

解压下载的Spark安装包

`tar -zxvf spark-3.2.0-bin-hadoop3.2.tgz -C /export/server/`

**解压完成之后，如果没有python环境，需要先安装相应的python环境**

## 环境变量


配置Spark由如下5个环境变量需要设置


-  SPARK_HOME: 表示Spark安装路径在哪里 
-  PYSPARK_PYTHON: 表示Spark想运行Python程序, 那么去哪里找python执行器 
-  JAVA_HOME: 告知Spark Java在哪里 
-  HADOOP_CONF_DIR: 告知Spark Hadoop的配置文件在哪里 
-  HADOOP_HOME: 告知Spark  Hadoop安装在哪里 



这5个环境变量 都需要配置在: `/etc/profile`中
​

![](https://pybd.yuque.com/api/filetransfer/images?url=https%3A%2F%2Fimage-set.oss-cn-zhangjiakou.aliyuncs.com%2Fimg-out%2F2021%2F09%2F08%2F20210908113758.png&sign=f8e5567e81c1b2c53b185d5b92deaaf1c0aed10141ccbb306d5d5f2cca37094e#from=url&id=L732v&margin=%5Bobject%20Object%5D&originHeight=476&originWidth=1248&originalType=binary&ratio=1&status=done&style=none)

PYSPARK_PYTHON和 JAVA_HOME 需要同样配置在: `~/.bashrc`中

vim ~/.bashrc


![](https://pybd.yuque.com/api/filetransfer/images?url=https%3A%2F%2Fimage-set.oss-cn-zhangjiakou.aliyuncs.com%2Fimg-out%2F2021%2F09%2F08%2F20210908113857.png&sign=579c959d0f5219e564d04439b5f4b025838305b36f7c0cebed7fbeb0c26e9422#from=url&id=IAEoX&margin=%5Bobject%20Object%5D&originHeight=56&originWidth=1124&originalType=binary&ratio=1&status=done&style=none)
## 上传Spark安装包


资料中提供了: `spark-3.2.0-bin-hadoop3.2.tgz`


上传这个文件到Linux服务器中


将其解压, 课程中将其解压(安装)到: `/export/server`内.

`tar -zxvf spark-3.2.0-bin-hadoop3.2.tgz -C /export/server/`

**之前代码报错，重装一下上面的这部分就好了**


由于spark目录名称很长, 给其一个软链接:

`ln -s /export/server/spark-3.2.0-bin-hadoop3.2 /export/server/spark`
**注意给hadoop建立软链接，否则就开启的时候会报错node1: no org.apache.spark.deploy.worker.Worker to stop**

![](https://pybd.yuque.com/api/filetransfer/images?url=https%3A%2F%2Fimage-set.oss-cn-zhangjiakou.aliyuncs.com%2Fimg-out%2F2021%2F09%2F08%2F20210908114221.png&sign=b8a1d0142e7c6954bf184f4d550edb1c31952122d3f000d010602fb0ec361f4c#from=url&id=wlN2r&margin=%5Bobject%20Object%5D&originHeight=342&originWidth=1046&originalType=binary&ratio=1&status=done&style=none)
![](https://pybd.yuque.com/api/filetransfer/images?url=https%3A%2F%2Fimage-set.oss-cn-zhangjiakou.aliyuncs.com%2Fimg-out%2F2021%2F09%2F08%2F20210908114508.png&sign=e4cc72b979e68e5947d8b49becb00f3b1b27ed47c9b234f6f0b8aff5e142d17d#from=url&id=U8FDC&margin=%5Bobject%20Object%5D&originHeight=457&originWidth=1113&originalType=binary&ratio=1&status=done&style=none)


## 测试


### bin/pyspark

bin/pyspark 程序, 可以提供一个  `交互式`的 Python解释器环境, 在这里面可以写普通python代码, 以及spark代码

这里测试bin/pyspark的时候需要先安装pyspark conda install pyspark，注意更换国内镜像源，不然太慢

![](https://pybd.yuque.com/api/filetransfer/images?url=https%3A%2F%2Fimage-set.oss-cn-zhangjiakou.aliyuncs.com%2Fimg-out%2F2021%2F09%2F08%2F20210908114547.png&sign=6751b93b4de12d40dfaa10e1471d2be2bd1a87c163a2fbefb2ef9c3722aff966#from=url&id=S8pd5&margin=%5Bobject%20Object%5D&originHeight=586&originWidth=1403&originalType=binary&ratio=1&status=done&style=none)


如图:


![](https://pybd.yuque.com/api/filetransfer/images?url=https%3A%2F%2Fimage-set.oss-cn-zhangjiakou.aliyuncs.com%2Fimg-out%2F2021%2F09%2F08%2F20210908114715.png&sign=5bda42b3ece8477fa034919b3e03b43cac1b67c70464bb36c70123426dca3391#from=url&id=WEXdN&margin=%5Bobject%20Object%5D&originHeight=87&originWidth=1006&originalType=binary&ratio=1&status=done&style=none)


在这个环境内, 可以运行spark代码


图中的: `parallelize` 和 `map` 都是spark提供的API


`sc.parallelize([1,2,3,4,5]).map(lambda x: x + 1).collect()`
​

### WEB UI (4040)


Spark程序在运行的时候, 会绑定到机器的`4040`端口上.

如果4040端口被占用, 会顺延到4041 ... 4042...
![](https://pybd.yuque.com/api/filetransfer/images?url=https%3A%2F%2Fimage-set.oss-cn-zhangjiakou.aliyuncs.com%2Fimg-out%2F2021%2F09%2F08%2F20210908115158.png&sign=8effd3fa6e7a3fcf74fa9fe4700307b83d6707510b98e84a2567e0272ded7433#from=url&id=Vk8If&margin=%5Bobject%20Object%5D&originHeight=243&originWidth=1449&originalType=binary&ratio=1&status=done&style=none)


4040端口是一个WEBUI端口, 可以在浏览器内打开:

输入:`服务器ip:4040` 即可打开:
![](https://pybd.yuque.com/api/filetransfer/images?url=https%3A%2F%2Fimage-set.oss-cn-zhangjiakou.aliyuncs.com%2Fimg-out%2F2021%2F09%2F08%2F20210908115234.png&sign=7469a808b4198ddb8045e8f73c52bab9693cb426510c2cb13ac556224df2bb94#from=url&id=DMMFd&margin=%5Bobject%20Object%5D&originHeight=696&originWidth=857&originalType=binary&ratio=1&status=done&style=none)


打开监控页面后, 可以发现 在程序内仅有一个Driver


因为我们是Local模式, Driver即管理 又 干活.

同时, 输入jps
​

![](https://pybd.yuque.com/api/filetransfer/images?url=https%3A%2F%2Fimage-set.oss-cn-zhangjiakou.aliyuncs.com%2Fimg-out%2F2021%2F09%2F08%2F20210908115310.png&sign=4820d28ae60a4023e8f144f465bf5e5a8fdc039b914dd412fd4ba98d70e269db#from=url&id=aX2AO&margin=%5Bobject%20Object%5D&originHeight=296&originWidth=684&originalType=binary&ratio=1&status=done&style=none)


可以看到local模式下的唯一进程存在


这个进程 即是master也是worker


### bin/spark-shell - 了解


同样是一个解释器环境, 和`bin/pyspark`不同的是, 这个解释器环境 运行的不是python代码, 而是scala程序代码


```shell
scala> sc.parallelize(Array(1,2,3,4,5)).map(x=> x + 1).collect()
res0: Array[Int] = Array(2, 3, 4, 5, 6)
```


> 这个仅作为了解即可, 因为这个是用于scala语言的解释器环境



### bin/spark-submit (PI)


作用: 提交指定的Spark代码到Spark环境中运行


使用方法:


```shell
# 语法
bin/spark-submit [可选的一些选项] jar包或者python代码的路径 [代码的参数]

# 示例
bin/spark-submit /export/server/spark/examples/src/main/python/pi.py 10
# 此案例 运行Spark官方所提供的示例代码 来计算圆周率值.  后面的10 是主函数接受的参数, 数字越高, 计算圆周率越准确.
```


对比

| 功能 | bin/spark-submit | bin/pyspark | bin/spark-shell |
| --- | --- | --- | --- |
| 功能 | 提交java\scala\python代码到spark中运行 | 提供一个`python`
解释器环境用来以python代码执行spark程序 | 提供一个`scala`
解释器环境用来以scala代码执行spark程序 |
| 特点 | 提交代码用 | 解释器环境 写一行执行一行 | 解释器环境 写一行执行一行 |
| 使用场景 | 正式场合, 正式提交spark程序运行 | 测试\学习\写一行执行一行\用来验证代码等 | 测试\学习\写一行执行一行\用来验证代码等 |



> Local模式将是我们7天Spark课程的主力使用模式



## hadoop配置

如果报hadoop权限有限，无法打开，直接通过能打开的那台机器向不能打开的机器进行传输

```
scp -r hadoop-3.3.0 root@node2:/export/server
```

# Spark StandAlone环境部署
## 新角色 历史服务器


> 历史服务器不是Spark环境的必要组件, 是可选的.

> 回忆: 在YARN中 有一个历史服务器, 功能: 将YARN运行的程序的历史日志记录下来, 通过历史服务器方便用户查看程序运行的历史信息.



Spark的历史服务器, 功能: 将Spark运行的程序的历史日志记录下来, 通过历史服务器方便用户查看程序运行的历史信息.

搭建集群环境, 我们一般`推荐将历史服务器也配置上`, 方面以后查看历史记录
​

## 集群规划


课程中 使用三台Linux虚拟机来组成集群环境, 非别是:


node1\ node2\ node3


node1运行: Spark的Master进程  和 1个Worker进程


node2运行: spark的1个worker进程


node3运行: spark的1个worker进程


整个集群提供: 1个master进程 和 3个worker进程


## 安装


### 在所有机器安装Python(Anaconda)


参考 附1内容, 如何在Linux上安装anaconda


同时不要忘记 都创建`pyspark`虚拟环境 以及安装虚拟环境所需要的包`pyspark jieba pyhive`


### 在所有机器配置环境变量


参考 Local模式下 环境变量的配置内容

`确保3台都配置`


### 配置配置文件


进入到spark的配置文件目录中, `cd $SPARK_HOME/conf`


配置workers文件


```shell
# 改名, 去掉后面的.template后缀
mv workers.template workers

# 编辑worker文件
vim workers
# 将里面的localhost删除, 追加
node1
node2
node3
到workers文件内

# 功能: 这个文件就是指示了  当前SparkStandAlone环境下, 有哪些worker
```


配置spark-env.sh文件


```shell
# 1. 改名
mv spark-env.sh.template spark-env.sh

# 2. 编辑spark-env.sh, 在底部追加如下内容

## 设置JAVA安装目录
## 注意复制粘贴的时候上面的字别露出来
JAVA_HOME=/export/server/jdk1.8.0_241

## HADOOP软件配置文件目录，读取HDFS上文件和运行YARN集群
HADOOP_CONF_DIR=/export/server/hadoop/etc/hadoop
YARN_CONF_DIR=/export/server/hadoop/etc/hadoop

## 指定spark老大Master的IP和提交任务的通信端口
# 告知Spark的master运行在哪个机器上
export SPARK_MASTER_HOST=node1
# 告知sparkmaster的通讯端口
export SPARK_MASTER_PORT=7077
# 告知spark master的 webui端口
SPARK_MASTER_WEBUI_PORT=8080

# worker cpu可用核数
SPARK_WORKER_CORES=1
# worker可用内存
SPARK_WORKER_MEMORY=1g
# worker的工作通讯地址
SPARK_WORKER_PORT=7078
# worker的 webui地址
SPARK_WORKER_WEBUI_PORT=8081

## 设置历史服务器
# 配置的意思是  将spark程序运行的历史日志 存到hdfs的/sparklog文件夹中
SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=hdfs://node1:8020/sparklog/ -Dspark.history.fs.cleaner.enabled=true"
```

注意, 上面的配置的路径 要根据你自己机器实际的路径来写

接下来如果没有开启hadoop需要先开启hadoop

```
start-all.sh
```

先输入命令

```
hadoop fs -ls /
```


在HDFS上创建程序运行历史记录存放的文件夹:


```shell
hadoop fs -mkdir /sparklog
hadoop fs -chmod 777 /sparklog
```

必备，否则会报错

```
java.io.FileNotFoundException: File does not exist: hdfs://node1:8020/sparklog
```



配置spark-defaults.conf文件


```shell
# 1. 改名
mv spark-defaults.conf.template spark-defaults.conf

# 2. 修改内容, 追加如下内容
# 开启spark的日期记录功能
spark.eventLog.enabled 	true
# 设置spark日志记录的路径
spark.eventLog.dir	 hdfs://node1:8020/sparklog/ 
# 设置spark日志是否启动压缩
spark.eventLog.compress 	true
```


配置log4j.properties 文件 [可选配置]


```shell
# 1. 改名
mv log4j.properties.template log4j.properties

# 2. 修改内容 参考下图
```
![](https://pybd.yuque.com/api/filetransfer/images?url=https%3A%2F%2Fimage-set.oss-cn-zhangjiakou.aliyuncs.com%2Fimg-out%2F2021%2F09%2F08%2F20210908151736.png&sign=9baf2cdc4826e60d35e84d1c175de20def3c2f65ecfdba1c01a1ecfa9d8084ab#from=url&id=WzrZH&margin=%5Bobject%20Object%5D&originHeight=753&originWidth=1889&originalType=binary&ratio=1&status=done&style=none)
> 这个文件的修改不是必须的,  为什么修改为WARN. 因为Spark是个话痨
>  
> 会疯狂输出日志, 设置级别为WARN 只输出警告和错误日志, 不要输出一堆废话.



### 将Spark安装文件夹  分发到其它的服务器上


```shell
scp -r spark-3.1.2-bin-hadoop3.2 node2:/export/server/
scp -r spark-3.1.2-bin-hadoop3.2 node3:/export/server/
```


不要忘记, 在node2和node3上 给spark安装目录增加软链接


`ln -s /export/server/spark-3.1.2-bin-hadoop3.2 /export/server/spark`


### 检查


检查每台机器的:


JAVA_HOME


SPARK_HOME


PYSPARK_PYTHON


等等 环境变量是否正常指向正确的目录


### 启动历史服务器


`sbin/start-history-server.sh`


### 启动Spark的Master和Worker进程


```shell
# 启动全部master和worker
start-all.sh
sbin/start-all.sh
#想要看到三个对应的节点，必须start-all.sh+/export/server/spark/sbin/start-all.sh

# 或者可以一个个启动:
# 启动当前机器的master
sbin/start-master.sh
# 启动当前机器的worker
sbin/start-worker.sh

# 停止全部
sbin/stop-all.sh

# 停止当前机器的master
sbin/stop-master.sh

# 停止当前机器的worker
sbin/stop-worker.sh
```


### 查看Master的WEB UI


默认端口master我们设置到了8080


如果端口被占用, 会顺延到8081 ...;8082... 8083... 直到申请到端口为止


可以在日志中查看, 具体顺延到哪个端口上:

`Service 'MasterUI' could not bind on port 8080. Attempting port 8081.`
​ui显示可能有一点延迟



![](https://pybd.yuque.com/api/filetransfer/images?url=https%3A%2F%2Fimage-set.oss-cn-zhangjiakou.aliyuncs.com%2Fimg-out%2F2021%2F09%2F08%2F20210908154429.png&sign=0b1a6dd223bf6797523bc22637c1c8a115159f94ba56a111da7bb2d64c824418#from=url&id=K3JFo&margin=%5Bobject%20Object%5D&originHeight=831&originWidth=1903&originalType=binary&ratio=1&status=done&style=none)


### 连接到StandAlone集群


#### bin/pyspark


执行:


```shell
#/export/server/spark/bin/pyspark --master spark://node1:7077
bin/pyspark --master spark://node1:7077
# 通过--master选项来连接到 StandAlone集群
# 这里的7077端口通过webui进行查看->node1:8080可以查看到
# 如果不写--master选项, 默认是local模式运行!!!
# bin/pyspark --master local
# Running Applications里面Application ID有个id内容可以看到
```
![](https://pybd.yuque.com/api/filetransfer/images?url=https%3A%2F%2Fimage-set.oss-cn-zhangjiakou.aliyuncs.com%2Fimg-out%2F2021%2F09%2F08%2F20210908154652.png&sign=d22ba394d315a9b4921ac0892d30ecd2516ab354222f1c6b812b63126d3ba64e#from=url&id=aMI2X&margin=%5Bobject%20Object%5D&originHeight=497&originWidth=1644&originalType=binary&ratio=1&status=done&style=none)


#### bin/spark-shell


```shell
bin/spark-shell --master spark://node1:7077
# 同样适用--master来连接到集群使用
```


```scala
// 测试代码
sc.parallelize(Array(1,2,3,4,5)).map(x=> x + 1).collect()
```


#### bin/spark-submit (PI)


```shell
bin/spark-submit --master spark://node1:7077 /export/server/spark/examples/src/main/python/pi.py 100
# 同样使用--master来指定将任务提交到集群运行
```


### 查看历史服务器WEB UI


历史服务器的默认端口是: 18080


我们启动在node1上, 可以在浏览器打开:


`node1:18080`来进入到历史服务器的WEB UI上.
![](https://pybd.yuque.com/api/filetransfer/images?url=https%3A%2F%2Fimage-set.oss-cn-zhangjiakou.aliyuncs.com%2Fimg-out%2F2021%2F09%2F08%2F20210908160451.png&sign=24bd99909213440824bcb26ac36fa691aeff18418e941b24f8beaf028ecf547d#from=url&id=niLUK&margin=%5Bobject%20Object%5D&originHeight=616&originWidth=1881&originalType=binary&ratio=1&status=done&style=none)




# Spark StandAlone HA 环境搭建


## 步骤
> 前提: 确保Zookeeper 和 HDFS 均已经启动

**注意Zookeeper需要已经安装并且启动之后，才能够运行**

## 注意点

发现ping不通百度也没有关系，虚拟机不一定非得要ping通百度

## hdfs文件损坏删除

今天运行程序的时候，发现hdfs文件内容有所损坏，

在spark界面之中显示

```
org.apache.hadoop.hdfs.BlockMissingException: Could not obtain block: BP-636802204-192.168.88.151-1635231729126:blk_1073742072_1248
```

损坏命令：

```
There are 259 missing blocks. The following files may be corrupted
```

这里我们首先查看缺失文件

```
hdfs fsck /
```

接下来删除缺失文件

```
hdfs fsck -delete
```



## 配置Zookeeper的过程

https://www.cnblogs.com/zhao907/p/15420659.html

1.去https://zookeeper.apache.org/下载zookeeper

注意版本，启动报错可能找不到主类，运行时报错，可以下载源码版（带有bin的）

链接：https://pan.baidu.com/s/1OmZn5XCD5_kwI6ImKAAR7w 提取码：zzoo

2.解压到指定位置

```
tar -zxvf /export/software/apache-zookeeper-3.7.0-bin.tar.gz -C /export/server/
```

3.软链接

```
ln -s /export/server/apache-zookeeper-3.7.0 /export/server/zookeeper
```

4.修改文件(可能遭遇报错： /export/server/zookeeper/bin/../conf/zoo.cfg: 没有那个文件或目录)

```
cd /export/server/zookeeper/conf
mv zoo_sample.cfg zoo.cfg

然后修改一下zoo.cfg中的dataDir
dataDir = /export/server/zookeeper/zkdata
```

5.启动

```
sh /export/server/zookeeper/bin/zkServer.sh start
```





先在`spark-env.sh`中, 删除: `SPARK_MASTER_HOST=node1`


原因: 配置文件中固定master是谁, 那么就无法用到zk的动态切换master功能了.


在`spark-env.sh`中, 增加:


```shell
SPARK_DAEMON_JAVA_OPTS="-Dspark.deploy.recoveryMode=ZOOKEEPER -Dspark.deploy.zookeeper.url=node1:2181,node2:2181,node3:2181 -Dspark.deploy.zookeeper.dir=/spark-ha"
# spark.deploy.recoveryMode 指定HA模式 基于Zookeeper实现
# 指定Zookeeper的连接地址
# 指定在Zookeeper中注册临时节点的路径
```


将spark-env.sh 分发到每一台服务器上


```shell
scp spark-env.sh node2:/export/server/spark/conf/
scp spark-env.sh node3:/export/server/spark/conf/
```


停止当前StandAlone集群


```shell
sbin/stop-all.sh
```


启动集群:


```shell
# 在node1上 启动一个master 和全部worker
sbin/start-all.sh
#完整命令
/export/server/spark/sbin/start-all.sh

# 注意, 下面命令在node2上执行
sbin/start-master.sh
# 在node2上启动一个备用的master进程
```
![](https://pybd.yuque.com/api/filetransfer/images?url=https%3A%2F%2Fimage-set.oss-cn-zhangjiakou.aliyuncs.com%2Fimg-out%2F2021%2F09%2F08%2F20210908162145.png&sign=c91a0734a2253c8d5502c116be7c8ae516af6bd9bed835bb25698d6cd4852203#from=url&id=CK971&margin=%5Bobject%20Object%5D&originHeight=366&originWidth=888&originalType=binary&ratio=1&status=done&style=none)
![](https://pybd.yuque.com/api/filetransfer/images?url=https%3A%2F%2Fimage-set.oss-cn-zhangjiakou.aliyuncs.com%2Fimg-out%2F2021%2F09%2F08%2F20210908162112.png&sign=7bad1d23f0bbc110082c84a1432f50ac211f3114c786eb01df7d4691e83fad21#from=url&id=BTOB5&margin=%5Bobject%20Object%5D&originHeight=440&originWidth=858&originalType=binary&ratio=1&status=done&style=none)
## master主备切换


提交一个spark任务到当前`alive`master上:


```shell
bin/spark-submit --master spark://node1:7077 /export/server/spark/examples/src/main/python/pi.py 1000
```


在提交成功后, 将alivemaster直接kill掉

不会影响程序运行:
![](https://pybd.yuque.com/api/filetransfer/images?url=https%3A%2F%2Fimage-set.oss-cn-zhangjiakou.aliyuncs.com%2Fimg-out%2F2021%2F09%2F08%2F20210908162555.png&sign=6c267116ad788645fdc2af413af7ac1c6e22ae0d655afe3dd4fda1117f6d5253#from=url&id=AAdNb&margin=%5Bobject%20Object%5D&originHeight=314&originWidth=1889&originalType=binary&ratio=1&status=done&style=none)
当新的master接收集群后, 程序继续运行, 正常得到结果.


> 结论 HA模式下, 主备切换 不会影响到正在运行的程序.
>  
> 最大的影响是 会让它中断大约30秒左右.

运行完成之后，node1:8080的Running Applications只会有一个任务

点击任务application id进入之后，可以看到所有的任务

点击Application Detail UI，可以看到有多个complete任务(node1:4040/jobs/)

4040为driver提供的ui页面

node1:4040/executors/可以看到

# Spark On YARN 环境搭建
## 部署
确保:


- HADOOP_CONF_DIR
- YARN_CONF_DIR

**不需要单独安装YARN，安装完spark之后即内部自带了YARN**

在spark-env.sh 以及 环境变量配置文件中即可

之前配置了hadoop和yarn的目录，所以spark就可以在hadoop和yarn中自行读取相关文件

spark知道hadoop和yarn的resourcemanager

## 连接到YARN中


### bin/pyspark


```shell
bin/pyspark --master yarn --deploy-mode client|cluster
# --deploy-mode 选项是指定部署模式, 默认是 客户端模式
# client就是客户端模式
# cluster就是集群模式
# --deploy-mode 仅可以用在YARN模式下
```


> 注意: 交互式环境 pyspark  和 spark-shell  无法运行 cluster模式

## 运行报错解决：Your endpoint configuration is wrong;

运行的时候发现出现错误：

retry.RetryInvocationHandler: java.net.ConnectException:
Your endpoint configuration is wrong;
For more details see: http://wiki.apache.org/hadoop/UnsetHostnameOrPort, while invoking ApplicationClientProtocolPBClientImpl.getNewApplication over null after 7 failover attempts.
Trying to failover after sleeping for 24161ms.
这里的原因在于yarn没有打开，需要先打开yarn：start-yarn.sh

### bin/spark-shell


```shell
bin/spark-shell --master yarn --deploy-mode client|cluster
```


> 注意: 交互式环境 pyspark  和 spark-shell  无法运行 cluster模式



### bin/spark-submit (PI)


```shell
bin/spark-submit --master yarn --deploy-mode client|cluster /xxx/xxx/xxx.py 参数
```


## spark-submit 和 spark-shell 和 pyspark的相关参数


参见: 附2
​



# 附1 Anaconda On Linux 安装 (单台服务器)


## 安装


上传安装包:


上传: 资料中提供的`Anaconda3-2021.05-Linux-x86_64.sh`文件到Linux服务器上


安装:


`sh ./Anaconda3-2021.05-Linux-x86_64.sh`
![](https://pybd.yuque.com/api/filetransfer/images?url=https%3A%2F%2Fimage-set.oss-cn-zhangjiakou.aliyuncs.com%2Fimg-out%2F2021%2F09%2F08%2F20210908111453.png&sign=5dccc0db81fe59d7b0ebbb93eeae3ace0f71fecaf674ba104dca6da708bbded1#from=url&id=sxALL&margin=%5Bobject%20Object%5D&originHeight=235&originWidth=779&originalType=binary&ratio=1&status=done&style=none)
![](https://pybd.yuque.com/api/filetransfer/images?url=https%3A%2F%2Fimage-set.oss-cn-zhangjiakou.aliyuncs.com%2Fimg-out%2F2021%2F09%2F08%2F20210908111604.png&sign=1a71b7a5eba7dbcc0c0a97a6b925fc3b99f4ca0ef850e2651846390807671247#from=url&id=l6Gen&margin=%5Bobject%20Object%5D&originHeight=469&originWidth=1388&originalType=binary&ratio=1&status=done&style=none)
![](https://pybd.yuque.com/api/filetransfer/images?url=https%3A%2F%2Fimage-set.oss-cn-zhangjiakou.aliyuncs.com%2Fimg-out%2F2021%2F09%2F08%2F20210908111618.png&sign=6260ba1bdd46edbd6b7be986e3251f217413df4cd355ffd67627dbb92ede0723#from=url&id=x4kli&margin=%5Bobject%20Object%5D&originHeight=96&originWidth=729&originalType=binary&ratio=1&status=done&style=none)
![](https://pybd.yuque.com/api/filetransfer/images?url=https%3A%2F%2Fimage-set.oss-cn-zhangjiakou.aliyuncs.com%2Fimg-out%2F2021%2F09%2F08%2F20210908111658.png&sign=f8e99538c446eced143a4a758eb9b6cb56c5f4d6cc8107311ee1092aff9a5fa0#from=url&id=w9LHq&margin=%5Bobject%20Object%5D&originHeight=459&originWidth=1128&originalType=binary&ratio=1&status=done&style=none)
![](https://pybd.yuque.com/api/filetransfer/images?url=https%3A%2F%2Fimage-set.oss-cn-zhangjiakou.aliyuncs.com%2Fimg-out%2F2021%2F09%2F08%2F20210908111910.png&sign=2a56dbadf6a6b999c4507d9c8aa6fda69fcd3c112d9ce83d6412244a09849c4e#from=url&id=ENG5U&margin=%5Bobject%20Object%5D&originHeight=196&originWidth=977&originalType=binary&ratio=1&status=done&style=none)
输入yes后就安装完成了.

安装完成后, `退出SecureCRT 重新进来`:
![](https://pybd.yuque.com/api/filetransfer/images?url=https%3A%2F%2Fimage-set.oss-cn-zhangjiakou.aliyuncs.com%2Fimg-out%2F2021%2F09%2F08%2F20210908111941.png&sign=1a5e6ba7018718a2d5f754264da96c4dec8fd4d209218cabebfcd5b569b1696b#from=url&id=BgIy7&margin=%5Bobject%20Object%5D&originHeight=97&originWidth=545&originalType=binary&ratio=1&status=done&style=none)


看到这个Base开头表明安装好了.


base是默认的虚拟环境.
​

## 国内源
如果你安装好后, 没有出现base, 可以打开:/root/.bashrc这个文件, 追加如下内容:



```shell
channels:
  - defaults
show_channel_urls: true
default_channels:
  - https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/main
  - https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/r
  - https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/msys2
custom_channels:
  conda-forge: https://mirrors.tuna.tsinghua.edu.cn/anaconda/cloud
  msys2: https://mirrors.tuna.tsinghua.edu.cn/anaconda/cloud
  bioconda: https://mirrors.tuna.tsinghua.edu.cn/anaconda/cloud
  menpo: https://mirrors.tuna.tsinghua.edu.cn/anaconda/cloud
  pytorch: https://mirrors.tuna.tsinghua.edu.cn/anaconda/cloud
  simpleitk: https://mirrors.tuna.tsinghua.edu.cn/anaconda/cloud
```


# 附2 spark-submit和pyspark相关参数


客户端工具我们可以用的有:


- bin/pyspark: pyspark解释器spark环境
- bin/spark-shell: scala解释器spark环境
- bin/spark-submit: 提交jar包或Python文件执行的工具
- bin/spark-sql: sparksql客户端工具



这4个客户端工具的参数基本通用.


以spark-submit 为例:


`bin/spark-submit --master spark://node1:7077 xxx.py`


```shell
Usage: spark-submit [options] <app jar | python file | R file> [app arguments]
Usage: spark-submit --kill [submission ID] --master [spark://...]
Usage: spark-submit --status [submission ID] --master [spark://...]
Usage: spark-submit run-example [options] example-class [example args]

Options:
  --master MASTER_URL         spark://host:port, mesos://host:port, yarn,
                              k8s://https://host:port, or local (Default: local[*]).
  --deploy-mode DEPLOY_MODE   部署模式 client 或者 cluster 默认是client
  --class CLASS_NAME          运行java或者scala class(for Java / Scala apps).
  --name NAME                 程序的名字
  --jars JARS                 Comma-separated list of jars to include on the driver
                              and executor classpaths.
  --packages                  Comma-separated list of maven coordinates of jars to include
                              on the driver and executor classpaths. Will search the local
                              maven repo, then maven central and any additional remote
                              repositories given by --repositories. The format for the
                              coordinates should be groupId:artifactId:version.
  --exclude-packages          Comma-separated list of groupId:artifactId, to exclude while
                              resolving the dependencies provided in --packages to avoid
                              dependency conflicts.
  --repositories              Comma-separated list of additional remote repositories to
                              search for the maven coordinates given with --packages.
  --py-files PY_FILES         指定Python程序依赖的其它python文件
  --files FILES               Comma-separated list of files to be placed in the working
                              directory of each executor. File paths of these files
                              in executors can be accessed via SparkFiles.get(fileName).
  --archives ARCHIVES         Comma-separated list of archives to be extracted into the
                              working directory of each executor.

  --conf, -c PROP=VALUE       手动指定配置
  --properties-file FILE      Path to a file from which to load extra properties. If not
                              specified, this will look for conf/spark-defaults.conf.

  --driver-memory MEM         Driver的可用内存(Default: 1024M).
  --driver-java-options       Driver的一些Java选项
  --driver-library-path       Extra library path entries to pass to the driver.
  --driver-class-path         Extra class path entries to pass to the driver. Note that
                              jars added with --jars are automatically included in the
                              classpath.

  --executor-memory MEM       Executor的内存 (Default: 1G).

  --proxy-user NAME           User to impersonate when submitting the application.
                              This argument does not work with --principal / --keytab.

  --help, -h                  显示帮助文件
  --verbose, -v               Print additional debug output.
  --version,                  打印版本

 Cluster deploy mode only(集群模式专属):
  --driver-cores NUM          Driver可用的的CPU核数(Default: 1).

 Spark standalone or Mesos with cluster deploy mode only:
  --supervise                 如果给定, 可以尝试重启Driver

 Spark standalone, Mesos or K8s with cluster deploy mode only:
  --kill SUBMISSION_ID        指定程序ID kill
  --status SUBMISSION_ID      指定程序ID 查看运行状态

 Spark standalone, Mesos and Kubernetes only:
  --total-executor-cores NUM  整个任务可以给Executor多少个CPU核心用

 Spark standalone, YARN and Kubernetes only:
  --executor-cores NUM        单个Executor能使用多少CPU核心

 Spark on YARN and Kubernetes only(YARN模式下):
  --num-executors NUM         Executor应该开启几个
  --principal PRINCIPAL       Principal to be used to login to KDC.
  --keytab KEYTAB             The full path to the file that contains the keytab for the
                              principal specified above.

 Spark on YARN only:
  --queue QUEUE_NAME          指定运行的YARN队列(Default: "default").
```




# 附3 Windows系统配置Anaconda
## 安装
打开资料中提供的:Anaconda3-2021.05-Windows-x86_64.exe文件,或者去官网下载:[https://www.anaconda.com/products/individual#Downloads](https://www.anaconda.com/products/individual#Downloads)
​

打开后,一直点击`Next`下一步即可:
如果想要修改安装路径, 可以修改
不必勾选add path
最终点击Finish完成安装
​

打开开始菜单, 搜索Anaconda
![image.png](https://cdn.nlark.com/yuque/0/2021/png/12371070/1635405295376-529e614f-eb90-4943-bc70-8b97908e92b9.png#clientId=u8fb108bc-acfc-4&from=paste&height=688&id=u6199808c&margin=%5Bobject%20Object%5D&name=image.png&originHeight=688&originWidth=542&originalType=binary&ratio=1&size=71371&status=done&style=none&taskId=u6d8631b1-1148-4250-90ef-2c7ab3ca453&width=542)
出现如图的程序, 安装成功.
​

打开 `Anaconda Prompt`程序:
出现`base`说明安装正确.
​



## 一、配置国内源
Anaconda默认源服务器在国外, 网速比较慢, 配置国内源加速网络下载.
​

打开上图中的 `Anaconda Prompt`程序:
执行:
`conda config --set show_channel_urls yes`
​

然后用记事本打开:
`\root\.condarc`文件, 将如下内容替换进文件内,保存即可:

```shell
channels:
  - defaults
show_channel_urls: true
default_channels:
  - https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/main
  - https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/r
  - https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/msys2
custom_channels:
  conda-forge: https://mirrors.tuna.tsinghua.edu.cn/anaconda/cloud
  msys2: https://mirrors.tuna.tsinghua.edu.cn/anaconda/cloud
  bioconda: https://mirrors.tuna.tsinghua.edu.cn/anaconda/cloud
  menpo: https://mirrors.tuna.tsinghua.edu.cn/anaconda/cloud
  pytorch: https://mirrors.tuna.tsinghua.edu.cn/anaconda/cloud
  simpleitk: https://mirrors.tuna.tsinghua.edu.cn/anaconda/cloud
```

如果是国内的镜像源，在C:\Users\47455下面创建一个.condarc文件，然后将上面的网址复制粘贴上去

接下来重进一次anaconda prompt，然后输入命令conda create -n pyspark python=3.8，这之后就开始下载安装程序了

然后conda activate pyspark就可以切换到虚拟环境了

接下来在windows上面安装包

```
pip install pyspark pyhive pymysql jieba -i https://pypi.tuna.tsinghua.edu.cn/simple
```

打开pycharm配置，选择D:\anaconda3\envs\pyspark\python.exe



先配置interpreter D:\anaconda3\ _conda.exe，再配置 D:\anaconda3\Scripts\conda.exe

不按照上述流程配置会报错：

勾选make available to all projects，注意这个一定要勾选，不然库放不进来

配置好并且创建好项目之后，点开右下角的解释器，查看对应的包是否安装成功

## 二、创建虚拟环境


```shell
# 创建虚拟环境 pyspark, 基于Python 3.8
conda create -n pyspark python=3.8

# 切换到虚拟环境内
conda activate pyspark

# 切换完成之后前缀会变成pyspark的前缀

# 在虚拟环境内安装包
pip install pyhive pyspark jieba -i https://pypi.tuna.tsinghua.edu.cn/simple 
```

装了conda环境之后仍然可以使用pip安装对应的库

因为这里已经切换到pyspark的虚拟环境中了

## 三、windows上添加解释器

点击pycharm右下角的小点点，有个添加解释器

添加ssh解释器

然后找到linux上可用解释器

/export/server/anaconda3/bin/python3





