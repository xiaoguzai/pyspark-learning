#提交命令的几种指令形式

把上面的代码放入/code/helloworld.py之中，然后使用命令进行运行，先尝试一下以local模式进行运行 /export/server/spark/bin/spark-submit --master local[*] /code/helloworld.py
接下来以yarn模式运行一下
/export/server/spark/bin/spark-submit --master yarn /code/helloworld.py
以yarn模式运行涉及到容器的构建，它们的构建速度不会太快
以yarn模式多核运行，确保node2和node3也能够参与进去
/export/server/spark/bin/spark-submit --master yarn --num-executors 6 /code/helloworld.py如果想要跑分布式，数据必须在hdfs上面，如果数据在node1上面会报错
