package org.example.HDFSClient;

import org.apache.log4j.BasicConfigurator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFS_UploadFile_2 {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        BasicConfigurator.configure();//自动快速地使用缺省Log4j环境
        URI uri = new URI("hdfs://node1:8020");

        Configuration configuration = new Configuration();

        configuration.set("dfs.replication","2");
        //设置副本数为2
        /***
         * hdfs-site.xml中也有配置副本数量
         *
         * <configuration>
         *     <property>
         *         <name>dfs.replication</name>
         *         <value>1</value>
         *     </property>
         * </configuration>
         *
         * 此时运行完成之后发现副本数量为2，说明代码中的副本数量优先级更高
         */
        String user = "root";
        FileSystem fs = FileSystem.get(uri,configuration,user);

        //上传文件,参数一：表示是否删除原数据，参数二：是否允许覆盖，参数三：原数据路径，参数四：目的地路径
        fs.copyFromLocalFile(false,true
                ,new Path("d:/beauty.jpg"),new Path("/xiaoguzai/"));

        /***
         * 参数优先级
         * hdfs-default.xml => hdfs-site.xml=>在项目资源目录下的配置文件
         */

        //关闭资源
        fs.close();
    }
}
