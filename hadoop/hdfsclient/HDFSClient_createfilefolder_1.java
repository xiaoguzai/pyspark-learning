package org.example.HDFSClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 客户端代码常用套路
 * 1、获取一个客户端对象
 * 2、执行相关的操作命令
 * 3、关闭资源
 */
public class HDFSClient_createfilefolder_1 {
    public static void main(String[] args)  throws IOException, URISyntaxException,
            InterruptedException{

        BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境
        //连接集群nn地址
        URI uri = new URI("hdfs://node1:8020");

        //创建一个配置文件
        Configuration configuration = new Configuration();

        // 用户
        String user = "root";

        //1 获取到了客户端对象
        FileSystem fs = FileSystem.get(uri,configuration,user);
        //2 创建一个文件夹
        fs.mkdirs(new Path("/xiaoguzai1/"));

        //3 关闭资源
        fs.close();
    }
    //有debug消息没事，目前这段代码能够成功的在hdfs上面创建xiaoguzai的目录
}
