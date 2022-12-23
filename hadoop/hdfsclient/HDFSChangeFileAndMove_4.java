package org.example.HDFSClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSChangeFileAndMove_4 {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        BasicConfigurator.configure();//快速地使用缺省环境
        URI uri = new URI("hdfs://node1:8020");

        //1.获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:8020"),configuration,"root");

        //2.修改文件名称
        fs.rename(new Path("/xiaoguzai/sunwukong.txt"),new Path("/xiaoguzai/meihouwang.txt"));

        //3.关闭资源
        fs.close();
    }
}
