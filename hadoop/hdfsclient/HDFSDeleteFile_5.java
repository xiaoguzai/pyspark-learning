package org.example.HDFSClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSDeleteFile_5 {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        BasicConfigurator.configure();
        Configuration configuration = new Configuration();
        //1.获取文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:8020"),
                configuration,"root");

        //2.执行删除,参数1：要删除的路径，参数2：是否递归删除
        fs.delete(new Path("/xiaoguzai/meihouwang.txt"),false);

        //3.关闭资源
        fs.close();
    }
}
