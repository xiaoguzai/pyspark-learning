package org.example.HDFSClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSDownloadClient_3 {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        //BasicConfigurator.configure();
        URI uri = new URI("hdfs://node1:8020");
        Configuration configuration = new Configuration();

        //1.获取文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:8020"),
                configuration,"root");

        //2.执行下载操作
        //boolean delSrc:是否将文件删除
        //Path src:要下载的文件路径
        //Path dst:将文件下载到的路径
        //boolean useRawLocalFileSystem:是否开启文件校验
        fs.copyToLocalFile(false,new Path("/xiaoguzai/beauty.jpg"),new Path("d:/beauty.jpg"),true);

        //关闭资源
        fs.close();
    }
}
