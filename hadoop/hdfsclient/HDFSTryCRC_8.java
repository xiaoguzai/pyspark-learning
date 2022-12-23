package org.example.HDFSClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSTryCRC_8 {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        BasicConfigurator.configure();
        PropertyConfigurator.configure("D:\\FlinkProject\\HDFSClient\\src\\test\\resources\\log4j.properties");
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:8020"),
                new Configuration(),"root");

        //参数一：原文件是否删除，参数二：原文件路径HDFS，参数三：目标地址路径win，参数四
        fs.copyToLocalFile(false,new Path("/input/words.txt"),new Path("d:\\"),false);
        //读取完成之后存在words.txt以及.words.txt.crs两个文件
        //这里的crs文件就是校验码，因此可以进行校验
    }
}
