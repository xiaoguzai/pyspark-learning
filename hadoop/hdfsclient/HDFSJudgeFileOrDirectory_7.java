package org.example.HDFSClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSJudgeFileOrDirectory_7 {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        BasicConfigurator.configure();
        PropertyConfigurator.configure("D:\\FlinkProject\\HDFSClient\\src\\test\\resources\\log4j.properties");
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:8020"),
                configuration,"root");
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for(FileStatus fileStatus:listStatus)
        {
            if(fileStatus.isFile())
            {
                System.out.println("f:"+fileStatus.getPath().getName());
            }
            else
            {
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }
        fs.close();
    }
}
