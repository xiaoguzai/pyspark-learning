package org.example.HDFSClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HDFSFileDetail_6 {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        BasicConfigurator.configure();
        URI uri = new URI("hdfs://node1:8020");

        //1.获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:8020"),
                configuration,"root");

        //2.获取文件详情,alt+回车抛出异常
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"),true);

        //3.遍历迭代器
        while(listFiles.hasNext())
        {
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("========"+fileStatus.getPath()+"========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            //获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            //打变量.var可以自动使用数组补全
            System.out.println(Arrays.toString(blockLocations));
            //blockLocations.sout也可以自动显示System.out内容
            //这里面打印的blockLocations是地址，而并不是详细信息，此时需要
            //加上toString变换类型
        }

        /***
         * ========hdfs://node1:8020/output/_SUCCESS========
         * rw-r--r--
         * root
         * supergroup
         * 0
         * 1668843965565
         * 3
         * 134217728
         * _SUCCESS
         * []
         */

        fs.close();
    }
}
