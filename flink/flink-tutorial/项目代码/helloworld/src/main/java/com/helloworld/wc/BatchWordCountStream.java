package com.helloworld.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import java.util.Arrays;

public class BatchWordCountStream {
    public static void main(String[] args) throws Exception{
        //1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.读取文本流
        DataStreamSource<String> lineDSS = env.readTextFile("input/words.txt");
        //Unable to make field private final byte[] java.lang.String.value accessible:
        //原因：java的版本太高，直接换成版本1.8即可
        //!!!注意这里需要查看运行的部分 D:\java-1.8\bin\java.exe ...
        //这样的情况下才调用1.8的java了，否则有可能设置了没有调用成功


        //3.转换计算
        SingleOutputStreamOperator<Tuple2<String,Long>> wordAndOneTuple = lineDSS
                .flatMap((String line, Collector<String> words)->{
                    Arrays.stream(line.split(" ")).forEach(words::collect);
                })
                .returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING,Types.LONG));

        //4.分组
        KeyedStream<Tuple2<String,Long>,String> wordAndOneKS = wordAndOneTuple
                .keyBy(t -> t.f0);
        //5.求和
        SingleOutputStreamOperator<Tuple2<String,Long>> result = wordAndOneKS
                .sum(1);
        //6.打印
        result.print();
        //7.执行
        env.execute();
        /***
         运行结果，针对words。txt
         hello world
         hello flink
         hello java
         运行结果
         3> (java,1)
         13> (flink,1)
         5> (hello,1)
         9> (world,1)
         5> (hello,2)
         5> (hello,3)
         每出现一次数据统计一次次数
         ***/
    }
}
