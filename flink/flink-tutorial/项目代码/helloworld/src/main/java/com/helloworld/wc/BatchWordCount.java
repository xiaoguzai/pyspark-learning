package com.helloworld.wc;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

//最经典的api的调用，从flink1.12开始使用批流处理一体datastream，此后不在使用dataset api
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.从文件读取数据 按行读取(存储的元素就是每行的文本)
        DataSource<String> lineDS = env.readTextFile("input/words.txt");

        //3.将每行数据进行分词，转换成二元组类型
        //(hello,1)把前面的单词作为分割的依据
        FlatMapOperator<String, Tuple2<String,Long>> wordAndOne = lineDS
                .flatMap((String line,Collector<Tuple2<String, Long>> out)->{
                    //flagMap中，line为输入参数，out为转换之后输出的参数
                    //这里面的输出并不是直接输出的，而是要引入一个Collector收集器进行收集
                    String[] words = line.split(" ");
                    for(String word:words)
                    {
                        out.collect(Tuple2.of(word,1L));
                        //这里构建的二元组对应Tuple2<String,Long> out类型
                        //构建(word,1)的对应元组
                    }
                })
                .returns(Types.TUPLE(Types.STRING,Types.LONG));

        //wordAndOne.print();

        //4.按照word进行分组
        UnsortedGrouping<Tuple2<String,Long>> wordAndOneUG = wordAndOne.groupBy(0);
        //按照当前的索引元素单独指定
        //5.分组内聚合统计
        AggregateOperator<Tuple2<String,Long>> sum = wordAndOneUG.sum(1);
        //按照第二个元素进行聚合，所有(word,1)聚合在一起

        //6.打印结果
        sum.print();
        //这里报错的原因，main函数必须抛出异常
    }
}
