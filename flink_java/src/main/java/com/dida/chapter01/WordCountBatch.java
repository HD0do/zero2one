package com.dida.chapter01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCountBatch {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //1.1设置并行度
        env.setParallelism(1);
        //2.读取文件-》数据源
        DataSource<String> lineDS = env.readTextFile("D:\\develop\\java\\flink_java\\src\\input\\wordCount");

        //3.转换数据格式
        FlatMapOperator<String, Tuple2<String, Long>> wordANDone = lineDS.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] split = line.split(" ");
            for (String s : split) {
                out.collect(Tuple2.of(s, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING,Types.LONG));

        //4.分组
        UnsortedGrouping<Tuple2<String, Long>> wordANDoneUG = wordANDone.groupBy(0);

        //5.组内聚合统计
        AggregateOperator<Tuple2<String, Long>> wordSum = wordANDoneUG.sum(1);

        //6.打印结果
       wordSum.print();


    }
}
