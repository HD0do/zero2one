package com.dida.practice.operator.tranceform.processFun;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Optional;

/**
 * @Author：zhd
 * @Date: 2023/3/6 16:57
 * @Dscription:
 */
public class CoProcessFun {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);




        env.configure(new ReadableConfig() {
            @Override
            public <T> T get(ConfigOption<T> option) {
                return null;
            }

            @Override
            public <T> Optional<T> getOptional(ConfigOption<T> option) {
                return Optional.empty();
            }
        });

        env.setParallelism(1);


//        env.setRestartStrategy();



        env.execute();


    }
}
