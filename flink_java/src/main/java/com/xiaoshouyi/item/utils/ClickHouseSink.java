package com.xiaoshouyi.item.utils;

import com.xiaoshouyi.item.bean.TransientSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author：zhd
 * @Date: 2021/8/22 14:49
 * @Dscription:
 */

public class ClickHouseSink {

    public static <T>SinkFunction getClickhouseSink(String sql){
        return JdbcSink.sink(sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {

                        //反射的方式获取所有的列别名，包括私有属性
                        Field[] declaredFields = t.getClass().getDeclaredFields();

                        int offset = 0;
                        for (int i = 0; i < declaredFields.length ; i++) {

                            //获取属性名字
                            Field declaredField = declaredFields[i];

                            //通过反射的方式获取属性值
                            declaredField.setAccessible(true);

                            //获取注解，判断属性值是否用写入库
                            TransientSink annotation = declaredField.getAnnotation(TransientSink.class);

                            if (annotation !=null){
                                offset++;
                                continue;
                            }

                            //官方写法
                            try {
                                preparedStatement.setObject(i+1-offset,declaredField.get(t));
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }

                        }


                    }
                }, JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(10)
                        .withBatchSize(10)
                        .withMaxRetries(100)
                        .build()
                ,new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("")
                        .withUrl("")
                        .build());
    }
}
