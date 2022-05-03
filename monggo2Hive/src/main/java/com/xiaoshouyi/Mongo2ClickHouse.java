package com.xiaoshouyi;

import org.apache.spark.sql.SparkSession;

/**
 * @Author：zhd
 * @Date: 2021/11/5 14:07
 * @Dscription:
 */
public class Mongo2ClickHouse {
    public static void main(String[] args) {

        //创建spark的基础配置环境
        SparkSession spark = SparkSession.builder().appName("Mongo2CK")
                .master("local[*]")
                .getOrCreate();

        spark.read().format("com.mongodb.spark.sql");






    }
}
