package com.xiaoshouyi;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class Mongo2Hive {
    public static void main(String[] args) {
        //spark 2.x
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession.builder()
                .master("local[2]")
                .appName("SparkReadMgToHive")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .config("spark.mongodb.input.uri", "mongodb://10.40.20.47:27017/test_db.test_table")
                .enableHiveSupport()
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        //  spark 1.x
        //  JavaSparkContext sc = new JavaSparkContext(conf);
        //  sc.addJar("/Users/mac/zhangchun/jar/mongo-spark-connector_2.11-2.2.2.jar");
        //  sc.addJar("/Users/mac/zhangchun/jar/mongo-java-driver-3.6.3.jar");
        //  SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkReadMgToHive");
        //  conf.set("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/test.mgtest");
        //  conf.set("spark. serializer","org.apache.spark.serializer.KryoSerialzier");
        //  HiveContext sqlContext = new HiveContext(sc);
        //  //create df from mongo
        //  Dataset<Row> df = MongoSpark.read(sqlContext).load().toDF();
        //  df.select("id","name","name").show();

        String querysql= "select id,name,location,sex,position from mgtohive_2 b";
        String opType ="P";

        SQLUtils sqlUtils = new SQLUtils();
        List<String> column = sqlUtils.getColumns(querysql);

        //create rdd from mongo
        JavaRDD<Document> rdd = MongoSpark.load(sc);
        //将Document转成Object
        JavaRDD<Object> Ordd = rdd.map(new Function<Document, Object>() {
            public Object call(Document document){
                List list = new ArrayList();
                for (int i = 0; i < column.size(); i++) {
                    list.add(String.valueOf(document.get(column.get(i))));
                }
                return list;

//                return list.toString().replace("[","").replace("]","");
            }
        });
        System.out.println(Ordd.first());
        //通过编程方式将RDD转成DF
        List ls= new ArrayList();
        for (int i = 0; i < column.size(); i++) {
            ls.add(column.get(i));
        }
        String schemaString = ls.toString().replace("[","").replace("]","").replace(" ","");
        System.out.println(schemaString);

        List<StructField> fields = new ArrayList<StructField>();
        for (String fieldName : schemaString.split(",")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRDD = Ordd.map((Function<Object, Row>) record -> {
            List fileds = (List) record;
            // String[] attributes = record.toString().split(",");
            return RowFactory.create(fileds.toArray());
        });

        Dataset<Row> df = spark.createDataFrame(rowRDD,schema);

        //将DF写入到Hive中
        //选择Hive数据库
        spark.sql("use datalake");
        //注册临时表
        df.registerTempTable("mgtable");

        if ("O".equals(opType.trim())) {
            System.out.println("数据插入到Hive ordinary table");
            Long t1 = System.currentTimeMillis();
            spark.sql("insert into mgtohive_2 " + querysql + " " + "where b.id not in (select id from mgtohive_2)");

            System.out.println("insert into mgtohive_2 " + querysql + " ");

            Long t2 = System.currentTimeMillis();
            System.out.println("共耗时：" + (t2 - t1)  + "分钟");
        }else if ("P".equals(opType.trim())) {

            System.out.println("数据插入到Hive dynamic partition table");
            Long t3 = System.currentTimeMillis();
            //必须设置以下参数 否则报错
            spark.sql("set hive.exec.dynamic.partition.mode=nonstrict");
            //sex为分区字段   select语句最后一个字段必须是sex
            spark.sql("insert into mg_hive_external partition(sex) select id,name,location,position,sex from mgtable b where b.id not in (select id from mg_hive_external)");
            Long t4 = System.currentTimeMillis();
            System.out.println("共耗时："+(t4 -t3)+ "分钟");
        }
        spark.stop();
    }
}
