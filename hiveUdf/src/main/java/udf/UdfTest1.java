package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import javax.tools.StandardJavaFileManager;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Description(name = "hive2kafka", value = "_FUNC_(brokerhost_and_port,topic, array<map<string,string>>) - Return ret ")
public class UdfTest1 extends GenericUDF {

    private String hostAndPort;
    private String topics;
    private String tablename;
    private StandardListObjectInspector paramsListInspector;
    private StandardMapObjectInspector paramsElementInspector;



    public ObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {
        if (arg0.length != 4) {
            throw new UDFArgumentException(" Expecting   two  arguments:<brokerhost:port> <topic>  array<map<string,string>> ");
        }
        // 第一个参数验证
        if (arg0[0].getCategory() == Category.PRIMITIVE
                && ((PrimitiveObjectInspector) arg0[0]).getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            if (!(arg0[0] instanceof ConstantObjectInspector)) {
                throw new UDFArgumentException("broker host:port  must be constant");
            }
            ConstantObjectInspector brokerhost_and_port = (ConstantObjectInspector) arg0[0];

            hostAndPort = brokerhost_and_port.getWritableConstantValue().toString();
        }

        // 第二个参数验证
        if (arg0[1].getCategory() == Category.PRIMITIVE
                && ((PrimitiveObjectInspector) arg0[1]).getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            if (!(arg0[1] instanceof ConstantObjectInspector)) {
                throw new UDFArgumentException("kafka topic must be constant");
            }
            ConstantObjectInspector topicCOI = (ConstantObjectInspector) arg0[1];

            topics = topicCOI.getWritableConstantValue().toString();
        }


        // 第三个参数验证
        if (arg0[2].getCategory() == Category.PRIMITIVE
                && ((PrimitiveObjectInspector) arg0[2]).getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            if (!(arg0[2] instanceof ConstantObjectInspector)) {
                throw new UDFArgumentException("kafka topic must be constant");
            }
            ConstantObjectInspector tablename_s = (ConstantObjectInspector) arg0[2];

            //需要将hadoop writable对象装换为java类型，否则序列化失败，乱码
            Object table_hadoop_obj = tablename_s.getWritableConstantValue();

            tablename = table_hadoop_obj.toString();
        }

        // 第四个参数验证
        if (arg0[3].getCategory() != Category.LIST) {
            throw new UDFArgumentException(" Expecting an array<map<string,string>> field as third argument ");
        }
        ListObjectInspector third = (ListObjectInspector) arg0[3];
        if (third.getListElementObjectInspector().getCategory() != Category.MAP) {
            throw new UDFArgumentException(" Expecting an array<map<string,string>> field as third argument ");
        }


        paramsListInspector = ObjectInspectorFactory.getStandardListObjectInspector(third.getListElementObjectInspector());
        paramsElementInspector = (StandardMapObjectInspector) third.getListElementObjectInspector();
        System.out.println(paramsElementInspector.getMapKeyObjectInspector().getCategory());
        System.out.println(paramsElementInspector.getMapValueObjectInspector().getCategory());

        return PrimitiveObjectInspectorFactory.writableIntObjectInspector;

    }

    public Object evaluate(DeferredObject[] arg0) throws HiveException {
        Properties props = new Properties();
        props.put("bootstrap.servers", hostAndPort);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 创建kafka生产者
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < paramsListInspector.getListLength(arg0[3].get()); i++) {
            Object row = paramsListInspector.getListElement(arg0[3].get(), i);
            Map<?, ?> map = paramsElementInspector.getMap(row);
            // Object obj = ObjectInspectorUtils.copyToStandardJavaObject(row,paramsElementInspector);
            // 转成标准的java map，否则里面的key value字段为hadoop writable对象
            Map<String, Object> data = new HashMap<String,Object>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (entry.getValue() != null && !"".equals(entry.getValue().toString())) {
                    Object value = entry.getValue();
                    data.put(entry.getKey().toString(),value);
                }
            }

            data.put("tablename",tablename);
            JSONObject jsonObject = new JSONObject(data);
            //指定数据均匀写入3个分区中
            //int part = i % 2;
            producer.send(new ProducerRecord<String, String>(topics,jsonObject.toString()));

        }

        producer.close();

        return new IntWritable(1);
    }

    public String getDisplayString(String[] strings) {
        return "hive2kafka(brokerhost_and_port,topic, array<map<string,string>>)";
    }
}