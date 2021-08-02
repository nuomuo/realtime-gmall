package utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 *  Kafka 工具类
 */
public class MyKafkaUtils {

    private static final String brokerList = "node1:9092,node2:9092,node3:9092";
    private static final String DWD_DEFAULT_TOPIC = "DWD_DEFAULT_TOPIC";
    /**
     * 获取 Kafka 生产者
     * @param topic 自定义主题名
     * @return 生产者
     */
    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic){
        //String brokerList = "node1:9092,node2:9092,node3:9092";
        return new FlinkKafkaProducer<String>(brokerList,topic,new SimpleStringSchema());

    }



    /**
     * 根据 Kafka 主题和消费者组 获取 FlinkKafkaConsumer
     * @param topic  主题
     * @param groupId 消费者组
     * @return FlinkKafkaConsumer
     */
    public static FlinkKafkaConsumer<String> getFlinkKafkaConsumer(String topic,String groupId){

        // 1. 创建kafka消费者配置类
        Properties properties = new Properties();
        // 2. 添加配置参数
        // 添加连接
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);

        // 配置序列化 必须
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // 配置消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // 是否自动提交 offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // 提交 offset 的时间周期
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");


        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),properties);

    }


    /**
     * 获取Kafka生产者对象
     */
    public static <T> FlinkKafkaProducer<T> getKafkaProducerWithSchema(KafkaSerializationSchema<T> serializationSchema) {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);

        return new FlinkKafkaProducer<T>(DWD_DEFAULT_TOPIC,
                serializationSchema,
                properties,
                // 两阶段提交
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    public static String getKafkaDDL(String topic, String groupId) {
        return " ( " +
                "  'connector' = 'kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + brokerList + "', " +
                "  'properties.group.id' = '" + groupId + "', " +
                "  'format' = 'json' " +
                ")";
    }

}
