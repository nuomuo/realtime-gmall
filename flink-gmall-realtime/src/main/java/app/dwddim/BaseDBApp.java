package app.dwddim;

import bean.TableProcess;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.sun.deploy.nativesandbox.NativeSandboxBroker;
import func.DimSink;
import func.TableProcessFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import utils.MyKafkaUtils;
import func.TableProcessStreamDebeziumDeserializationSchema;

import javax.annotation.Nullable;

public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT, "8081-8089");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);


        // 配置状态后端
        // TODO 从Kafka中读取业务数据
        String topic = "ods_base_db";
        String groupId = "ods_db_group";
        DataStreamSource<String> source = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer(topic, groupId));
        // 将数据转化成 json 对象 并过滤脏数据
        SingleOutputStreamOperator<JSONObject> jsonObjectDStream = source.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String data = jsonObject.getString("data");
                    //System.out.println(data);
                    if (data != null && data.length() > 0){
                        out.collect(jsonObject);
                    }


                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        //jsonObjectDStream.print();

        // TODO 编写操作读取配置表形成广播流
        DebeziumSourceFunction<String> tableProcessSourceFunction = MySQLSource.<String>builder()
                .hostname("node1")
                .port(3306)
                .username("root")
                .password("111111")
                .databaseList("gmall2020_realtime")
                .tableList("gmall2020_realtime.table_process")
                .deserializer(new TableProcessStreamDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> tableProcessDStream = env.addSource(tableProcessSourceFunction);

        //7.将配置信息流作为广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("table-process-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessDStream.broadcast(mapStateDescriptor);

        //tableProcessDStream.print();

        // TODO 将主流和广播流进行连接
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjectDStream.connect(broadcastStream);

        //TODO 根据广播流中发送来的数据将主流分为 Kafka事实数据流 和 HBase维度数据流
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE) {
        };
        SingleOutputStreamOperator<JSONObject> kafkaDStream
                = connectedStream.process(new TableProcessFunction(hbaseTag, mapStateDescriptor));

        DataStream<JSONObject> hbaseDStream = kafkaDStream.getSideOutput(hbaseTag);


        kafkaDStream.addSink(MyKafkaUtils.getKafkaProducerWithSchema(new KafkaSerializationSchema<JSONObject>() {
            //element:{"sinkTable":"dwd_xxx","database":"","tableName":"","type":"","data":{"id":"1",...},"before":{"id":"1001",...}}
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {

                return new ProducerRecord<>(jsonObject.getString("sinkTable"),
                        jsonObject.getString("data").getBytes());
            }
        }));

        hbaseDStream.addSink(new DimSink());

        env.execute();
    }
}
