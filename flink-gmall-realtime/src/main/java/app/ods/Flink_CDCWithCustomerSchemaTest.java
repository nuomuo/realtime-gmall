package app.ods;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import utils.MyKafkaUtils;

import java.util.List;

public class Flink_CDCWithCustomerSchemaTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        env.enableCheckpointing(5000L);
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink0108/ck"));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        DebeziumSourceFunction<String> source = MySQLSource.<String>builder()
                .hostname("node1")
                .port(3306)
                .username("root")
                .password("111111")
                .databaseList("gmall2020")
                //.tableList("base_trademark")
                // 开始 offset 设置
                .startupOptions(StartupOptions.latest())
                //.deserializer(new StringDebeziumDeserializationSchema())
                .deserializer(new MyJsonStringDeserializationSchema())
                .build();

        DataStreamSource<String> streamSource = env.addSource(source);

        streamSource.print();


        streamSource.addSink(MyKafkaUtils.getFlinkKafkaProducer("ods_base_db"));




        //  Flink sql
        /*
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql("CREATE TABLE base_trademark (\n" +
                " id INT,\n" +
                " name STRING,\n" +
                " description STRING \n" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'hostname' = 'localhost',\n" +
                " 'port' = '3306',\n" +
                " 'username' = 'root',\n" +
                " 'password' = '111111',\n" +
                " 'database-name' = 'gmall2020',\n" +
                " 'table-name' = 'base_trademark'\n" +
                ");");

        tableEnv.executeSql("select * from base_trademark").print();
        */

        env.execute();

    }
    // 自定义序列化器
    public static class MyJsonStringDeserializationSchema implements DebeziumDeserializationSchema<String>{
        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
            // 想清楚数据应该是什么样子的
            // 获取主题信息,包含着数据库和表名
            // SourceRecord{
            //   sourcePartition={server=mysql_binlog_source},
            //   sourceOffset={file=mysql-bin.000009, pos=4004425, row=1, snapshot=true}}
            //   ConnectRecord{
            //        topic='mysql_binlog_source.gmall2020.user_info', kafkaPartition=null, key=Struct{id=3999},
            //        keySchema=Schema{mysql_binlog_source.gmall2020.user_info.Key:STRUCT},
            //        value=Struct{
            //             after=Struct{id=3999,login_name=pvy1won4o,nick_name=厚庆,name=宋厚庆,phone_num=13274778653,email=pvy1won4o@0355.net,user_level=1,birthday=2004-12-04,gender=M,create_time=2020-12-04 23:28:45},
            //             source=Struct{version=1.4.1.Final,connector=mysql,name=mysql_binlog_source,ts_ms=0,snapshot=true,db=gmall2020,table=user_info,server_id=0,file=mysql-bin.000009,pos=4004425,row=0},op=c,ts_ms=1627371008507},
            //             valueSchema=Schema{mysql_binlog_source.gmall2020.user_info.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}

            // mysql_binlog_source.gmall2020.user_info
            //构建结果对象
            JSONObject result = new JSONObject();

            //获取数据库名称&表名称
            String topic = sourceRecord.topic();
            String[] fields = topic.split("\\.");
            String database = fields[1];
            String tableName = fields[2];

            //获取数据
            Struct value = (Struct) sourceRecord.value();

            //After
            Struct after = value.getStruct("after");
            JSONObject data = new JSONObject();
            if (after != null) { //delete数据,则after为null
                Schema schema = after.schema();
                List<Field> fieldList = schema.fields();

                for (int i = 0; i < fieldList.size(); i++) {
                    Field field = fieldList.get(i);
                    Object fieldValue = after.get(field);
                    data.put(field.name(), fieldValue);
                }
            }

            //Before
            Struct before = value.getStruct("before");
            JSONObject beforeData = new JSONObject();
            if (before != null) { //delete数据,则after为null
                Schema schema = before.schema();
                List<Field> fieldList = schema.fields();

                for (int i = 0; i < fieldList.size(); i++) {
                    Field field = fieldList.get(i);
                    Object fieldValue = before.get(field);
                    beforeData.put(field.name(), fieldValue);
                }
            }

            //获取操作类型 CREATE UPDATE DELETE
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);
            String type = operation.toString().toLowerCase();
            if ("create".equals(type)) {
                type = "insert";
            }

            //封装数据
            result.put("database", database);
            result.put("tableName", tableName);
            result.put("data", data);
            result.put("before", beforeData);
            result.put("type", type);
            //result.put("ts", System.currentTimeMillis());

            //输出封装好的数据
            collector.collect(result.toJSONString());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }

}