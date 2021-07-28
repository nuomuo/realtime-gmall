package func;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * 自定义序列化器 自定义 处理流输出数据格式
 */
public class TableProcessStreamDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {
    /**
     * {
     * "database":"",
     * "tableName":"",
     * "data":"{"id":"1001","name":"zhangsan"}",
     * "before":"",
     * "type":"insert"
     * }
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

        //创建结果JSON
        JSONObject result = new JSONObject();

        //取出数据库&表名
        String topic = sourceRecord.topic();
        String[] split = topic.split("\\.");
        String database = split[1];
        String tableName = split[2];

        //取出数据本身
        Struct value = (Struct) sourceRecord.value();
        Struct after = value.getStruct("after");
        JSONObject afterData = new JSONObject();
        if (after != null) {
            Schema schema = after.schema();
            List<Field> fields = schema.fields();
            for (Field field : fields) {
                afterData.put(field.name(), after.get(field));
            }
        }

        Struct before = value.getStruct("before");
        JSONObject beforeData = new JSONObject();
        if (before != null) {
            Schema schema = before.schema();
            List<Field> fields = schema.fields();
            for (Field field : fields) {
                beforeData.put(field.name(), before.get(field));
            }
        }

        //获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }

        //补充字段
        result.put("database", database);
        result.put("tableName", tableName);
        result.put("data", afterData);
        result.put("before", beforeData);
        result.put("type", type);

        //输出数据
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
