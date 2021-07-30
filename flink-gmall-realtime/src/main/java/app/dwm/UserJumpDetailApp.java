package app.dwm;

import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import utils.MyKafkaUtils;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<String> source = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer("dwd_page_log", "UserJumpDetailApp"));
        SingleOutputStreamOperator<JSONObject> map = source.map(JSONObject::parseObject);

        SingleOutputStreamOperator<JSONObject> ts = map.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts");
                            }
                        }));


        KeyedStream<JSONObject, String> keyedStream = ts.keyBy(json -> json.getJSONObject("common").getString("mid"));


        // 匹配规则
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("begin")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.length() <= 0;
                    }
                })
                .times(2) //注意官方规定的循环模式属于 “宽松近邻” ！
                .consecutive() //注意必须加一个.consecutive()，使其转变为 “严格紧邻” ！
                /*
                .next("end")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.length() <= 0;
                    }
                })
                 */
                .within(Time.seconds(10));
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);


        //TODO 7.提取事件,匹配上的和超时事件都需要提取
        OutputTag<JSONObject> outputTag = new OutputTag<JSONObject>("TimeOut") {
        };
        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(outputTag,
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                        //提取超时事件
                        List<JSONObject> startList = map.get("start");
                        return startList.get(0);
                    }
                }, new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                        //提取匹配上的事件
                        List<JSONObject> startList = map.get("start");
                        return startList.get(0);
                    }
                });

        //TODO 8.将数据写入Kafka
        DataStream<JSONObject> timeOutDS = selectDS.getSideOutput(outputTag);
        DataStream<JSONObject> unionDS = selectDS.union(timeOutDS);

        unionDS.map(JSONAware::toJSONString).addSink(MyKafkaUtils.getFlinkKafkaProducer("dwm_user_jump_detail"));

        //TODO 9.启动
        env.execute();



    }
}
