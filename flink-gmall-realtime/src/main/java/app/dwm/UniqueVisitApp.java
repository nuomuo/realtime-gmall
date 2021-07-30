package app.dwm;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import utils.MyKafkaUtils;

import java.text.SimpleDateFormat;

/**
 * UV 计算
 */
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 1.设置 CHECKPOINT配置和 状态后端
        //env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);

        // TODO 2.从 Kafka 的 dwd_page_log 主题读取数据
        DataStreamSource<String> kafkaSource = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer("dwd_page_log", "UniqueVisitApp"));
        // 过滤掉脏数据以及 将字符串 转化成 JSONObject 脏数据进入侧输出流
        OutputTag<String> dirtyOutput = new OutputTag<String>("dirtyOutput"){};
        SingleOutputStreamOperator<JSONObject> objectDStream = kafkaSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyOutput, value);
                }
            }
        });

        // TODO 3.核心过滤
        KeyedStream<JSONObject, String> keyedStream = objectDStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });
        // 主要目的是从数据中 去重 只保留 唯一一个mid （每一个 mid 创建一个 状态后端）
        SingleOutputStreamOperator<JSONObject> filteredDStream = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            private SimpleDateFormat simpleDateFormat;
            private ValueState<String> valueState;


            @Override
            public void open(Configuration parameters) throws Exception {
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<>("visit-state", String.class);

                //创建状态TTL配置项
                StateTtlConfig stateTtlConfig = StateTtlConfig
                        // 表示状态的过期时间 如果上次访问的时间戳 + TTL 超过了当前时间，则表明状态过期
                        .newBuilder(Time.days(1))
                        //状态时间戳更新时机 ： disable不更新、onCreateAndWrite 当状态创建或每次写入时都会更新时间戳、
                        // OnReadAndWrite 在状态创建和写入时更新时间戳外，读取也会更新状态的时间戳
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        // 对已过期但还未被清理掉的状态如何处理
                        // ReturnExpiredIfNotCleanedUp，那么即使这个状态的时间戳表明它已经过期了，但是只要还未被真正清理掉，就会被返回给调用方；
                        // 如果设置为 NeverReturnExpired，那么一旦这个状态过期了，那么永远不会被返回给调用方，只会返回空状态
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        .build();
                stringValueStateDescriptor.enableTimeToLive(stateTtlConfig);

                valueState = getRuntimeContext().getState(stringValueStateDescriptor);

                //valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("state", Integer.class));

            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                // 取出上一次访问页面 如果 last_page_id 有值(不为null)说明 该数据一定不是初次访问生成的数据 直接过滤掉
                String last_page_id = value.getJSONObject("page").getString("last_page_id");
                if (last_page_id == null || last_page_id.length() <= 0) {
                    // 如果没有值 则 看状态后端有没有值 没有值说明是第一条数据 保留
                    // 有值也可能是第二天的数据了 所有需要将 ts字段转化成 日期存到存到状态后端
                    // 只要不是同一天数据就保留 并且跟新 状态后端
                    String firstVisitTime = valueState.value();

                    Long ts = value.getLong("ts");
                    String date = simpleDateFormat.format(ts);

                    if (firstVisitTime == null || !firstVisitTime.equals(date)) {
                        valueState.update(date);
                        return true;
                    } else {
                        return false;
                    }

                } else {
                    return false;
                }
            }
        });


        filteredDStream.map(new MapFunction<JSONObject, String>() {
            @Override
            public String map(JSONObject value) throws Exception {
                return value.toJSONString();
            }
        }).addSink(MyKafkaUtils.getFlinkKafkaProducer("dwm_unique_visit"));

        env.execute();





    }
}
