package app.dws;

import bean.VisitorStats;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.ClickHouseUtil;
import utils.DateTimeUtil;
import utils.MyKafkaUtils;

import java.time.Duration;
import java.util.Date;

public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // TODO 1.设置 CK & 状态后端
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointTimeout(5000L);
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-210108/cdc/ck"));

        //TODO 2.读取 Kafka 主题创建流并转换为JSON对象
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        String groupId = "VisitorStatsApp";
        SingleOutputStreamOperator<JSONObject> uvKafkaDS = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer(uniqueVisitSourceTopic, groupId))
                .map(JSONObject::parseObject);
        SingleOutputStreamOperator<JSONObject> ujKafkaDS = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer(userJumpDetailSourceTopic, groupId))
                .map(JSONObject::parseObject);
        SingleOutputStreamOperator<JSONObject> pageKafkaDS = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer(pageViewSourceTopic, groupId))
                .map(JSONObject::parseObject);


        SingleOutputStreamOperator<VisitorStats> visitorStatsUvDS = uvKafkaDS.map(json -> {
            //获取公共字段
            JSONObject common = json.getJSONObject("common");

            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    json.getLong("ts"));
        });

        //3.2 ujKafkaDS
        SingleOutputStreamOperator<VisitorStats> visitorStatsUjDS = ujKafkaDS.map(json -> {
            //获取公共字段
            JSONObject common = json.getJSONObject("common");

            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 1L, 0L,
                    json.getLong("ts"));
        });

        //3.3 pageKafkaDS
        SingleOutputStreamOperator<VisitorStats> visitorStatsPageDS = pageKafkaDS.map(json -> {
            //获取公共字段
            JSONObject common = json.getJSONObject("common");

            //获取访问时间
            JSONObject page = json.getJSONObject("page");
            Long durTime = page.getLong("during_time");

            long sv = 0L;
            String last_page_id = page.getString("last_page_id");
            if (last_page_id == null) {
                sv = 1L;
            }

            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 1L, sv, 0L, durTime,
                    json.getLong("ts"));
        });


        //TODO 4.Union多个流
        DataStream<VisitorStats> unionDS = visitorStatsUvDS.union(visitorStatsUjDS,
                visitorStatsPageDS);

        //TODO 5.提取时间戳生成Watermark
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWmDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        //TODO 6.分组,开窗,聚合
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = visitorStatsWithWmDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return new Tuple4<>(value.getCh(),
                        value.getAr(),
                        value.getIs_new(),
                        value.getVc());
            }
        });

        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowedStream =
                keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));


        SingleOutputStreamOperator<VisitorStats> resultDStream = windowedStream.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                return new VisitorStats("", "",
                        value1.getVc(),
                        value1.getCh(),
                        value1.getAr(),
                        value1.getIs_new(),
                        value1.getUv_ct() + value2.getUv_ct(),
                        value1.getPv_ct() + value2.getPv_ct(),
                        value1.getSv_ct() + value2.getSv_ct(),
                        value1.getUj_ct() + value2.getUj_ct(),
                        value1.getDur_sum() + value2.getDur_sum(),
                        value2.getTs());
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                //获取窗口的开始和结束时间
                long start = window.getStart();
                long end = window.getEnd();

                //获取聚合后的数据
                VisitorStats visitorStats = input.iterator().next();

                //补充字段
                visitorStats.setStt(DateTimeUtil.toYMDhms(new Date(start)));
                visitorStats.setEdt(DateTimeUtil.toYMDhms(new Date(end)));

                out.collect(visitorStats);
            }
        });
        //TODO 7.写入ClickHouse
        resultDStream.addSink(ClickHouseUtil.getClickHouseSink("insert into visitor_stats values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 8.启动
        env.execute();

    }
}
