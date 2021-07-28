package app.dwddim;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import utils.MyKafkaUtils;

public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        // 为 Kafka 主题的分区数
        env.setParallelism(1);

        //env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        //.getCheckpointConfig().setCheckpointTimeout(60000L);

        //System.setProperty("HADOOP_USER_NAME","root");
        //env.setStateBackend(new FsStateBackend("hdfs://node1:8020/gmall/dwd_log/ck"));

        String topic = "ods_base_log";
        String groupId = "ods_dwd_base_log_app";

        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtils.getFlinkKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaSource);
        //source.print();


        // 类型擦除
        // OutputTag<String> dirtyOutputTag = new OutputTag<String>("dirtyOutputTag", TypeInformation.of(String.class)){};
        OutputTag<String> dirtyOutputTag = new OutputTag<String>("dirtyOutputTag"){};
        // 将字符串装换成 json object
        SingleOutputStreamOperator<JSONObject> mapSource = source.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                // 捕获 脏数据 保证程序正常运行 使用 侧输出流捕获脏数据
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                }catch (Exception e){
                    ctx.output(dirtyOutputTag,value);
                }
            }
        });

        DataStream<String> sideOutput = mapSource.getSideOutput(dirtyOutputTag);

        //mapSource.print();

        // TODO 识别新老用户
        KeyedStream<JSONObject, String> keyedStream = mapSource.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });
        SingleOutputStreamOperator<JSONObject> map = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> flagState;

            @Override
            public void open(Configuration parameters) throws Exception {
                flagState = getRuntimeContext().getState(new ValueStateDescriptor<String>("flagState", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {

                String isNew = value.getJSONObject("common").getString("is_new");
                if ("1".equals(isNew)) {
                    String state = flagState.value();
                    if (state != null) {
                        value.getJSONObject("common").put("is_new", "0");
                    } else {
                        flagState.update("isexit");
                    }
                }
                return value;
            }
        });

        // TODO 使用侧输出流对数据进行分流处理 页面日志-主流 启动日志-侧输出流 曝光日志-侧输出流


        OutputTag<JSONObject> startOutputTag = new OutputTag<JSONObject>("startOutputTag") {
        };
        OutputTag<JSONObject> displayOutputTag = new OutputTag<JSONObject>("displayOutputTag") {
        };
        SingleOutputStreamOperator<JSONObject> streamOperator = map.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                // 获取启动相关的数据
                String start = value.getString("start");
                if (start != null && start.length() > 0) {
                    // 将数据写入 启动侧输出流
                    ctx.output(startOutputTag, value);
                } else {
                    out.collect(value);
                    // 获取曝光数据
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {

                        String page_id = value.getJSONObject("page").getString("page_id");
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject jsonObject = displays.getJSONObject(i);
                            jsonObject.put("page_id", page_id);
                            ctx.output(displayOutputTag, jsonObject);
                        }
                    }
                }
            }
        });


        DataStream<JSONObject> startOutputStream = streamOperator.getSideOutput(startOutputTag);
        DataStream<JSONObject> displayOutputStream = streamOperator.getSideOutput(displayOutputTag);

        // startOutputStream.print("start");
        // displayOutputStream.print("display");


        startOutputStream.map((MapFunction<JSONObject, String>) JSONAware::toJSONString)
                .addSink(MyKafkaUtils.getFlinkKafkaProducer("dwd_start_log"));

        displayOutputStream.map((MapFunction<JSONObject, String>) JSONAware::toJSONString)
                .addSink(MyKafkaUtils.getFlinkKafkaProducer("dwd_display_log"));

        streamOperator.map((MapFunction<JSONObject, String>) JSONAware::toJSONString)
                .addSink(MyKafkaUtils.getFlinkKafkaProducer("dwd_page_log"));




        env.execute();

    }
}
