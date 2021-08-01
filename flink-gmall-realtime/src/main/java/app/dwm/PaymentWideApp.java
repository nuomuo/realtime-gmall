package app.dwm;

import bean.OrderWide;
import bean.PaymentInfo;
import bean.PaymentWide;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import utils.MyKafkaUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //设置CK & 状态后端
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointTimeout(5000L);
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-210108/cdc/ck"));

        //TODO 2.读取Kafka主题数据并转换为JavaBean对象
        String groupId = "PaymentWideApp";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        SingleOutputStreamOperator<PaymentInfo> paymentKafkaDS = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer(paymentInfoSourceTopic, groupId))
                .map(line -> JSONObject.parseObject(line, PaymentInfo.class));
        SingleOutputStreamOperator<OrderWide> orderWideKafkaDS = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer(orderWideSourceTopic, groupId))
                .map(new MapFunction<String, OrderWide>() {
                    @Override
                    public OrderWide map(String value) throws Exception {
                        return JSONObject.parseObject(value, OrderWide.class);
                    }
                });

        //TODO 3.提取时间戳生成WaterMark
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentKafkaDS.assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                    @Override
                    public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            return sdf.parse(element.getCreate_time()).getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                            return recordTimestamp;
                        }
                    }
                }));
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideKafkaDS.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                    @Override
                    public long extractTimestamp(OrderWide element, long recordTimestamp) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            return sdf.parse(element.getCreate_time()).getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                            return recordTimestamp;
                        }
                    }
                }));

        //TODO 4.双流JOIN
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoDS.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideDS.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {

                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

        //TODO 5.将数据写入Kafka
        paymentWideDS.print(">>>>>>>");
        paymentWideDS
                .map(JSONObject::toJSONString)
                .addSink(MyKafkaUtils.getFlinkKafkaProducer(paymentWideSinkTopic));

        //TODO 6.执行
        env.execute();
    }
}
