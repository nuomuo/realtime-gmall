package app.dwm;

import bean.OrderDetail;
import bean.OrderInfo;
import bean.OrderWide;
import com.alibaba.fastjson.JSONObject;
import func.AsyncDimFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import utils.MyKafkaUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

public class OrderWideApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // TODO 0.设置 CHECKPOINT配置和 状态后端
        //env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);
        //env.enableCheckpointing(5000L);

        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "OrderWideApp";

        // TODO 1.从Kafka读取数据转化成javabean 并指定watermark
        SingleOutputStreamOperator<OrderInfo> orderInfoSingleOutputStreamOperator = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer(orderInfoSourceTopic, groupId))
                .map(new MapFunction<String, OrderInfo>() {
                        @Override
                        public OrderInfo map(String value) throws Exception {
                            OrderInfo orderInfo = JSONObject.parseObject(value, OrderInfo.class);
                            // 2020-12-21 14:52:16
                            String create_time = orderInfo.getCreate_time();
                            String[] dateTimeArr = create_time.split(" ");
            
                            orderInfo.setCreate_date(dateTimeArr[0]);
                            orderInfo.setCreate_hour(dateTimeArr[1].split(":")[0]);
            
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            orderInfo.setCreate_ts(sdf.parse(create_time).getTime());
                            //返回数据
                            return orderInfo;
                        }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                        @Override
                        public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                            return element.getCreate_ts();
                        }
                 }));

        SingleOutputStreamOperator<OrderDetail> orderDetailSingleOutputStreamOperator = env.addSource(MyKafkaUtils.getFlinkKafkaConsumer(orderDetailSourceTopic, groupId))
                .map(new MapFunction<String, OrderDetail>() {
                    @Override
                    public OrderDetail map(String value) throws Exception {
                        OrderDetail orderDetail = JSONObject.parseObject(value, OrderDetail.class);
                        String create_time = orderDetail.getCreate_time();
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                        orderDetail.setCreate_ts(sdf.parse(create_time).getTime());

                        return orderDetail;
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));

        // TODO 2. 双流join
        SingleOutputStreamOperator<OrderWide> processedDStream = orderInfoSingleOutputStreamOperator.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailSingleOutputStreamOperator.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left, right));
                    }
                });

        // TODO 3. 从 Hbase读取维度数据，进行关联
        /*
        processedDStream.map(new MapFunction<OrderWide, OrderWide>() {
            @Override
            public OrderWide map(OrderWide value) throws Exception {
                // 3.1 关联用户维度
            }
        })
        */
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(processedDStream,
                new AsyncDimFunction<OrderWide>("DIM_USER_INFO") {
            @Override
            public String getKey(OrderWide orderWide) {
                return orderWide.getUser_id().toString();
            }

            @Override
            public void join(OrderWide orderWide, JSONObject dimInfo) throws Exception {
                //补充用户性别
                String gender = dimInfo.getString("GENDER");
                orderWide.setUser_gender(gender);

                //补充用户年龄
                String birthday = dimInfo.getString("BIRTHDAY");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                long ts = sdf.parse(birthday).getTime();

                Long age = (System.currentTimeMillis() - ts) / (1000 * 60 * 60 * 24 * 365L);
                orderWide.setUser_age(new Integer(age.toString()));
            }
        }, 60, TimeUnit.SECONDS);


        //TODO 5.2关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserDS,
                new AsyncDimFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {

                        //提取维度数据
                        String name = dimInfo.getString("NAME");
                        String area_code = dimInfo.getString("AREA_CODE");
                        String iso_code = dimInfo.getString("ISO_CODE");
                        String code2 = dimInfo.getString("ISO_3166_2");

                        //将数据补充进OrderWide
                        orderWide.setProvince_name(name);
                        orderWide.setProvince_area_code(area_code);
                        orderWide.setProvince_iso_code(iso_code);
                        orderWide.setProvince_3166_2_code(code2);

                    }
                }, 60,
                TimeUnit.SECONDS);

        //TODO 5.3关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(orderWideWithProvinceDS,
                new AsyncDimFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSku_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {

                        //提取数据
                        Long spu_id = dimInfo.getLong("SPU_ID");
                        Long tm_id = dimInfo.getLong("TM_ID");
                        Long category3_id = dimInfo.getLong("CATEGORY3_ID");

                        //赋值
                        orderWide.setSpu_id(spu_id);
                        orderWide.setTm_id(tm_id);
                        orderWide.setCategory3_id(category3_id);

                    }
                }, 60,
                TimeUnit.SECONDS);

        //TODO 5.4关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(orderWideWithSkuDS,
                new AsyncDimFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSpu_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        //获取数据
                        String spu_name = dimInfo.getString("SPU_NAME");

                        //赋值
                        orderWide.setSpu_name(spu_name);
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 5.5关联TradeMark维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS, new AsyncDimFunction<OrderWide>("DIM_BASE_TRADEMARK") {

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 5.6关联Category维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithTmDS, new AsyncDimFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 6.将数据写入Kafka
        orderWideWithCategory3DS
                .map(JSONObject::toJSONString)
                .addSink(MyKafkaUtils.getFlinkKafkaProducer(orderWideSinkTopic));

        //TODO 7.启动
        env.execute();
    }
}
