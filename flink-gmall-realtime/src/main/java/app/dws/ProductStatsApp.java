package app.dws;

import bean.OrderWide;
import bean.PaymentWide;
import bean.ProductStats;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import common.GmallConstant;
import func.AsyncDimFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import utils.ClickHouseUtil;
import utils.DateTimeUtil;
import utils.MyKafkaUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class ProductStatsApp {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //如果读取的是Kafka中数据,则需要与Kafka的分区数保持一致
        env.setParallelism(1);

        //设置CK & 状态后端
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(5000L);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-210108/cdc/ck"));

        //TODO 2.读取Kafka7个主题数据创建流
        String groupId = "ProductStatsApp";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtils.getFlinkKafkaConsumer(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtils.getFlinkKafkaConsumer(orderWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> paymentWideSource = MyKafkaUtils.getFlinkKafkaConsumer(paymentWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> favorInfoSourceSouce = MyKafkaUtils.getFlinkKafkaConsumer(favorInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> cartInfoSource = MyKafkaUtils.getFlinkKafkaConsumer(cartInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> refundInfoSource = MyKafkaUtils.getFlinkKafkaConsumer(refundInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> commentInfoSource = MyKafkaUtils.getFlinkKafkaConsumer(commentInfoSourceTopic, groupId);

        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);
        DataStreamSource<String> favorInfoDStream = env.addSource(favorInfoSourceSouce);
        DataStreamSource<String> orderWideDStream = env.addSource(orderWideSource);
        DataStreamSource<String> paymentWideDStream = env.addSource(paymentWideSource);
        DataStreamSource<String> cartInfoDStream = env.addSource(cartInfoSource);
        DataStreamSource<String> refundInfoDStream = env.addSource(refundInfoSource);
        DataStreamSource<String> commentInfoDStream = env.addSource(commentInfoSource);

        //TODO 3.统一数据格式
        //3.1 pageViewDStream  点击&曝光
        SingleOutputStreamOperator<ProductStats> productStatsWithClickAndDisplayDS = pageViewDStream.process(new ProcessFunction<String, ProductStats>() {
            @Override
            public void processElement(String value, Context ctx, Collector<ProductStats> out) throws Exception {

                //a.将数据转换为JSON对象
                JSONObject jsonObject = JSONObject.parseObject(value);

                //b.判断是否为点击数据,获取页面信息
                //{"during_time":5289,"item":"3","item_type":"sku_id","last_page_id":"good_list","page_id":"good_detail","source_type":"promotion"}
                Long ts = jsonObject.getLong("ts");
                System.out.println("行为：" + ts);
                JSONObject page = jsonObject.getJSONObject("page");
                if ("good_detail".equals(page.getString("page_id")) && "sku_id".equals(page.getString("item_type"))) {
                    out.collect(ProductStats.builder()
                            .sku_id(page.getLong("item"))
                            .click_ct(1L)
                            .ts(ts)
                            .build());
                }

                //c.判断是否为曝光数据
                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {
                    //遍历曝光数据
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject display = displays.getJSONObject(i);

                        if ("sku_id".equals(display.getString("item_type"))) {
                            out.collect(ProductStats.builder()
                                    .sku_id(display.getLong("item"))
                                    .display_ct(1L)
                                    .ts(ts)
                                    .build());
                        }
                    }
                }
            }
        });

        //3.2 favorInfoDStream 收藏
        SingleOutputStreamOperator<ProductStats> productStatsWithFavorDS = favorInfoDStream.map(line -> {
            //将数据转换为JSON对象
            JSONObject jsonObject = JSONObject.parseObject(line);
            //写出数据
            System.out.println("业务：" + DateTimeUtil.toTs(jsonObject.getString("create_time")));
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .favor_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        //3.3 cartInfoDStream 购物车数据
        SingleOutputStreamOperator<ProductStats> productStatsWithCartDS = cartInfoDStream.map(line -> {
            //将数据转换为JSON对象
            JSONObject jsonObject = JSONObject.parseObject(line);
            //写出数据
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .cart_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        //3.4 orderWideDStream 下单数据
        SingleOutputStreamOperator<ProductStats> productStatsWithOrderDS = orderWideDStream.map(line -> {

            //将数据转换为JavaBean
            OrderWide orderWide = JSONObject.parseObject(line, OrderWide.class);

            //写出数据
            HashSet<Long> orderIdSet = new HashSet<>();
            orderIdSet.add(orderWide.getOrder_id());
            return ProductStats.builder()
                    .sku_id(orderWide.getSku_id())
                    .order_amount(orderWide.getTotal_amount())
                    .order_sku_num(orderWide.getSku_num())
                    .orderIdSet(orderIdSet)
                    .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                    .build();
        });

        //3.5  paymentWideDStream 支付数据
        SingleOutputStreamOperator<ProductStats> productStatsWithPaymentDS = paymentWideDStream.map(line -> {

            //将数据转换为支付宽表
            PaymentWide paymentWide = JSONObject.parseObject(line, PaymentWide.class);

            //写出数据
            HashSet<Long> paydOrderIdSet = new HashSet<>();
            paydOrderIdSet.add(paymentWide.getOrder_id());
            return ProductStats.builder()
                    .sku_id(paymentWide.getSku_id())
                    .payment_amount(paymentWide.getTotal_amount())
                    .paidOrderIdSet(paydOrderIdSet)
                    .ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
                    .build();
        });

        //3.6 refundInfoDStream  退款
        SingleOutputStreamOperator<ProductStats> productStatsWithRefundDS = refundInfoDStream.map(line -> {

            //将数据转换为JSON对象
            JSONObject jsonObject = JSONObject.parseObject(line);

            //写出数据
            HashSet<Long> refundOrderIdSet = new HashSet<>();
            refundOrderIdSet.add(jsonObject.getLong("order_id"));
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .refundOrderIdSet(refundOrderIdSet)
                    .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        //3.7 commentInfoDStream 评价数据
        SingleOutputStreamOperator<ProductStats> productStatsWithCommentDS = commentInfoDStream.map(line -> {

            //将数据转换为JSON对象
            JSONObject jsonObject = JSONObject.parseObject(line);

            //取出评价数据
            String appraise = jsonObject.getString("appraise");
            Long good = 0L;
            if (GmallConstant.APPRAISE_GOOD.equals(appraise)) {
                good = 1L;
            }

            //写出数据
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .comment_ct(1L)
                    .good_comment_ct(good)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        //TODO 4.合并7个流
        DataStream<ProductStats> unionDS = productStatsWithClickAndDisplayDS.union(productStatsWithCartDS,
                productStatsWithCommentDS,
                productStatsWithFavorDS,
                productStatsWithOrderDS,
                productStatsWithPaymentDS,
                productStatsWithRefundDS);

        //TODO 5.分组(SKU_ID)
        KeyedStream<ProductStats, Long> keyedStream = unionDS
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                    @Override
                    public long extractTimestamp(ProductStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }))
                .keyBy(ProductStats::getSku_id);

        //TODO 6.开窗聚合
        SingleOutputStreamOperator<ProductStats> result = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                        stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                        stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                        stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                        stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                        stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                        stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                        stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                        stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                        stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                        stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                        stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                        stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                        stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);

                        stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                        stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());
                        return stats1;
                    }
                }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {

                        //取出数据
                        ProductStats productStats = input.iterator().next();

                        //补充窗口开始和结束时间
                        String stt = DateTimeUtil.toYMDhms(new Date(window.getStart()));
                        String edt = DateTimeUtil.toYMDhms(new Date(window.getEnd()));

                        productStats.setStt(stt);
                        productStats.setEdt(edt);

                        //写出数据
                        out.collect(productStats);
                    }
                });

        //TODO 7.关联商品维度信息
        //7.1 SKU
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(result,
                new AsyncDimFunction<ProductStats>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws Exception {

                        BigDecimal price = dimInfo.getBigDecimal("PRICE");
                        String sku_name = dimInfo.getString("SKU_NAME");

                        Long spu_id = dimInfo.getLong("SPU_ID");
                        Long tm_id = dimInfo.getLong("TM_ID");
                        Long category3_id = dimInfo.getLong("CATEGORY3_ID");

                        //补充信息
                        productStats.setSku_price(price);
                        productStats.setSku_name(sku_name);
                        productStats.setSpu_id(spu_id);
                        productStats.setTm_id(tm_id);
                        productStats.setCategory3_id(category3_id);
                    }
                }, 60, TimeUnit.SECONDS);

        //7.2 SPU
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS = AsyncDataStream.unorderedWait(productStatsWithSkuDS,
                new AsyncDimFunction<ProductStats>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSpu_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws Exception {
                        //取出数据
                        String spu_name = dimInfo.getString("SPU_NAME");
                        //补充数据
                        productStats.setSpu_name(spu_name);
                    }
                }, 60, TimeUnit.SECONDS);

        //7.3 TM
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDS = AsyncDataStream.unorderedWait(productStatsWithSpuDS,
                new AsyncDimFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getTm_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws Exception {
                        //取出数据
                        String tm_name = dimInfo.getString("TM_NAME");
                        //补充信息
                        productStats.setTm_name(tm_name);
                    }
                }, 60, TimeUnit.SECONDS);

        //7.4 Category
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS = AsyncDataStream.unorderedWait(productStatsWithTmDS,
                new AsyncDimFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getCategory3_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws Exception {
                        //补充信息
                        productStats.setCategory3_name(dimInfo.getString("NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 8.将数据写入ClickHouse
        productStatsWithCategory3DS.print(">>>>>>>>>>");
        productStatsWithCategory3DS.addSink(ClickHouseUtil.getClickHouseSink("insert into product_stats values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 9.启动任务
        env.execute();
    }
}
