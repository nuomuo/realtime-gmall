package app.dws;

import bean.ProvinceStats;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import utils.ClickHouseUtil;
import utils.MyKafkaUtils;

public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //如果读取的是Kafka中数据,则需要与Kafka的分区数保持一致
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //设置CK & 状态后端
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(5000L);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-210108/cdc/ck"));

        //TODO 2.通过DDL方式读取Kafka主题数据,订单宽表,指定时间戳生成WaterMark
        String groupId = "province_stats_210108";
        String topic = "dwm_order_wide";
        tableEnv.executeSql("CREATE TABLE order_wide (  " +
                "  `province_id` BIGINT,  " +
                "  `province_name` STRING,  " +
                "  `province_area_code` STRING,  " +
                "  `province_iso_code` STRING,  " +
                "  `province_3166_2_code` STRING,  " +
                "  `order_id` BIGINT,  " +
                "  `total_amount` DECIMAL,  " +
                "  `create_time` STRING,  " +
                "  `rowtime` as TO_TIMESTAMP(create_time),  " +
                "  WATERMARK FOR rowtime AS rowtime  " +
                ") with" + MyKafkaUtils.getKafkaDDL(topic, groupId));

        //TODO 3.开窗  计算每个地区订单数及订单总金额
        Table table = tableEnv.sqlQuery("select " +
                "   DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "   DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                "   province_id,  " +
                "   province_name, " +
                "   province_area_code, " +
                "   province_iso_code, " +
                "   province_3166_2_code, " +
                "   count(distinct order_id) order_count, " +
                "   sum(total_amount) order_amount, " +
                "   UNIX_TIMESTAMP()*1000 ts " +
                "from order_wide " +
                "group by TUMBLE(rowtime, INTERVAL '10' SECOND), " +
                "   province_id," +
                "   province_name, " +
                "   province_area_code, " +
                "   province_iso_code, " +
                "   province_3166_2_code");

        //TODO 4.将动态表转换为流
        DataStream<ProvinceStats> provinceStatsDataStream = tableEnv.toAppendStream(table, ProvinceStats.class);

        //TODO 5.写出数据到ClickHouse
        provinceStatsDataStream.print(">>>>>>>>");
        provinceStatsDataStream.addSink(ClickHouseUtil.getClickHouseSink("insert into province_stats_210108 values(?,?,?,?,?,?,?,?,?,?)"));

        //TODO 6.启动任务
        env.execute();

    }
}
