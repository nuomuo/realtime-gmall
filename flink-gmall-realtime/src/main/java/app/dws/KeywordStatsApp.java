package app.dws;

import bean.KeywordStats;
import common.GmallConstant;
import func.KeywordUDTF;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import utils.ClickHouseUtil;
import utils.MyKafkaUtils;

/**
 * 搜索关键字计算
 */
public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 0.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);
        /*
        //CK相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop102:8020/gmall/flink/checkpoint/ProvinceStatsSqlApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","atguigu");
        */
        //TODO 1.定义Table流环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);




        //TODO 2.注册自定义函数
        tableEnv.createTemporarySystemFunction("ik_analyze",  KeywordUDTF.class);

        //TODO 3.将数据源定义为动态表
        String groupId = "KeywordStatsApp";
        String pageViewSourceTopic ="dwd_page_log";

        tableEnv.executeSql("CREATE TABLE page_view " +
                "(common MAP<STRING,STRING>, " +
                "page MAP<STRING,STRING>,ts BIGINT, " +
                "rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')) ," +
                "WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND) " +
                "WITH ("+ MyKafkaUtils.getKafkaDDL(pageViewSourceTopic,groupId)+")");


        //TODO 3.过滤出搜索的数据
        Table fullwordTable = tableEnv.sqlQuery("" +
                "select " +
                "   page['item'] fullword, " +
                "   ts, " +
                "   rowtime " +
                "from page_log " +
                "where page['last_page_id'] = 'search' " +
                "and page['item_type'] = 'keyword' " +
                "and page['item'] is not null");

        //TODO 4.将搜索的关键词进行分词处理
        Table splitWordTable = tableEnv.sqlQuery("SELECT word,ts,rowtime " +
                "FROM " + fullwordTable + ", LATERAL TABLE(ik_analyze(fullword))");

        //TODO 5.分组、开窗、聚合
        Table result = tableEnv.sqlQuery("select " +
                "'" + GmallConstant.KEYWORD_SEARCH + "' source," +
                "   DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "   DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                "   word keyword, " +
                "   count(*) ct, " +
                "   max(ts) ts " +
                "from " + splitWordTable + " " +
                "group by TUMBLE(rowtime, INTERVAL '10' SECOND),word");

        //TODO 6.将动态表转换为流
        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(result, KeywordStats.class);

        //TODO 7.将数据写入ClickHouse
        keywordStatsDataStream.print(">>>>>>>>>>");
        keywordStatsDataStream.addSink(ClickHouseUtil.getClickHouseSink("insert into keyword_stats(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));


        env.execute();

    }
}
