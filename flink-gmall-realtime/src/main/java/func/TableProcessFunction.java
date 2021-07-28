package func;

import bean.TableProcess;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {

    private OutputTag<JSONObject> outputTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private Connection connection;

    public TableProcessFunction(OutputTag<JSONObject> outputTag,MapStateDescriptor<String, TableProcess> mapStateDescriptor){
        this.outputTag = outputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    // TODO 处理广播流的数据
    // //value:{"database":"","tableName":"","type":"","data":{"source_table":"base_trademark",...},"before":{"id":"1001",...}}
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        // 将字符串数据转化为 TableProcess
        JSONObject jsonObject = JSONObject.parseObject(value);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("data"), TableProcess.class);

        // 如果是
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())){
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());
        }


        //3.写入状态,广播出去
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "_" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);


    }
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //1.获取广播过来的配置信息
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = value.getString("tableName") + "_" + value.getString("type");
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {

            //2.根据配置信息过滤字段
            filterColumn(value.getJSONObject("data"), tableProcess.getSinkColumns());

            //3.分流
            //将sinkTable字段添加至数据中
            value.put("sinkTable", tableProcess.getSinkTable());

            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                //将数据写入侧输出流
                ctx.output(outputTag, value);
            } else if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                //将输出写入主流
                out.collect(value);
            }

        } else {
            System.out.println("该组合Key：" + key + "不存在配置信息！");
        }

    }


    //根据配置信息过滤字段 data：{"id":"1","tm_name":"atguigu","logo":"xxx"}
    private void filterColumn(JSONObject data, String sinkColumns) {

        //将需要保留的字段切分
        String[] columnsArr = sinkColumns.split(",");

        List<String> columnList = Arrays.asList(columnsArr);

        //遍历data数据中的列信息
        Set<Map.Entry<String, Object>> entries = data.entrySet();
//        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
//        while (iterator.hasNext()) {
//            Map.Entry<String, Object> next = iterator.next();
//            if (!columnList.contains(next.getKey())) {
//                iterator.remove();
//            }
//        }

        entries.removeIf(next -> !columnList.contains(next.getKey()));

    }




    // TODO 在 phoenix 中建表
    // create table if not exists yy.xx(aa varchar primary key,bb varchar)
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        PreparedStatement preparedStatement = null;

        try {
            //处理主键和扩展字段,给定默认值
            if (sinkPk == null) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }

            //1.拼接SQL
            StringBuilder createSql = new StringBuilder("create table if not exists ");
            createSql.append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {

                String column = columns[i];
                //判断当前的列是否为主键
                if (sinkPk.equals(column)) {
                    createSql.append(column).append(" varchar primary key ");
                } else {
                    createSql.append(column).append(" varchar ");
                }

                //如果当前字段不是最后一个字段,则需要添加","
                if (i < columns.length - 1) {
                    createSql.append(",");
                }
            }

            //拼接扩展字段
            createSql.append(")").append(sinkExtend);

            //打印建表语句
            System.out.println(createSql.toString());

            //2.编译SQL
            preparedStatement = connection.prepareStatement(createSql.toString());

            //3.执行,建表
            preparedStatement.execute();

        } catch (SQLException e) {
            throw new RuntimeException("Phoenix创建" + sinkTable + "表失败！");
        } finally {

            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
