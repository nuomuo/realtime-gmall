package func;

import com.alibaba.fastjson.JSONObject;
import common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import utils.DimUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSink extends RichSinkFunction<JSONObject> {
    //声明连接
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {

        //创建连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //value:{"sinkTable":"dim_xxx","database":"","tableName":"","type":"","data":{"id":"1",...},"before":{"id":"1001",...}}
    //sql:upsert into yy.table_name (id,name,sex) values(xxx,xxx,xxx)
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;
        try {
            //1.封装SQL语句
            String tableName = value.getString("sinkTable");
            JSONObject data = value.getJSONObject("data");
            String upsertSql = genUpsertSql(tableName, data);
            System.out.println(upsertSql);

            //2.编译SQL
            preparedStatement = connection.prepareStatement(upsertSql);

            // 如果当前数据为更新数据,则删除Redis中对应的数据
//            if ("update".equals(value.getString("type"))) {
//                DimUtil.delRedisDimInfo(tableName.toUpperCase(), data.getString("id"));
//            }

            //3.执行插入数据操作
            preparedStatement.execute();
            connection.commit();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }

    }

    /**
     * 拼接 sql 字符串
     * @param tableName 维度数据表名
     * @param data      待写入的数据 ： {"id":"1001","name":"zhangsan","sex":"male"}
     * @return "upsert into GMALL2020_REALTIME.table_name (id,name,sex) values('1001','zhangsan','male')"
     */
    private String genUpsertSql(String tableName, JSONObject data) {

        //构建SQL语句
        StringBuilder sql = new StringBuilder("upsert into ")
                .append(GmallConfig.HBASE_SCHEMA)
                .append(".")
                .append(tableName)
                .append("(");

        //拼接列信息
        Set<String> keySet = data.keySet();
        sql.append(StringUtils.join(keySet, ","))
                .append(") values ('");

        //拼接值信息
        Collection<Object> values = data.values();
        sql.append(StringUtils.join(values, "','"))
                .append("')");

        return sql.toString();
    }
}
