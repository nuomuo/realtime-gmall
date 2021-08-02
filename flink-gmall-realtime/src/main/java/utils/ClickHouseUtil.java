package utils;

import bean.TransientSink;
import bean.VisitorStats;
import common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {

    public static <T> SinkFunction<T> getClickHouseSink(String sql) {

        return JdbcSink.sink(sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {

                        //通过反射的方式获取所有列信息
                        Field[] fields = t.getClass().getDeclaredFields();

                        //遍历属性,获取每个属性对应的值信息
                        int offset = 0;
                        for (int i = 0; i < fields.length; i++) {

                            //拿到列信息
                            Field field = fields[i];

                            //获取该列上是否存在不需要写出注解
                            TransientSink annotation = field.getAnnotation(TransientSink.class);
                            if (annotation != null) {
                                offset++;
                                continue;
                            }

                            //设置私有属性可访问
                            field.setAccessible(true);

                            try {
                                //获取值
                                Object value = field.get(t);

                                //给预编译SQL赋值
                                preparedStatement.setObject(i + 1 - offset, value);

                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }

                        }

                    }
                }, new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build());

    }
}
