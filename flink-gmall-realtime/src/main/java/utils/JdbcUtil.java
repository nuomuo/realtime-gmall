package utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {
    /**
     *
     * @param connection
     * @param sql
     * @param cls
     * @param underScoreToCamel
     * @param <T>
     * @return
     */
    public static <T> List<T> query(Connection connection, String sql, Class<T> cls, Boolean underScoreToCamel) {

        //创建结果集合
        ArrayList<T> result = new ArrayList<>();

        try {
            //编译SQL
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            //执行查询
            ResultSet resultSet = preparedStatement.executeQuery();

            //解析结果
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            //对查询结果中的行进行遍历
            while (resultSet.next()) {

                T t = cls.newInstance();

                //对列进行遍历
                for (int i = 1; i <= columnCount; i++) {

                    //获取列名
                    String columnName = metaData.getColumnName(i);

                    //如果指定需要下划线转换为驼峰命名方式
                    if (underScoreToCamel) {
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }

                    //获取数据
                    Object value = resultSet.getObject(i);

                    //给T对象属性赋值
                    BeanUtils.setProperty(t, columnName, value);
                }

                //将T对象添加至集合
                result.add(t);

            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        //返回集合
        return result;
    }

    public static void main(String[] args) throws Exception {

//        String name = "aa_bb";
//        System.out.println(CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name));

//        Class.forName("com.mysql.jdbc.Driver");
//        Connection connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall-210108-realtime",
//                "root",
//                "000000");

        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        List<JSONObject> infoList = query(connection,
                "select * from GMALL_REALTIME.DIM_BASE_CATEGORY1",
                JSONObject.class,
                false);

        for (JSONObject orderInfo : infoList) {
            System.out.println(orderInfo);
        }

        connection.close();

    }

}
