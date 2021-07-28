package utils;

import com.alibaba.fastjson.JSONObject;
import common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String id) {

        //先查询Redis
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        String jsonStr = jedis.get(redisKey);
        if (jsonStr != null) {
            //重置过期时间
            jedis.expire(redisKey, 60 * 60 * 24);
            jedis.close();
            return JSONObject.parseObject(jsonStr);
        }

        //拼接SQL
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id='" + id + "'";
        System.out.println(querySql);

        //查询数据获取返回值
        List<JSONObject> queryList = JdbcUtil.query(connection, querySql, JSONObject.class, false);

        //将数据写入Redis
        JSONObject jsonObject = queryList.get(0);
        jedis.set(redisKey, jsonObject.toJSONString());
        //设置过期时间
        jedis.expire(redisKey, 60 * 60 * 24);
        jedis.close();

        //返回数据
        return jsonObject;

    }

    public static void delRedisDimInfo(String tableName, String id) {

        //拼接RedisKey
        String redisKey = "DIM:" + tableName + ":" + id;

        //删除数据
        Jedis jedis = RedisUtil.getJedis();
        jedis.del(redisKey);

        //释放连接
        jedis.close();

    }

    public static void main(String[] args) throws Exception {

        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        long start = System.currentTimeMillis();
        System.out.println(getDimInfo(connection, "DIM_BASE_TRADEMARK", "56"));
        long second = System.currentTimeMillis();
        System.out.println(getDimInfo(connection, "DIM_BASE_TRADEMARK", "56"));
        long end = System.currentTimeMillis();

        System.out.println(second - start);
        System.out.println(end - second);
    }

}