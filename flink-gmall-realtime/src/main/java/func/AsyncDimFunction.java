package func;

import com.alibaba.fastjson.JSONObject;
import common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import utils.DimUtil;
import utils.JdbcUtil;
import utils.ThreadPoolUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class AsyncDimFunction<T> extends RichAsyncFunction<T,T> implements AsyncJoinFunction<T> {

    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;

    private String tableName;

    public AsyncDimFunction(String tableName){
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        threadPoolExecutor = ThreadPoolUtil.getInstance();
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        //打印超时数据,一旦发生,需要检查维度表中是否存在你查询的信息
        System.out.println("TimeOut:" + input);
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {

        threadPoolExecutor.submit(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                String key = getKey(input);

                JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, key);

                join(input,dimInfo);

                resultFuture.complete(Collections.singletonList(input));

            }
        });



    }
}
