package func;

import com.alibaba.fastjson.JSONObject;

public interface AsyncJoinFunction <T>{
    String getKey(T input);

    void join(T input, JSONObject dimInfo) throws Exception;

}
