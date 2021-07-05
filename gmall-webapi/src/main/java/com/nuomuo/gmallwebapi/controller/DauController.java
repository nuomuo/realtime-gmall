package com.nuomuo.gmallwebapi.controller;

import com.alibaba.fastjson.JSONObject;
import com.nuomuo.gmallwebapi.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class DauController {

    @Autowired
    private PublisherService publisherService;


    /**
     * 获得一天的 访问数
     * @param date 具体的哪一天
     * @return 一个 json 字符串
     */
    @RequestMapping("realtime-total")
    public String realtimeTotal(@RequestParam("date") String date) {
        //从 service层获取日活总数数据
        Integer dauTotal = publisherService.getDauTotal(date);

        //创建list集合存放最终数据
        ArrayList<HashMap<String,Object>> result = new ArrayList<>();

        //创建存放新增日活的map集合
        HashMap<String, Object> dauMap = new HashMap<>();

        //创建存放新增设备的map集合
        HashMap<String, Object> devMap = new HashMap<>();

        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        devMap.put("id", "new_mid");
        devMap.put("name", "新增设备");
        devMap.put("value", 233);

        result.add(dauMap);
        result.add(devMap);

        return JSONObject.toJSONString(result);
    }

    /**
     * 封装分时数据 根据
     * @param id
     * @param date
     * @return
     */
    /*
    @RequestMapping("realtime-hours")
    public String realtimeHours(@RequestParam("id") String id,
                                @RequestParam("date") String date) {

        //创建map集合用于存放结果数据
        HashMap<String, Object> result = new HashMap<>();

        result.put("yesterday", yesterdayHourMap);
        result.put("today", todayHourMap);

        return JSONObject.toJSONString(result);
    }
     */

}
