package com.nuomuo.gmallwebapi.service.impl;

import com.nuomuo.gmallwebapi.mapper.DauMapper;
import com.nuomuo.gmallwebapi.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    DauMapper dauMapper;


    @Override
    public int getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauHours(String date) {

        //从mapper层获取数据
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //创建map集合存放结果数据
        HashMap<String, Long> result = new HashMap<>();

        for (Map map : list) {
            result.put((String) map.get("LH"), (Long) map.get("CT"));
        }

        return result;
    }
}
