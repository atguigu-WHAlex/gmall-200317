package com.atguigu.gmallpulisher.service.impl;

import com.atguigu.gmallpulisher.mapper.DauMapper;
import com.atguigu.gmallpulisher.mapper.OrderMapper;
import com.atguigu.gmallpulisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;
    @Autowired
    private OrderMapper orderMapper;

    //获取日活总数
    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    //获取日活分时数据
    @Override
    public Map getDauTotalHourMap(String date) {

        //1.查询Phoenix
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //2.创建Map用于存放调整结构后的数据
        HashMap<String, Long> result = new HashMap<>();

        //3.调整结构
        for (Map map : list) {
            result.put((String) map.get("LH"), (Long) map.get("CT"));
        }

        //4.返回数据
        return result;
    }

    @Override
    public Double getOrderAmountTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHourMap(String date) {

        //1.查询Phoenix
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);

        //2.创建Map用于存放结果数据
        HashMap<String, Double> result = new HashMap<>();

        //3.遍历list,调整结构
        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"), (Double) map.get("SUM_AMOUNT"));
        }

        //4.返回结果
        return result;
    }
}
