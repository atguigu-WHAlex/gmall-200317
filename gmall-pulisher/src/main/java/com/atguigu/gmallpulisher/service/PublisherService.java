package com.atguigu.gmallpulisher.service;

import java.util.Map;

public interface PublisherService {

    public Integer getDauTotal(String date);

    public Map getDauTotalHourMap(String date);

    public Double getOrderAmountTotal(String date);

    public Map getOrderAmountHourMap(String date);

}
