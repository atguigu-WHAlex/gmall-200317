package com.atguigu.gmallpulisher.service.impl;

import com.atguigu.gmallpulisher.bean.Option;
import com.atguigu.gmallpulisher.bean.Stat;
import com.atguigu.gmallpulisher.mapper.DauMapper;
import com.atguigu.gmallpulisher.mapper.OrderMapper;
import com.atguigu.gmallpulisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;
    @Autowired
    private OrderMapper orderMapper;
    @Autowired
    private JestClient jestClient;

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

    @Override
    public Map getSaleDetail(String date, int startpage, int size, String keyword) {

        //1.创建DSL语句构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //1.1 添加查询条件
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        //1.1.1 条件全值匹配选项
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("dt", date);
        boolQueryBuilder.filter(termQueryBuilder);
        //1.1.2 条件分词匹配选项
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("sku_name", keyword).operator(MatchQueryBuilder.Operator.AND);
        boolQueryBuilder.must(matchQueryBuilder);
        searchSourceBuilder.query(boolQueryBuilder);

        //1.2 添加聚合组
        //1.2.1 年龄聚合组
        TermsBuilder ageAggs = AggregationBuilders.terms("groupByAge").field("user_age").size(100);
        searchSourceBuilder.aggregation(ageAggs);
        //1.2.2 性别聚合组
        TermsBuilder genderAggs = AggregationBuilders.terms("groupByGender").field("user_gender").size(3);
        searchSourceBuilder.aggregation(genderAggs);

        //1.3 分页相关
        searchSourceBuilder.from((startpage - 1) * size);
        searchSourceBuilder.size(size);

        //2.创建Search对象
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("gmall200317_sale_detail-query")
                .addType("_doc")
                .build();

        //3.执行查询
        SearchResult searchResult = null;
        try {
            searchResult = jestClient.execute(search);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //4.解析searchResult
        //4.1 定义Map用于存放最终的结果数据
        HashMap<String, Object> result = new HashMap<>();

        //4.2 获取查询总数
        assert searchResult != null;
        Long total = searchResult.getTotal();

        //4.3 封装年龄以及性别聚合组数据
        MetricAggregation aggregations = searchResult.getAggregations();
        ArrayList<Stat> stats = new ArrayList<>();

        //4.3.1 年龄聚合组
        TermsAggregation groupByAge = aggregations.getTermsAggregation("groupByAge");

        //定义每个年龄段的人数变量
        Long lower20 = 0L;
        Long upper20to30 = 0L;

        //遍历年龄数据,将各个年龄的数据转换为各个年龄段的数据
        for (TermsAggregation.Entry entry : groupByAge.getBuckets()) {
            long age = Long.parseLong(entry.getKey());
            if (age < 20) {
                lower20 += entry.getCount();
            } else if (age < 30) {
                upper20to30 += entry.getCount();
            }
        }

        //将人数转换为比例
        double lower20Ratio = (Math.round(lower20 * 1000D / total)) / 10D;
        double upper20to30Ratio = (Math.round(upper20to30 * 1000D / total)) / 10D;
        double upper30Ratio = 100D - lower20Ratio - upper20to30Ratio;

        //创建3个年龄段的Option对象
        Option lower20Opt = new Option("20岁以下", lower20Ratio);
        Option upper20to30Opt = new Option("20岁到30岁", upper20to30Ratio);
        Option upper30Opt = new Option("30岁及30岁以上", upper30Ratio);

        //创建年龄的Stat对象
        ArrayList<Option> ageOptions = new ArrayList<>();
        ageOptions.add(lower20Opt);
        ageOptions.add(upper20to30Opt);
        ageOptions.add(upper30Opt);
        Stat ageStat = new Stat(ageOptions, "用户年龄占比");

        //将年龄的饼图数据添加至集合
        stats.add(ageStat);

        //4.3.2 性别聚合组
        TermsAggregation groupByGender = aggregations.getTermsAggregation("groupByGender");
        Long femaleCount = 0L;

        //遍历聚合组中性别数据
        for (TermsAggregation.Entry entry : groupByGender.getBuckets()) {
            if ("F".equals(entry.getKey())) {
                femaleCount += entry.getCount();
            }
        }

        //计算男女性别比例
        double femaleRatio = (Math.round(femaleCount * 1000D / total)) / 10D;
        double maleRatio = 100D - femaleRatio;

        //创建2个性别Option对象
        Option maleOpt = new Option("男", maleRatio);
        Option femaleOpt = new Option("女", femaleRatio);

        //创建性别的Stat对象
        ArrayList<Option> genderOptions = new ArrayList<>();
        genderOptions.add(maleOpt);
        genderOptions.add(femaleOpt);
        Stat genderStat = new Stat(genderOptions, "用户性别占比");

        //将用户性别饼图数据添加至集合
        stats.add(genderStat);

        //4.4 向结果集中添加明细数据
        ArrayList<Map> details = new ArrayList<>();
        //4.4.1 获取明细数据
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            details.add(hit.source);
        }

        //将准备的数据放入结果集
        result.put("total", total);
        result.put("stat", stats);
        result.put("detail", details);

        //5.将封装好的数据返回
        return result;
    }
}
