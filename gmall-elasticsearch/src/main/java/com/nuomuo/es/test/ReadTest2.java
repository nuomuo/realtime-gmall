package com.nuomuo.es.test;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ReadTest2 {
    public static void main(String[] args) throws IOException {
        // 1.创建 ES 客户端连接池
        JestClientFactory factory = new JestClientFactory();

        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://node1:9200").build();
        factory.setHttpClientConfig(httpClientConfig);
        JestClient jestClient = factory.getObject();


        //5.编写查询语句
        // 类似“{}”
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //---------------------------query-----------------------------
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("sex","1");
        boolQueryBuilder.filter(termQueryBuilder);
        sourceBuilder.query(boolQueryBuilder);
        //---------------------------must-----------------------------
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("favo","篮球");
        boolQueryBuilder.must(matchQueryBuilder);
        //---------------------------aggs-----------------------------
        TermsAggregationBuilder groupByClass1 = AggregationBuilders.terms("groupBySex").field("id").size(10);

        MaxAggregationBuilder maxAge1 = AggregationBuilders.max("maxAge").field("age");

        sourceBuilder.aggregation(groupByClass1.subAggregation(maxAge1));
//        sourceBuilder.aggregation(maxAge1);

        sourceBuilder.from(0);
        sourceBuilder.size(2);
        //6.执行查询
        Search search = new Search.Builder(sourceBuilder.toString())
                .addIndex("student_index")
                .addType("_doc")
                .build();


        SearchResult searchResult = jestClient.execute(search);

        System.out.println(searchResult.getTotal());
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);

        for (SearchResult.Hit<Map, Void> hit : hits) {
            System.out.println(hit.score);

            Map source = hit.source;
            Set keySet = source.keySet();
            for (Object o : keySet) {
                System.out.println(o+":"+source.get(o));
            }


        }

        MetricAggregation aggregations = searchResult.getAggregations();
        TermsAggregation groupBySex = aggregations.getTermsAggregation("groupBySex");
        List<TermsAggregation.Entry> buckets = groupBySex.getBuckets();
        for (TermsAggregation.Entry bucket : buckets) {
            System.out.println(bucket.getKey());

            MaxAggregation maxAge = bucket.getMaxAggregation("maxAge");
            Double max = maxAge.getMax();
        }


        jestClient.shutdownClient();

    }
}
