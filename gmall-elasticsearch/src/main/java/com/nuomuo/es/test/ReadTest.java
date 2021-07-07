package com.nuomuo.es.test;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ReadTest {
    public static void main(String[] args) throws IOException {
        // 1.创建 ES 客户端连接池
        JestClientFactory factory = new JestClientFactory();

        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://node1:9200").build();
        factory.setHttpClientConfig(httpClientConfig);
        JestClient jestClient = factory.getObject();


        Search search = new Search.Builder("{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"sex\": \"1\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"must\": [\n" +
                "        {\n" +
                "          \"match\": {\n" +
                "            \"favo\": \"篮球\"\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  },\n" +
                "  \"aggs\": {\n" +
                "    \"groupBySex\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"id\",\n" +
                "        \"size\": 10\n" +
                "      },\n" +
                "      \"aggs\": {\n" +
                "        \"maxAge\": {\n" +
                "          \"max\": {\n" +
                "            \"field\": \"age\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}")
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
