package com.hsh.flink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author hushihai
 * @version V1.0, 2019/3/9
 */
public class OrderAdvanceStream {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://192.168.61.61:3306/erp_income_test")
                .setUsername("offcnitc")
                .setPassword("offcnitc_200@test")
                .setQuery("select * from order_advance_income2019")
                .setRowTypeInfo(new RowTypeInfo(
                        TypeInformation.of(Long.class),
                        TypeInformation.of(Long.class),
                        TypeInformation.of(String.class),
                        TypeInformation.of(Long.class),
                        TypeInformation.of(Long.class),
                        TypeInformation.of(String.class),
                        TypeInformation.of(String.class),
                        TypeInformation.of(Integer.class),
                        TypeInformation.of(Integer.class),
                        TypeInformation.of(Integer.class),
                        TypeInformation.of(Integer.class),
                        TypeInformation.of(Integer.class),
                        TypeInformation.of(Boolean.class),
                        TypeInformation.of(String.class),
                        TypeInformation.of(Integer.class),
                        TypeInformation.of(BigDecimal.class),
                        TypeInformation.of(BigDecimal.class),
                        TypeInformation.of(BigDecimal.class),
                        TypeInformation.of(BigDecimal.class),
                        TypeInformation.of(BigDecimal.class)
                ))
                .finish();
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("47.104.158.165", 9200, "http"));
        ElasticsearchSink.Builder<Tuple2<String, Integer>> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Tuple2<String, Integer>>() {
                    private static final long serialVersionUID = 1068840945371070962L;
                    IndexRequest createIndexRequest(Tuple2<String, Integer> element) {
                        Map<String, String> json = new HashMap<>(0);
                        json.put("keyword",element.f0);
                        json.put("count", String.valueOf(element.f1));
                        return Requests.indexRequest().index("flink").type("wordCount").source(json);
                    }
                    @Override
                    public void process(Tuple2<String, Integer> element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );
        DataSource source = env.createInput(jdbcInputFormat);
        source.print();
    }
}
