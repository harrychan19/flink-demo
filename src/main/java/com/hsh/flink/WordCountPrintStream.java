package com.hsh.flink;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * stream 流统计 demo主函数
 * @author hushihai
 * @version V1.0, 2019/3/7
 */
@SuppressWarnings("all")
public class WordCountPrintStream {
     private static final Logger log = LoggerFactory.getLogger(Object.class);

     

    public static void main(String[] args) throws Exception {
        //分析文件中的内容
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> stringStream = env.readTextFile("D://java.txt").setParallelism(1);
        DataSet<Tuple2<String, Integer>> dataStream = stringStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String split : s.split("\\s+")) {
                    collector.collect(new Tuple2<>(split,1));
                }
            }
        }).groupBy(0).sum(1);
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("47.104.158.165", 9200, "http"));
        dataStream.output(new OutputFormat<Tuple2<String, Integer>>() {
            @Override
            public void configure(Configuration configuration) {
                // config nothing
            }

            @Override
            public void open(int i, int i1) throws IOException {
                // open index
                RestClient restClient = RestClient
                        .builder(new HttpHost("47.104.158.165", 9200, "http"))
                        .build();

            }

            @Override
            public void writeRecord(Tuple2<String, Integer> tuple2) throws IOException {
                JSONObject json = new JSONObject();
                json.put("word",tuple2.f0);
                json.put("count",tuple2.f1);
                Requests.indexRequest().index("flink").type("wordCount").source(json);
            }

            @Override
            public void close() throws IOException {
                //出错了
                log.error("程序出错了");
            }
        });
        //设置程序名称
        env.execute("Window WordCount");
    }
}