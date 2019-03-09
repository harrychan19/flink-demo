package com.hsh.flink;

/**
 * stream 流统计 demo主函数
 * @author hushihai
 * @version V1.0, 2019/3/7
 */
public class WordCountPrintStream {

    public static void main(String[] args) throws Exception {
        /*//分析文件中的内容
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStreamSource<String> stringStream = env.readTextFile("D://java.txt").setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream = stringStream.flatMap(new Splitter())
                .keyBy(0)
                .sum(1);
        DataStreamSink<Tuple2<String, Integer>> print = dataStream.print();
        System.out.println(print.toString());
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
        // 必须设置flush参数
        //刷新前缓冲的最大动作量
        esSinkBuilder.setBulkFlushMaxActions(1);
        //刷新前缓冲区的最大数据大小（以MB为单位）
        esSinkBuilder.setBulkFlushMaxSizeMb(500);
        //论缓冲操作的数量或大小如何都要刷新的时间间隔
        esSinkBuilder.setBulkFlushInterval(5000);
        dataStream.addSink(esSinkBuilder.build());
        //设置程序名称
        env.execute("Window WordCount");*/
    }
}