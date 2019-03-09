package com.hsh.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author hushihai
 * @version V1.0, 2019/3/9
 */
public  class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
    private static final long serialVersionUID = -6440114959851522691L;

    @Override
    public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
        String regex = "\\s+";
        for (String word: sentence.split(regex)) {
            out.collect(new Tuple2<>(word, 1));
        }
        System.out.println(out);
    }
}