package com.innowise.pubmed.simple.parser.functions;

import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class CountReduceFunction implements ReduceFunction<Tuple2<String, Integer>> {

    @Override
    @SneakyThrows
    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
        return new Tuple2<String, Integer>(value1.f0, value1.f1 + value2.f1);
    }

}
