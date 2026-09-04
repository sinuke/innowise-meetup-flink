package com.innowise.pubmed.simple.parser.utils;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@UtilityClass
@Slf4j
public class Utils {

    public StreamExecutionEnvironment createEnv(int parallelism, int port) {
        var config = new Configuration();
        if (port != 0) config.setString("rest.port", String.valueOf(port));
        var env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        if (parallelism != 0) env.setParallelism(parallelism);
        return env;
    }

    public void printResults(JobExecutionResult result) {
        log.info("************************************************");
        log.info("Processing time {} seconds", result.getNetRuntime() / 1000.0);
        log.info("************************************************");
    }

    public String appendTimestamp(String path) {
        return String.format("%s%s%d", path, path.endsWith("/") ? "" : "/", System.currentTimeMillis());
    }

}
