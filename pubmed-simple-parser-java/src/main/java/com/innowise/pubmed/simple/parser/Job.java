package com.innowise.pubmed.simple.parser;

import com.innowise.pubmed.simple.parser.functions.CountReduceFunction;
import com.innowise.pubmed.simple.parser.sink.CSVStringEncoderWithHeader;
import com.innowise.pubmed.simple.parser.source.PubMedStreamFormat;
import com.innowise.pubmed.simple.parser.utils.Utils;
import lombok.Builder;
import lombok.SneakyThrows;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;

@Builder
public class Job {

    private StreamExecutionEnvironment env;

    @SneakyThrows
    public JobExecutionResult run(String inputPath, String outputPath) {
        var inputStream = env.fromSource(
                FileSource
                        .forRecordStreamFormat(new PubMedStreamFormat(), new Path(inputPath))
                        .build(),
                WatermarkStrategy.noWatermarks(),
                "Parse PubMed records from raw XML files"
        );

        var calculatedStream = inputStream
                .filter(rec -> !rec.issn().isEmpty())
                .name("Filter out records without ISSN values")
                .map(rec -> new Tuple2<>(rec.issn(), 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .name("Create a tuples with ISSN value and 1 to make counts")
                .keyBy(t -> t.f0)
                .reduce(new CountReduceFunction())
                .name("Count records for each unique ISSN");

        var csvReadyStream = calculatedStream
                .map(rec -> rec.f0 + "," + rec.f1, TypeInformation.of(String.class))
                .name("Convert tuples into strings for CSV");

        FileSink<String> sink = FileSink
                .forRowFormat(
                        new Path(Utils.appendTimestamp(outputPath)),
                        new CSVStringEncoderWithHeader<String>("issn,count", "UTF-8")
                )
                .withBucketAssigner(new BasePathBucketAssigner<>())
                .build();

        csvReadyStream
                .sinkTo(sink)
                .name("Saving results as CSV on disk");

        return env.execute("PubMed simple parser");
    }

}
