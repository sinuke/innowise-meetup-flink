package com.innowise.pubmed.simple.parser;

import com.innowise.pubmed.simple.parser.utils.Utils;

public class Main {

    private static final String INPUT_DATA_PATH = "../input_raws/pubmed_sample_120K_records/";
    private static final String OUTPUT_DATA_BASE_PATH = "../output_csv/java/";

    public static void main(String[] args) throws Exception {
        var app = Job.builder()
                .parallelism(1)
                .port(8082)
                .build();
        
        var result = app.run(
//                INPUT_DATA_PATH,
                "../input_raws/full_pubmed_baseline_2026/",
                OUTPUT_DATA_BASE_PATH
        );

        Utils.printResults(result);
    }

}
