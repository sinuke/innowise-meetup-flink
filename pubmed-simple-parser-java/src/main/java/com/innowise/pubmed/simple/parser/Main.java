package com.innowise.pubmed.simple.parser;

import com.innowise.pubmed.simple.parser.utils.Utils;

public class Main {

    private static final String INPUT_DATA_PATH = "../input_raws/pubmed_sample_120K_records/";
    private static final String OUTPUT_DATA_BASE_PATH = "../output_csv/java/";
    private static final int PARALLELISM = 1;
    private static final int PORT = 8082;

    public static void main(String[] args) throws Exception {
        var env = Utils.createEnv(PARALLELISM, PORT);

        var app = Job.builder()
                .env(env)
                .build();
        
        var result = app.run(
//                INPUT_DATA_PATH,
                "../input_raws/full_pubmed_baseline_2026/",
                OUTPUT_DATA_BASE_PATH
        );

        Utils.printResults(result);
    }

}
