package com.innowise.pubmed.simple.parser.sink;

import org.apache.flink.api.common.serialization.Encoder;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

public class CSVStringEncoderWithHeader<IN> implements Encoder<IN> {

    private String charsetName;
    private String header;
    private transient Charset charset;
    private boolean isHeaderAdded = false;

    public CSVStringEncoderWithHeader(String header, String charsetName) {
        this.header = header;
        this.charsetName = charsetName;
    }

    @Override
    public void encode(IN element, OutputStream stream) throws IOException {
        if (charset == null) {
            charset = Charset.forName(charsetName);
        }

        if (!isHeaderAdded) {
            stream.write(header.getBytes(charset));
            stream.write('\n');
            isHeaderAdded = true;
        }

        stream.write(element.toString().getBytes(charset));
        stream.write('\n');
    }

}
