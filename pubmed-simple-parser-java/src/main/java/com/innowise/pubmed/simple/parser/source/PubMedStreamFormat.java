package com.innowise.pubmed.simple.parser.source;

import com.innowise.pubmed.simple.parser.model.PubMedRecord;
import lombok.SneakyThrows;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;

public class PubMedStreamFormat implements StreamFormat<PubMedRecord> {

    @Override
    @SneakyThrows
    public Reader<PubMedRecord> createReader(Configuration config, FSDataInputStream stream, long fileLen, long splitEnd) {
        return new PubMedReader(stream);
    }

    @Override
    @SneakyThrows
    public Reader<PubMedRecord> restoreReader(Configuration config, FSDataInputStream stream, long restoredOffset, long fileLen, long splitEnd){
        throw new UnsupportedOperationException("Offset restoring is not supported for GZIP");
    }

    @Override
    public boolean isSplittable() {
        return false;
    }

    @Override
    public TypeInformation<PubMedRecord> getProducedType() {
        return TypeInformation.of(PubMedRecord.class);
    }

}
