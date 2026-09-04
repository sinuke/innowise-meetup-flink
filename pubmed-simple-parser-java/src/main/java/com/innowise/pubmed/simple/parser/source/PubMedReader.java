package com.innowise.pubmed.simple.parser.source;

import com.innowise.pubmed.simple.parser.model.PubMedRecord;
import lombok.SneakyThrows;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.util.ArrayList;
import java.util.List;

public class PubMedReader implements StreamFormat.Reader<PubMedRecord> {
    private final XMLStreamReader reader;

    private final StringBuilder textBuffer = new StringBuilder();
    private final StringBuilder titleBuffer = new StringBuilder();

    @SneakyThrows
    public PubMedReader(FSDataInputStream stream) {
        XMLInputFactory factory = XMLInputFactory.newInstance();
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        this.reader = factory.createXMLStreamReader(stream);
    }

    @Override
    @SneakyThrows
    public PubMedRecord read() {
        String pmid = null, title = null, issn = null;
        List<String> authors = new ArrayList<>();

        String foreName = "", lastName = "", collectiveName = "";
        boolean inMedlineCitation = false, inArticleTitle = false, inAuthor = false;

        while (reader.hasNext()) {
            int event = reader.next();

            switch (event) {
                case XMLStreamConstants.START_ELEMENT -> {
                    String tag = reader.getLocalName();
                    textBuffer.setLength(0);

                    switch (tag) {
                        case "MedlineCitation" -> inMedlineCitation = true;
                        case "ArticleTitle" -> {
                            inArticleTitle = true;
                            titleBuffer.setLength(0);
                        }
                        case "Author" -> {
                            inAuthor = true;
                            foreName = "";
                            lastName = "";
                            collectiveName = "";
                        }
                    }
                }
                case XMLStreamConstants.CHARACTERS -> {
                    String text = reader.getText();
                    if (inArticleTitle) {
                        titleBuffer.append(text);
                    }
                    textBuffer.append(text);
                }
                case XMLStreamConstants.END_ELEMENT -> {
                    String tag = reader.getLocalName();
                    String text = textBuffer.toString().trim();

                    switch (tag) {
                        case "MedlineCitation" -> inMedlineCitation = false;
                        case "PMID" -> {
                            if (inMedlineCitation && pmid == null) pmid = text;
                        }
                        case "ArticleTitle" -> {
                            inArticleTitle = false;
                            title = titleBuffer.toString().trim();
                        }
                        case "ISSN" -> {
                            if (issn == null) issn = text;
                        }
                        case "ForeName" -> { if (inAuthor) foreName = text; }
                        case "LastName" -> { if (inAuthor) lastName = text; }
                        case "CollectiveName" -> { if (inAuthor) collectiveName = text; }
                        case "Author" -> {
                            inAuthor = false;
                            if (!foreName.isEmpty() || !lastName.isEmpty()) {
                                authors.add((foreName + " " + lastName).trim());
                            } else if (!collectiveName.isEmpty()) {
                                authors.add(collectiveName);
                            }
                        }
                        case "PubmedArticle" -> {
                            return new PubMedRecord(
                                    pmid == null ? "" : pmid,
                                    title == null ? "" : title,
                                    issn == null ? "" : issn,
                                    authors
                            );
                        }
                    }
                }
            }
        }

        return null;
    }

    @Override
    @SneakyThrows
    public void close() {
        if (reader != null) reader.close();
    }

}
