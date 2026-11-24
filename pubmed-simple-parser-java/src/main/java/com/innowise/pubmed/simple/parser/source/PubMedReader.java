package com.innowise.pubmed.simple.parser.source;

import com.innowise.pubmed.simple.parser.model.PubMedRecord;
import lombok.SneakyThrows;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.BufferedInputStream;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNullElse;
import static javax.xml.stream.XMLStreamConstants.CHARACTERS;
import static javax.xml.stream.XMLStreamConstants.END_ELEMENT;
import static javax.xml.stream.XMLStreamConstants.START_ELEMENT;

public class PubMedReader implements StreamFormat.Reader<PubMedRecord> {
    private final XMLStreamReader reader;

    private final StringBuilder buffer = new StringBuilder();

    @SneakyThrows
    public PubMedReader(FSDataInputStream stream) {
        XMLInputFactory factory = XMLInputFactory.newInstance();
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        factory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, false);
        factory.setProperty(XMLInputFactory.IS_COALESCING, true);
        this.reader = factory.createXMLStreamReader(new BufferedInputStream(stream, 64 * 1024));
    }

    @Override
    @SneakyThrows
    public PubMedRecord read() {
        String pmid = null, title = null, issn = null;
        List<String> authors = new ArrayList<>();
        boolean inMedlineCitation = false;

        while (reader.hasNext()) {
            switch (reader.next()) {
                case START_ELEMENT -> {
                    switch (reader.getLocalName()) {
                        case "MedlineCitation" -> inMedlineCitation = true;
                        case "PMID" -> { if (inMedlineCitation && pmid == null) pmid = readText(); }
                        case "ISSN" -> { if (issn == null) issn = readText(); }
                        case "ArticleTitle" -> title = readText();
                        case "Author" -> {
                            String author = readAuthor();
                            if (!author.isEmpty()) authors.add(author);
                        }
                    }
                }
                case END_ELEMENT -> {
                    switch (reader.getLocalName()) {
                        case "MedlineCitation" -> inMedlineCitation = false;
                        case "PubmedArticle" -> {
                            return new PubMedRecord(
                                    requireNonNullElse(pmid, ""),
                                    requireNonNullElse(title, ""),
                                    requireNonNullElse(issn, ""),
                                    authors
                            );
                        }
                    }
                }
            }
        }

        return null;
    }

    /** Reads the current {@code <Author>} up to its end tag: "ForeName LastName", else CollectiveName, else "". */
    private String readAuthor() throws XMLStreamException {
        String foreName = "", lastName = "", collectiveName = "";

        while (reader.hasNext()) {
            switch (reader.next()) {
                case START_ELEMENT -> {
                    switch (reader.getLocalName()) {
                        case "ForeName" -> foreName = readText();
                        case "LastName" -> lastName = readText();
                        case "CollectiveName" -> collectiveName = readText();
                    }
                }
                case END_ELEMENT -> {
                    if (reader.getLocalName().equals("Author")) {
                        String name = (foreName + " " + lastName).trim();
                        return name.isEmpty() ? collectiveName : name;
                    }
                }
            }
        }

        return "";
    }

    /**
     * Reads all text of the current element, including nested markup like {@code <i>} or {@code <sup>},
     * and leaves the reader on its end tag. Unlike {@link XMLStreamReader#getElementText()}, does not fail on child elements.
     */
    private String readText() throws XMLStreamException {
        buffer.setLength(0);
        for (int depth = 1; depth > 0; ) {
            switch (reader.next()) {
                case CHARACTERS -> buffer.append(reader.getText());
                case START_ELEMENT -> depth++;
                case END_ELEMENT -> depth--;
            }
        }
        return buffer.toString().trim();
    }

    @Override
    @SneakyThrows
    public void close() {
        if (reader != null) reader.close();
    }

}
