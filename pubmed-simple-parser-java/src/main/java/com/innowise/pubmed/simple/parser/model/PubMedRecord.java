package com.innowise.pubmed.simple.parser.model;

import java.util.List;

public record PubMedRecord(String pmid, String title, String issn, List<String> authors) { }
