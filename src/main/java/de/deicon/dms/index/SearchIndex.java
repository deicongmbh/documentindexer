package de.deicon.dms.index;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.*;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class SearchIndex {
    private final String indexDir;
    private final File indexDirFile;
    private final FSDirectory indexDirectory;
    private final DirectoryReader directoryReader;
    private final IndexSearcher indexSearcher;

    public SearchIndex(String indexDir) throws IOException {
        this.indexDir = indexDir;
        this.indexDirFile = new File(this.indexDir);
        this.indexDirectory = FSDirectory.open(indexDirFile.toPath());

        this.directoryReader = DirectoryReader.open(indexDirectory);
        this.indexSearcher = new IndexSearcher(directoryReader);

    }

    public List<Document> search(int hitsPerPage, int offset, String keywords) throws ParseException, IOException {
        // create Query from Keywords
        String queryStr = keywords;
        Query query = new QueryParser("text", new StandardAnalyzer()).parse(queryStr);

        TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage, hitsPerPage*10);

        indexSearcher.search(query, collector);



        // results are collected in collector

        ScoreDoc[] hits = collector.topDocs(offset, hitsPerPage).scoreDocs;
        return Arrays.stream(hits).map(this::extractDoc).filter(Objects::nonNull).collect(Collectors.toList());
    }

    private Document extractDoc(ScoreDoc hit) {
        try {
            return directoryReader.document(hit.doc);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    public static void main(String[] args) throws IOException, ParseException {
        String query = Arrays.asList(args).stream().collect(Collectors.joining(" "));
        List<Document> documents = new SearchIndex("/home/dieter/temp/index").search(100, 0, query);
        documents.forEach(a-> {
            //a.getFields().forEach(indexableField -> System.out.println(a.toString()));
            System.out.printf("%s\n", a.get("file"));
        });
    }
}
