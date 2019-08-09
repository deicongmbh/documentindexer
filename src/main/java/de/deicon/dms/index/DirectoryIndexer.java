package de.deicon.dms.index;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Access to Lucene Index
 */
public class DirectoryIndexer {

    private static final Logger LOG = Logger.getLogger(DirectoryIndexer.class.getName());

    private final File indexDirFile;
    /**
     * Lucene Index is kept in Filesystem
     */
    private final String indexDir;
    private final Directory indexDirectory;

    public DirectoryIndexer(String indexDir) throws IOException {
        this.indexDir = indexDir;
        this.indexDirFile = new File(this.indexDir);
        this.indexDirectory = FSDirectory.open(indexDirFile.toPath());
    }


    public List<Document> parseFilesInDirectory(String dirName) {


        List<Document> result = new ArrayList<>();
        File dir = new File(dirName);

        return Arrays.asList(dir.listFiles()).parallelStream()
        .map(f -> f.isDirectory() ? parseFilesInDirectory(f.getAbsolutePath()) : this.parseFile(f))
        .flatMap(Collection::stream).filter(Objects::nonNull).collect(Collectors.toList());

    }

    private List<Document> parseFile(File file) {

        String fileName = file.getAbsolutePath();
        // create new MetaData for each entry
        Metadata meta = new Metadata();
        ContentHandler bodyContentHandler = new BodyContentHandler();
        ParseContext parseContext = new ParseContext();

        TesseractOCRConfig imageOcrConfig = new TesseractOCRConfig();
        parseContext.set(TesseractOCRConfig.class, imageOcrConfig);

        /** This knows about most File formats like docx, pdf, svg, txt, etc. */
        Parser parser = new AutoDetectParser();
        LOG.info("Parsing " + fileName);

        // open file
        try (InputStream in = new FileInputStream(file)) {
            parser.parse(in, bodyContentHandler, meta, parseContext);

            // once parsed this should contain the text
            String text = bodyContentHandler.toString();

            // create new Lucene Document
            Document doc = new Document();

            doc.add(new Field("file", fileName, TextField.TYPE_STORED));

            // adding found metadata from Tika parser
            for (String key : meta.names()) {
                String name = key.toLowerCase();
                String value = meta.get(key);
                if (StringUtils.isBlank(value)) {
                    continue;
                }

                if ("keywords".equalsIgnoreCase(key)) {
                    for (String keyword : value.split(",?(\\s+)")) {
                        doc.add(new Field(name, keyword, TextField.TYPE_STORED));
                    }
                } else if ("title".equalsIgnoreCase(key)) {
                    doc.add(new Field(name, value, TextField.TYPE_STORED));
                } else {
                    doc.add(new Field(name, fileName, TextField.TYPE_NOT_STORED));
                }
            }

            doc.add(new Field("text", text, TextField.TYPE_NOT_STORED));
            LOG.info("Finished " + fileName);
            return Arrays.asList(doc);
        } catch (TikaException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

    /**
     * Adding directory to index
     *
     * @param directory The directory containing the files to index
     */
    public void addDirectory(String directory) throws IOException {

        // will be done in separate thread eventually
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(this.indexDirectory, config);
        writer.deleteAll();
        parseFilesInDirectory(directory).forEach(doc -> {
            try {
                writer.addDocument(doc);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });


        writer.commit();
        writer.deleteUnusedFiles();

        System.out.println(writer.getMaxCompletedSequenceNumber() + " documents written");
    }

    public static void main(String[] args) throws IOException {
        new DirectoryIndexer(args[0]).addDirectory(args[1]);
    }


}
