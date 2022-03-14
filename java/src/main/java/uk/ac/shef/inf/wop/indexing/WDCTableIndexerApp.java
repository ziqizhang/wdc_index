package uk.ac.shef.inf.wop.indexing;

/**
 * Indexing data from the WDC table corpus (based on the schema.org data, e.g.,
 * http://webdatacommons.org/structureddata/schemaorgtables/) using solr
 *
 * For the schema, /resources/template
 *
 */

import opennlp.tools.langdetect.Language;
import opennlp.tools.langdetect.LanguageDetector;
import opennlp.tools.langdetect.LanguageDetectorME;
import opennlp.tools.langdetect.LanguageDetectorModel;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ForkJoinPool;

public class WDCTableIndexerApp {
    private static final Logger LOG = Logger.getLogger(WDCTableIndexerApp.class.getName());

    public static void main(String[] args) throws IOException {
        InputStream is = new FileInputStream(new File(args[5]));
        LanguageDetectorModel m = new LanguageDetectorModel(is);
        LanguageDetector languageDetector = new LanguageDetectorME(m);

//        LanguageDetector detector =LanguageDetector.getDefaultLanguageDetector();
//        detector.addText("This is english");
//        detector.addText("This is english");
//        detector.addText("This is english");
//        detector.addText("This is english");
//        detector.addText("This is english");
//        detector.addText("This is english");
//        detector.addText("This is english");
//        detector.addText("This is english");
//        detector.addText("This is english");
//        LanguageResult languageResult=detector.detect();
//
//        System.out.println("end");
//        System.exit(0);

        CoreContainer solrContainer = new CoreContainer(args[1]);
        solrContainer.load();

        SolrClient entitiesCoreClient = new EmbeddedSolrServer(solrContainer.getCore("entities"));
        List<String> zipFiles = new ArrayList<>();
        for (File f: Objects.requireNonNull(new File(args[0]).listFiles()))
            zipFiles.add(f.toString());
        Collections.sort(zipFiles);
        LOG.info("Initialisation completed.");
        WDCTableIndexerWorker worker = new WDCTableIndexerWorker(0,entitiesCoreClient,zipFiles,languageDetector);

        try {

            ForkJoinPool forkJoinPool = new ForkJoinPool();
            int total = forkJoinPool.invoke(worker);

            LOG.info(String.format("Completed, total entities=%s", total, new Date().toString()));

        } catch (Exception ioe) {
            StringBuilder sb = new StringBuilder("Failed!");
            sb.append("\n").append(ExceptionUtils.getFullStackTrace(ioe));
            LOG.info(sb.toString());
        }


        entitiesCoreClient.close();
        System.exit(0);
    }

}