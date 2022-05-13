package uk.ac.shef.inf.wdc.indexing;

/**
 * Indexing data from the WDC table corpus (based on the schema.org data, e.g.,
 * http://webdatacommons.org/structureddata/schemaorgtables/) using solr
 *
 * For the schema, /resources/template
 *
 */

import org.apache.commons.lang.exception.ExceptionUtils;

import java.io.FileWriter;
import java.io.PrintWriter;
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

//        LanguageDetectorModel m = new LanguageDetectorModel(is);
//        LanguageDetector languageDetector = new LanguageDetectorME(m);

        Map<String, Integer> ignoredTLDs=new HashMap<>();
        Map<String, Set<String>> ignoredNoneEnglish=new HashMap<>();

        CoreContainer solrContainer = new CoreContainer(args[1]);
        solrContainer.load();

        SolrClient entitiesCoreClient = new EmbeddedSolrServer(solrContainer.getCore("entities"));
        List<String> zipFiles = new ArrayList<>();
        for (File f: Objects.requireNonNull(new File(args[0]).listFiles()))
            zipFiles.add(f.toString());
        Collections.sort(zipFiles);
        LOG.info("Initialisation completed.");
        WDCTableIndexerWorker worker =
                new WDCTableIndexerWorker(0,entitiesCoreClient,zipFiles,ignoredTLDs,ignoredNoneEnglish);

        try {

            ForkJoinPool forkJoinPool = new ForkJoinPool();
            int total = forkJoinPool.invoke(worker);

            LOG.info(String.format("Completed, total entities=%s", total, new Date().toString()));

            LOG.info("Optimising index...");
            entitiesCoreClient.optimize();
        } catch (Exception ioe) {
            StringBuilder sb = new StringBuilder("Failed!");
            sb.append("\n").append(ExceptionUtils.getFullStackTrace(ioe));
            LOG.info(sb.toString());
        }


        entitiesCoreClient.close();

        LOG.info("Total ignored TLDs as follows");
        for (Map.Entry<String, Integer> en: ignoredTLDs.entrySet())
            System.out.println("\t"+en.getKey()+"\t"+en.getValue());
        LOG.info("Total ignored NON English as follows");
        for (Map.Entry<String, Set<String>> en: ignoredNoneEnglish.entrySet())
            System.out.println("\t"+en.getKey()+"\t"+en.getValue().size());
        LOG.info("Saving ignored NON English to a file...");
        PrintWriter w  = new PrintWriter(new FileWriter(args[1]+"/ignored_non_english.txt"));
        for (Map.Entry<String, Set<String>> en: ignoredNoneEnglish.entrySet()) {
            w.println(en.getKey());
            for (String h: en.getValue()){
                w.println(("\t\t"+h));
            }
        }
        w.close();
        System.exit(0);
    }

}