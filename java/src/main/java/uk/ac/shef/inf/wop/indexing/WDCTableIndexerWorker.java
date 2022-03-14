package uk.ac.shef.inf.wop.indexing;

import java.util.logging.Logger;

import com.google.common.net.InternetDomainName;
import opennlp.tools.langdetect.Language;
import opennlp.tools.langdetect.LanguageDetector;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.RecursiveTask;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;


/**
 * this file reads the zipped json table files from http://webdatacommons.org/structureddata/schemaorgtables/
 * and index them in a solr index.
 *
 * Only english data is selected
 */

public class WDCTableIndexerWorker extends RecursiveTask<Integer> {
    private LanguageDetector langDetector ;

    private SolrClient entitiesCoreClient;
    //private SolrClient predicatesCoreClient;
    private int commitBatch = 10000;
    private int languageSample=50;
    private int workerID;

    private static final Logger LOG = Logger.getLogger(WDCTableIndexerWorker.class.getName());

    private int maxZipFilesPerThread = 2000;
    private List<String> zipFiles;


    public WDCTableIndexerWorker(int id,
                                 SolrClient entitiesCoreClient, List<String> zipFiles,
                                 LanguageDetector langdetector) {
        this.workerID = id;
        this.entitiesCoreClient = entitiesCoreClient;
        //this.predicatesCoreClient = predicatesCoreClient;
        this.zipFiles = zipFiles;
        this.langDetector =langdetector;
    }

    protected int runSingleThread(List<String> zipFiles) throws IOException {
        //each zip file is a schemaorg class
        for (String inputZipFile : zipFiles) {
            try {
                ZipFile zipFile = new ZipFile(inputZipFile);
                String schemaorgClass = inputZipFile.substring(
                        inputZipFile.lastIndexOf("/") + 1, inputZipFile.lastIndexOf("_")
                );
                LOG.info("Thread " + workerID + " processing file " + inputZipFile + " with " + zipFile.size() + " entries");
                Enumeration<? extends ZipEntry> entries = zipFile.entries();
                int entryCount = 0;

                //going through each gz file
                List<String> sampleText = new ArrayList<>();
                while (entries.hasMoreElements()) {
                    entryCount++;
                    ZipEntry entry = entries.nextElement();
                    LOG.info("\tThread " + workerID + " item " + entryCount + "/" + zipFile.size() + ": " + entry.getName());
                    InputStream fi = zipFile.getInputStream(entry);

                    GZIPInputStream gzip = new GZIPInputStream(fi);
                    BufferedReader breader = new BufferedReader(new InputStreamReader(gzip));
                    String line;
                    int recordID = 0;
                    Collection<SolrInputDocument> toAdd = new ArrayList<>();
                    boolean checkLanguage = true;
                    while ((line = breader.readLine()) != null) {
                        String docid = entry.getName() + "_thread" + workerID + "_" + recordID;
                        SolrInputDocument entityDoc = new SolrInputDocument();
                        entityDoc.addField("id", docid);
                        entityDoc.addField("schemaorg_class", schemaorgClass);
                        JSONTokener tokener = new JSONTokener(line);
                        JSONObject json = new JSONObject(tokener);
                        for (String k : json.keySet()) {
                            Object o = json.get(k);
                            String field = k;
                            if (k.equalsIgnoreCase("page_url")) {
                                //get domain, get tld
                                try {
                                    entityDoc.addField("page_url", o.toString());
                                    URI u = new URI(o.toString());
                                    String host = u.getHost();
                                    InternetDomainName topPrivateDomain = InternetDomainName.from(u.getHost()).topPrivateDomain();
                                    String tld = topPrivateDomain.hasPublicSuffix() ? topPrivateDomain.publicSuffix().toString() :
                                            "";
                                    entityDoc.addField("page_domain", host);
                                    entityDoc.addField("page_tld", tld);
                                } catch (Exception e) {
                                    //LOG.info(String.format("\t\tencountered issues when trying to parse (uri, or tld): %s", o));
                                }
                            } else if (o instanceof String) {
                                String text = o.toString().trim();
                                if (text.split(" ").length > 10) {
                                    sampleText.add(text);
                                }
                                entityDoc.addField(field + "_t", text);
                            } else if (o instanceof JSONObject) {
                                JSONObject innerJSON = (JSONObject) o;
                                for (String innerField : innerJSON.keySet()) {
                                    String innerValue = innerJSON.get(innerField).toString().trim();
                                    entityDoc.addField(field + "_" + innerField + "_t", innerValue);
                                }
                            }
                        }

                        if (checkLanguage && sampleText.size() >= languageSample) {
                            //detect language
                            checkLanguage = false;
                            boolean isEnglish = isEnglish(sampleText);
                            sampleText.clear();
                            if (!isEnglish) {
                                LOG.info(String.format("\t\t\tdata file is not English, ignored: %d, file=%s",
                                        entryCount, entry.getName()));
                                toAdd.clear();
                                break;
                            }
                        }
                        toAdd.add(entityDoc);
                        if (toAdd.size() >= commitBatch) {
                            try {
                                entitiesCoreClient.add(toAdd);
                                LOG.info(String.format("\t\tadded batch size: %d, total=%d",
                                        commitBatch, recordID));
                                toAdd.clear();
                            } catch (Exception e) {
                                LOG.info(String.format("\t\tencountered exception when adding batch, current record id=%d, " +
                                                "previous batch size=%d\n%s",
                                        recordID, commitBatch, ExceptionUtils.getFullStackTrace(e)));
                            }
                        }

                        recordID++;
                    }
                    breader.close();
                    try {
                        if (toAdd.size() > 0) {
                            entitiesCoreClient.add(toAdd);
                            LOG.info(String.format("\t\tadded batch size: %d, total=%d",
                                    commitBatch, recordID));
                            toAdd.clear();
                        }
                    } catch (Exception e) {
                        LOG.info(String.format("\t\tencountered exception when adding batch, current record id=%d, " +
                                        "previous batch size=%d\n%s",
                                recordID, commitBatch, ExceptionUtils.getFullStackTrace(e)));
                    }
                    //BufferedReader br = new BufferedReader(new InputStreamReader(fi));
                }
                zipFile.close();
            }catch (ZipException e){
                LOG.info(String.format("\tThread " + workerID + " unable to process zip file: "+inputZipFile+"\n%s"
                        ,ExceptionUtils.getFullStackTrace(e)));
            }
        }
        return 0;
    }

    private boolean isEnglish(List<String> texts) {
        Collections.shuffle(texts);
        StringBuilder longText= new StringBuilder();
        for (int i=0; i<100 && i<texts.size(); i++)
            longText.append(texts.get(i)).append(" ");
        Language bestLanguage = langDetector.predictLanguage(longText.toString().trim());
        //check result
        return bestLanguage.getLang().equalsIgnoreCase("eng");
    }

    @Override
    protected Integer compute() {
        if (this.zipFiles.size() > maxZipFilesPerThread) {
            List<WDCTableIndexerWorker> subWorkers =
                    new ArrayList<>(createSubWorkers());
            for (WDCTableIndexerWorker subWorker : subWorkers)
                subWorker.fork();
            return mergeResult(subWorkers);
        } else {
            try {
                return runSingleThread(this.zipFiles);
            } catch (IOException e) {
                LOG.info(String.format("\t\tunable to read input zip file: %s, \n %s",
                        this.zipFiles.toString(), ExceptionUtils.getFullStackTrace(e)));
                return 0;
            }
        }
    }


    protected List<WDCTableIndexerWorker> createSubWorkers() {
        List<WDCTableIndexerWorker> subWorkers =
                new ArrayList<>();

        boolean b = false;
        List<String> splitTask1 = new ArrayList<>();
        List<String> splitTask2 = new ArrayList<>();
        for (String s : zipFiles) {
            if (b)
                splitTask1.add(s);
            else
                splitTask2.add(s);
            b = !b;
        }

        WDCTableIndexerWorker subWorker1 = createInstance(splitTask1, this.workerID + 1);
        WDCTableIndexerWorker subWorker2 = createInstance(splitTask2, this.workerID + 2);

        subWorkers.add(subWorker1);
        subWorkers.add(subWorker2);

        return subWorkers;
    }

    /**
     * NOTE: classes implementing this method must call setHashtagMap and setMaxPerThread after creating your object!!
     *
     * @param splitTasks
     * @param id
     * @return
     */
    protected WDCTableIndexerWorker createInstance(List<String> splitTasks, int id) {
        WDCTableIndexerWorker indexer = new WDCTableIndexerWorker(id,
                entitiesCoreClient, splitTasks, langDetector);
        return indexer;
    }
    /*{
        return new NTripleIndexerApp(id, this.solrClient, splitTasks, maxTasksPerThread, outFolder);
    }*/

    protected int mergeResult(List<WDCTableIndexerWorker> workers) {
        Integer total = 0;
        for (WDCTableIndexerWorker worker : workers) {
            total += worker.join();
        }
        return total;
    }
}