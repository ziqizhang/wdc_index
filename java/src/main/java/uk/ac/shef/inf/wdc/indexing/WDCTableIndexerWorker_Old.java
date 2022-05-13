package uk.ac.shef.inf.wdc.indexing;

import java.util.logging.Logger;

import com.google.common.net.InternetDomainName;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.tika.language.detect.LanguageDetector;
import org.apache.tika.language.detect.LanguageResult;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.concurrent.RecursiveTask;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;


/**
 * this file reads the zipped json table files from http://webdatacommons.org/structureddata/schemaorgtables/
 * and index them in a solr index.
 * <p>
 * Only english data is selected
 */

public class WDCTableIndexerWorker_Old extends RecursiveTask<Integer> {
    private LanguageDetector langDetector;

    private SolrClient entitiesCoreClient;
    //private SolrClient predicatesCoreClient;
    private int commitBatch = 10000;
    private int languageSample = 500;
    private int workerID;

    private static final Logger LOG = Logger.getLogger(WDCTableIndexerWorker_Old.class.getName());

    private int maxZipFilesPerThread = 2000;
    private List<String> zipFiles;
    private Map<String, Integer> ignoredTLDs;
    private Map<String, Set<String>> ignoredNonEnglish;

    private List<String> invalidDomains = Arrays.asList("ru", "rs", "gr", "pl", "md", "fr",
            "ro", "dk", "ua", "at", "bg", "tw", "by", "hk", "it", "jp", "no", "lt", "hu",
            "ch", "ir", "kz", "mx", "su", "br",
            "cz", "ee", "sk", "si", "be", "de", "es", "cn",
            "fi","eu", "co","cymru", "cy","ge","vn","ar","mk","id","ec","tr",
            "fm","ba","se","kr","il","cl","pe","pk","ps","pt","mt","tv","hr","lu","lv",
            "gt","sv","me");//nl - netherland, sometimes ok

    /*
    Poe&#x27;s famous icon, The Raven displayed on 15 oz. coffee mug - also see the Poe mug
    18&quot;x12&quot; Artist print on card stock by Jake Prendez
zzgl. Versandkosten Muß man als Stels \" Pilot \" einfach haben. Stels Tasse mit dem gewissen Aufdruck.....
Pour tous les événements importants de votre vie, pensez à la carte personnalisée. Texte/image à votre convenance au gré de vos envies et de votre fantaisie. Création Carlotta Kapa Voir galerie &quot;boutique créations&quot;. Délai de livraison 7 jours.

     */
    public WDCTableIndexerWorker_Old(int id,
                                 SolrClient entitiesCoreClient, List<String> zipFiles,
                                 Map<String, Integer> ignoredTLDs,
                                 Map<String, Set<String>> ignoredNonEnglish) throws IOException {
        this.workerID = id;
        this.entitiesCoreClient = entitiesCoreClient;
        //this.predicatesCoreClient = predicatesCoreClient;
        this.zipFiles = zipFiles;
        LanguageDetector detector = LanguageDetector.getDefaultLanguageDetector().loadModels();
        this.langDetector = detector;
        this.ignoredTLDs = ignoredTLDs;
        this.ignoredNonEnglish=ignoredNonEnglish;
    }

    protected int runSingleThread(List<String> zipFiles) throws IOException {
        //each zip file is a schemaorg class
        for (String inputZipFile : zipFiles) {
            try {
                ZipFile zipFile = new ZipFile(inputZipFile);
                String schemaorgClass = inputZipFile.substring(
                        inputZipFile.lastIndexOf("/") + 1, inputZipFile.lastIndexOf("_")
                );
                String batchSource = inputZipFile.substring(
                        inputZipFile.lastIndexOf("_") + 1, inputZipFile.lastIndexOf(".")
                );
                LOG.info("Thread " + workerID + " processing file " + inputZipFile + " with " + zipFile.size() + " entries");
                Enumeration<? extends ZipEntry> entries = zipFile.entries();
                int entryCount = 0;

                //going through each gz file
                while (entries.hasMoreElements()) {
                    Set<String> sampleText = new HashSet<>();
                    entryCount++;
                    boolean added=false;
                    ZipEntry entry = entries.nextElement();
                    LOG.info("\tThread " + workerID + " item " + entryCount + "/" + zipFile.size() + ": " + entry.getName());
                    InputStream fi = zipFile.getInputStream(entry);

                    GZIPInputStream gzip = new GZIPInputStream(fi);
                    BufferedReader breader = new BufferedReader(new InputStreamReader(gzip));
                    String line;
                    int recordID = 0;
                    Collection<SolrInputDocument> toAdd = new ArrayList<>();
                    boolean checkLanguage = true;
                    boolean invalidHost=false, isEnglish=true;
                    String host="";
                    while ((line = breader.readLine()) != null) {
                        String docid = entry.getName() + "_thread" + workerID + "_" + batchSource + "_" + recordID;
                        SolrInputDocument entityDoc = new SolrInputDocument();
                        entityDoc.addField("id", docid);
                        entityDoc.addField("schemaorg_class", schemaorgClass);
                        entityDoc.addField("batch_source_t", batchSource);
                        JSONTokener tokener = new JSONTokener(line);
                        JSONObject json = new JSONObject(tokener);

                        //check every field/column of this table
                        for (String k : json.keySet()) {
                            Object o = json.get(k);
                            String field = k;
                            if (k.equalsIgnoreCase("page_url")) {
                                //get domain, get tld
                                try {
                                    entityDoc.addField("page_url", o.toString());
                                    URI u = new URI(o.toString());
                                    host = u.getHost();
                                    InternetDomainName topPrivateDomain = InternetDomainName.from(u.getHost()).topPrivateDomain();
                                    String tld = topPrivateDomain.hasPublicSuffix() ? topPrivateDomain.publicSuffix().toString() :
                                            "";
                                    if (isInvalidHost(host)){
                                        invalidHost=true;
                                        break;
                                    }
                                    entityDoc.addField("page_domain", host);
                                    entityDoc.addField("page_tld", tld);
                                } catch (Exception e) {
                                    //LOG.info(String.format("\t\tencountered issues when trying to parse (uri, or tld): %s", o));
                                }
                            }
                            else if (o instanceof String) {
                                String text=o.toString();
                                try {
                                    text = StringEscapeUtils.unescapeHtml4(o.toString())
                                            .replaceAll("[\\p{Punct}\\d]", "").trim();
                                }catch (Exception e){
                                    text=text.replaceAll("[\\p{Punct}\\d]", "").trim();
                                }

                                if (text.contains("http"))
                                    continue;
                                if (text.length() > 10 || text.split(" ").length >= 2) {
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

                        if (invalidHost){
                            //this line is from invaild host
                            LOG.info(String.format("\t\t\tdata file is not a valid host, file=%s",
                                    entry.getName()));
                            invalidHost=true;
                            break;
                        }
                        //check language when we collected enough samples, and are still within the
                        //file reader of this zip file
                        if (checkLanguage && sampleText.size() >= languageSample) {
                            //detect language
                            checkLanguage = false;
                            isEnglish = isEnglish(sampleText);
                            if (!isEnglish) {
                                LOG.info(String.format("\t\t\tdata file is not English, file=%s",
                                        entry.getName()));
                                toAdd.clear();
                                sampleText.clear();
                                break;
                            }
                        }

                        //will not execute if language checking to be non english
                        toAdd.add(entityDoc);
                        if (toAdd.size() >= commitBatch) {
                            if (checkLanguage){//language has not been checked, wait
                                continue;
                            }
                            try {
                                entitiesCoreClient.add(toAdd);
                                LOG.info(String.format("\t\tadded batch size: %d, total=%d",
                                        commitBatch, recordID));
                                toAdd.clear();
                                added=true;
                            } catch (Exception e) {
                                LOG.info(String.format("\t\tencountered exception when adding batch, current record id=%d, " +
                                                "previous batch size=%d\n%s",
                                        recordID, commitBatch, ExceptionUtils.getFullStackTrace(e)));
                            }
                        }

                        recordID++;
                    } //end while (one json)
                    breader.close();

                    //check language if we never had enough samples, but collected some texts anyway
                    if (!invalidHost && checkLanguage ) {
                        isEnglish = isEnglish(sampleText);
                        if (!isEnglish) {
                            LOG.info(String.format("\t\t\tdata file is not English, file=%s",
                                    entry.getName()));
                            toAdd.clear();
                        }else if (toAdd.size() > 0) {
                            try {
                                added = true;
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
                        sampleText.clear();
                    }

                    //BufferedReader br = new BufferedReader(new InputStreamReader(fi));
                    if (!added){
                        LOG.info(String.format("\t\t\t>>> NOT ADDED: file=%s",
                                entry.getName()));
                        toAdd.clear();
                        if (invalidHost) {
                            int ignored = ignoredTLDs.getOrDefault(schemaorgClass, 0);
                            ignored++;
                            ignoredTLDs.put(schemaorgClass, ignored);
                        }else if (!isEnglish){
                            Set<String> nonEnglishHosts = ignoredNonEnglish.getOrDefault(schemaorgClass, new HashSet<>());
                            nonEnglishHosts.add(host);
                            ignoredNonEnglish.put(schemaorgClass, nonEnglishHosts);
                        }

                    }
                }
                zipFile.close();
            } catch (ZipException e) {
                LOG.info(String.format("\tThread " + workerID + " unable to process zip file: " + inputZipFile + "\n%s"
                        , ExceptionUtils.getFullStackTrace(e)));
            }
        }
        return 0;
    }

    private boolean isInvalidHost(String host){
        for (String tld : invalidDomains){
            if (host.startsWith(tld+".")|| host.endsWith("."+tld))
                return true;
        }
        return false;
    }
    private boolean isEnglish(Set<String> texts) {
        int eng=0, ignored=0;
        for (String t:texts) {
            langDetector.reset();
            langDetector.addText(t);
            LanguageResult languageResult = langDetector.detect();
            String lang= languageResult.getLanguage();
            if (lang.equalsIgnoreCase("")) {
                ignored += 1;
                continue;
            }
            float conf = languageResult.getRawScore();
            if (lang.equalsIgnoreCase("en"))
                eng++;
        }
        double eng_ratio=(double)eng/(texts.size()-ignored);
        if (eng==0)
            return false;
        return eng_ratio>0.5;
    }

    private boolean isEnglish_Old(List<String> texts) {

        Collections.shuffle(texts);
        langDetector.reset();
        for (int i = 0; i < 100 && i < texts.size(); i++)
            langDetector.addText(texts.get(i));

        //longText.append(texts.get(i)).append(" ");
        LanguageResult languageResult = langDetector.detect();
        //check result
        //return bestLanguage.getLang().equalsIgnoreCase("eng");
        String lang= languageResult.getLanguage();
        float conf = languageResult.getRawScore();

//        if (lang.equalsIgnoreCase("en") && conf<0.9)
//            System.out.println("low confident");

        return lang.equalsIgnoreCase("en");

    }

    @Override
    protected Integer compute() {
        if (this.zipFiles.size() > maxZipFilesPerThread) {
            try {
                List<WDCTableIndexerWorker_Old> subWorkers =
                        new ArrayList<>(createSubWorkers());
                for (WDCTableIndexerWorker_Old subWorker : subWorkers)
                    subWorker.fork();
                return mergeResult(subWorkers);

            } catch (IOException e) {
                LOG.info(String.format("\t\tunable to create thread: %s, \n %s",
                        this.zipFiles.toString(), ExceptionUtils.getFullStackTrace(e)));
                System.exit(1);
                return 0;
            }
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


    protected List<WDCTableIndexerWorker_Old> createSubWorkers() throws IOException {
        List<WDCTableIndexerWorker_Old> subWorkers =
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

        WDCTableIndexerWorker_Old subWorker1 = createInstance(splitTask1, this.workerID + 1);
        WDCTableIndexerWorker_Old subWorker2 = createInstance(splitTask2, this.workerID + 2);

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
    protected WDCTableIndexerWorker_Old createInstance(List<String> splitTasks, int id) throws IOException {
        WDCTableIndexerWorker_Old indexer = new WDCTableIndexerWorker_Old(id,
                entitiesCoreClient, splitTasks, ignoredTLDs, ignoredNonEnglish);
        return indexer;
    }
    /*{
        return new NTripleIndexerApp(id, this.solrClient, splitTasks, maxTasksPerThread, outFolder);
    }*/

    protected int mergeResult(List<WDCTableIndexerWorker_Old> workers) {
        Integer total = 0;
        for (WDCTableIndexerWorker_Old worker : workers) {
            total += worker.join();
        }
        return total;
    }

    private boolean isValidHost(String host) {
        for (String d : invalidDomains) {
            if (host.endsWith("." + d) || host.startsWith("." + d))
                return false;
        }

        return true;
    }
}
