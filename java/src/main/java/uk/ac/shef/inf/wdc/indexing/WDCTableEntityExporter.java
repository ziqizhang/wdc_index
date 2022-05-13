package uk.ac.shef.inf.wdc.indexing;

import com.opencsv.CSVWriter;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.core.CoreContainer;
import org.apache.tika.language.detect.LanguageDetector;
import org.apache.tika.language.detect.LanguageResult;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class reads the index created by ProdDescExporter, to export textual data of a specific class
 */
public class WDCTableEntityExporter implements Runnable {

    private static final Logger LOG = Logger.getLogger(WDCTableEntityExporter.class.getName());
    /*
    Rules:
     */
    //must have description and must have at least 5 words
    private static final int MIN_DESCRIPTION_WORDS = 5;
    //must have description and must have at least 40 chars
    private static final int MIN_DESCRIPTION_CHARS = 40;
    //host must have at least 10 records
    private static final int MIN_RECORDS_PER_HOST=10;
    private Set<String> validHosts;

    private static long maxRecordsPerFile = 10000;
    //private long maxWordsPerFile=500;

    private String id;
    private int start;
    private int end;
    private SolrClient wdcTableIndex;
    private int resultBatchSize;
    private String outfolder;
    private Map<String, String> queryParams = new HashMap<>();
    private Map<String, String> optionalFields = new HashMap<>();
    private String queryString = "";
    private boolean checkLanguage;
    private LanguageDetector langDetector;

    public WDCTableEntityExporter(String id, int start, int end, Set<String> validHosts,
                                  SolrClient wdcTableIndex,
                                  int resultBatchSize, String outputFolder, boolean checkLanguage,
                                  String... queries) throws IOException {
        this.id = id;
        this.start = start;
        this.end = end;
        this.validHosts=validHosts;
        this.wdcTableIndex = wdcTableIndex;
        this.resultBatchSize = resultBatchSize;
        this.outfolder = outputFolder;
        this.checkLanguage = checkLanguage;
        LanguageDetector detector = LanguageDetector.getDefaultLanguageDetector().loadModels();
        this.langDetector = detector;
        LOG.info("\tLanguage detector loaded");
        for (String q : queries) {
            //description_t:*
            if (q.startsWith("c=")) {
                q=q.substring(2);
                String[] params = q.trim().split(":");
                queryParams.put(params[0], params[1]);
                queryString += q + " AND ";
            }else if (q.startsWith("o=")){
                q=q.substring(2);
                optionalFields.put(q,"");
            }
        }
        queryString = queryString.trim();
        if (queryString.endsWith(" AND"))
            queryString = queryString.substring(0, queryString.length() - 4).trim();
    }

    public void run() {
        SolrQuery q = createQuery(resultBatchSize, start);
        QueryResponse res;
        boolean stop = false;
        long total = 0;

        LOG.info(String.format("\tthread %s has started the query=%s, begin=%d end=%d...",
                id, queryString, q.getStart(), end));

        try {
            int fileCounter = 0;
            CSVWriter writer = new CSVWriter(new FileWriter(outfolder + "/n_" + id + "_" + fileCounter+".csv", true), ',',
                    CSVWriter.DEFAULT_QUOTE_CHARACTER,
                    CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                    CSVWriter.DEFAULT_LINE_END);
            //write header
            List<String> headersL = new ArrayList<>(queryParams.keySet());
            headersL.addAll(optionalFields.keySet());
            String[] headers = headersL.toArray(new String[0]);
            writer.writeNext(headers);

            int countRecords = 0;
            while (!stop) {
                try {
                    res = wdcTableIndex.query(q);
                    if (res != null)
                        total = res.getResults().getNumFound();
                    //update results
                    LOG.info(String.format("\t\tthread %s and file %d: total results of %d, currently processing from %d to %d...",
                            id, fileCounter, total, q.getStart(), q.getStart() + q.getRows()));

                    for (SolrDocument d : res.getResults()) {
                        String host = d.getFieldValue("page_domain").toString();
                        if (!validHosts.contains(host))
                            continue;
                        List<String> row = new ArrayList<>();
                        String description = d.getFieldValue(SolrSchema.FIELD_DESCRIPTION.fieldname()).toString();
                        StringBuilder allText = new StringBuilder();
                        for (String field : headers) {
                            if (queryParams.containsKey(field)) {
                                String v = d.getFieldValue(field).toString();
                                row.add(v);
                                if (field.equalsIgnoreCase(SolrSchema.FIELD_CLASS.fieldname()))
                                    continue;
                                allText.append(v).append(" ");
                            }else if (optionalFields.containsKey(field)){
                                Object fv=d.getFieldValue(field);
                                String v = fv ==null? "": fv.toString();
                                row.add(v);
                            }
                        }

                        boolean isEnglish = validateRecord(description, allText.toString(), checkLanguage);
                        if (!isEnglish)
                            continue;
                        writer.writeNext(row.toArray(new String[0]));
                        countRecords++;
                    }

                    if (countRecords >= maxRecordsPerFile) {
                        writer.close();
                        fileCounter++;
                        writer = new CSVWriter(new FileWriter(outfolder + "/n_" + id + "_" + fileCounter+".csv", true), ',',
                                CSVWriter.DEFAULT_QUOTE_CHARACTER,
                                CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                                CSVWriter.DEFAULT_LINE_END);
                        //write header
                        writer.writeNext(headers);
                        countRecords = 0;
                    }

                } catch (Exception e) {
                    LOG.warn(String.format("\t\t thread %s unable to successfully index product triples starting from index %s. Due to error: %s",
                            id, start,
                            ExceptionUtils.getFullStackTrace(e)));

                }

                int curr = q.getStart() + q.getRows();
                if (curr < end && curr < total)
                    q.setStart(curr);
                else {
                    stop = true;
                    LOG.info("\t\tthread " + id + " reached the end. Stopping...");
                }
            }

            try {
                writer.close();
            } catch (Exception e) {
                LOG.warn(String.format("\t\t thread %d unable to shut down servers due to error: %s",
                        id, ExceptionUtils.getFullStackTrace(e)));
            }
        } catch (IOException ioe) {
            LOG.warn(String.format("\t\t thread %d unable to create output files, io exception: %s",
                    id, ExceptionUtils.getFullStackTrace(ioe)));
        }
    }

    private boolean validateRecord(String desc, String allText, boolean checkLanguage) {
        String[] toks = desc.split("\\s+");
        if (toks.length < MIN_DESCRIPTION_WORDS)
            return false;
        if (desc.length() < MIN_DESCRIPTION_CHARS)
            return false;

        if (!checkLanguage)
            return true;
        else {
            langDetector.addText(allText);
            LanguageResult languageResult = langDetector.detect();
            //check result
            //return bestLanguage.getLang().equalsIgnoreCase("eng");
            String lang = languageResult.getLanguage();
            float conf = languageResult.getRawScore();
            langDetector.reset();
            return lang.equalsIgnoreCase("en");
        }
    }

    private long[] exportRecord(SolrDocument d,
                                PrintWriter nameFile, PrintWriter descFile) {

        Object nameData = d.getFieldValue("name");
        Object descData = d.getFirstValue("text");
        long[] res = new long[2];

        if (nameData != null) {
            String name = cleanData(nameData.toString());
            long tokens = name.split("\\s+").length;
            if (name.length() > 10 && tokens > 2) {
                nameFile.println(name);
                res[0] = tokens;
            }
        }
        if (descData != null) {
            String desc = cleanData(descData.toString());
            long tokens = desc.split("\\s+").length;
            if (desc.length() > 20 && tokens > 5) {
                descFile.println(desc);
                res[1] = tokens;
            }
        }

        return res;
    }

    private String cleanData(String value) {
        value = StringEscapeUtils.unescapeJava(value);
        value = value.replaceAll("\\s+", " ");
        value = StringUtils.stripAccents(value);
        return value.trim();
    }

    private SolrQuery createQuery(int resultBatchSize, int start) {
        SolrQuery query = new SolrQuery();
        query.setQuery(queryString);
        //query.setSort("random_1234", SolrQuery.ORDER.asc);
        query.setStart(start);
        query.setRows(resultBatchSize);

        return query;
    }

    private static Set<String> facetQueryForValidHosts(SolrClient client, String queryStr, int minCount,
                                                       String facetField) throws SolrServerException, IOException {
        SolrQuery query = new SolrQuery();
        query.setQuery(queryStr);
        //query.setSort("random_1234", SolrQuery.ORDER.asc);
        query.setFacet(true);
        query.setFacetMinCount(minCount);
        query.setFacetLimit(-1);
        query.addFacetField(facetField);
        QueryResponse qr = client.query(query);
        List<FacetField.Count> counts= qr.getFacetFields().get(0).getValues();
        Set<String> valid=new HashSet<>();
        for (FacetField.Count c: counts)
            valid.add(c.getName());
        return valid;
    }

    public static void main(String[] args) throws IOException, SolrServerException {
        //test inputs
        /*
        "c=schemaorg_class:Product,c=name_t:*,c=description_t:*,o=category_t,o=brand_t,o=page_domain"
        hotel
        "c=schemaorg_class:Hotel,c=name_t:*,c=description_t:*,o=page_domain"
         */

        /*
         * 0 - solr index
         * 1 - outfolder
         * 2 - check language or not
         * 3 - num of threads
         * 4 - start index
         * 5 - max records to process each thread
         * 6 - tasks (a file containing lines and each line is a 'label\tqueries separated by ,' record
         */
        //74488335
        int jobStart = Integer.valueOf(args[4]);
        int jobs = Integer.valueOf(args[5]);
        int threads = Integer.valueOf(args[3]);

        CoreContainer prodNDContainer = new CoreContainer(args[0]);
        prodNDContainer.load();
        SolrClient solrIndex = new EmbeddedSolrServer(prodNDContainer.getCore("entities"));
        File file = new File(args[6]);
        Scanner input = new Scanner(file);

        int count=0;
        while (input.hasNextLine()) {
            count++;
            String[] values= input.nextLine().split("\t");
            if (values.length<2)
                continue;
            String label=values[0];
            String queries = values[1];
            String[] queries_as_array = queries.split(",");
            String schemaorg_class="*:*";
            for (String q: queries_as_array){
                if (q.contains(SolrSchema.FIELD_CLASS.fieldname()))
                    schemaorg_class=q.substring(2); //expected format: c=schema...
            }
            ExecutorService executor = Executors.newFixedThreadPool(threads);

            LOG.info(String.format("Task Begins for #%d, >%s<, counting valid hosts (mincount=%d)...",
                    count,label, MIN_RECORDS_PER_HOST));
            Set<String> validHosts = facetQueryForValidHosts(solrIndex,schemaorg_class,MIN_RECORDS_PER_HOST, "page_domain");
            LOG.info(String.format("\tFound %d hosts meeting the requirement...",
                    validHosts.size()));

            for (int i = 0; i < threads; i++) {
                int start = jobStart + i * jobs;
                int end = start + jobs;
            /*
            int id, int start, int end,
                                    SolrClient wdcTableIndex,
                                    int resultBatchSize, String outputFolder,boolean checkLanguage,
                                    String... queries
             */
                Runnable exporter = new WDCTableEntityExporter(label+"-"+String.valueOf(i),
                        start, end,validHosts,
                        solrIndex,
                        10000,
                        args[1], Boolean.parseBoolean(args[2]), queries_as_array);
                executor.execute(exporter);
            }
            executor.shutdown();
            while (!executor.isTerminated()) {
            }
        }

        solrIndex.close();
        System.exit(0);
        LOG.info("COMPLETE!");

    }
}
