package uk.ac.shef.inf.wdc.indexing;

import org.apache.lucene.search.QueryRescorer;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.core.CoreContainer;

import java.io.IOException;

public class TestQuery {

    public static void main(String[] args) throws SolrServerException, IOException {
        CoreContainer solrContainer = new CoreContainer("/home/zz/Work/data/wdc/wdctable_202012_index");
        solrContainer.load();

        SolrClient entitiesCoreClient = new EmbeddedSolrServer(solrContainer.getCore("entities"));
        SolrQuery query = new SolrQuery();
        query.setQuery("schemaorg_class:Product");
        //query.setSort("random_1234", SolrQuery.ORDER.asc);
        query.setFacet(true);
        query.setFacetMinCount(10);
        query.setFacetLimit(-1);
        query.addFacetField("page_domain");
        QueryResponse qr = entitiesCoreClient.query(query);
        //qr.getFacetFields().get(0).getValues().get(0).
        System.out.println("done");

    }
}
