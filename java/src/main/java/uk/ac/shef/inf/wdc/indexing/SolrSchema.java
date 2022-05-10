package uk.ac.shef.inf.wdc.indexing;

public enum SolrSchema {
    FIELD_CLASS("schemaorg_class"),
    FIELD_DESCRIPTION("description_t");

    private final String fieldname;

    SolrSchema(String fieldname) {
        this.fieldname = fieldname;
    }
    public String fieldname() { return this.fieldname; }
}

