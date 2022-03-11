package simpledb.materialize;


public class DefaultField implements Field {
    private String fldname;

    public DefaultField(String fldname) {
        this.fldname = fldname;
    };

    public String fieldName() {
        return fldname;
    }

    public boolean isAggregate() {
        return false;
    }
}
