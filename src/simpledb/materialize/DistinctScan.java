package simpledb.materialize;

import java.util.*;

import simpledb.query.*;

/**
 * The Scan class for the <i>distinct</i> operator.
 * @author Edward Sciore
 */
public class DistinctScan implements Scan {
    private Scan s;
    private List<String> fields;
    private Set<List<Constant>> uniqueRecords;
    private List<String> selectFields;

    /**
     * Create a distinct scan.
     * @param s the distinct scan
     */
    public DistinctScan(Scan s, List<String> fields, List<String> selectFields) {
        this.s = s;
        uniqueRecords = new HashSet<>();
        this.fields = fields;
        this.selectFields = selectFields;
        beforeFirst();
    }

    /**
     * Position the scan before the first group.
     * Internally, the underlying scan is always
     * positioned at the first record of a group, which
     * means that this method moves to the
     * first underlying record.
     * @see simpledb.query.Scan#beforeFirst()
     */
    public void beforeFirst() {
        s.beforeFirst();
    }

    /**
     * Move to the next tuple.
     * Adds the list of constants of the tuple into the set
     * constants that are already in the set is not added
     * List of Constants added are within the selectfields
     * @see simpledb.query.Scan#next()
     */
    public boolean next() {

        if(!s.next()) {
            return false;
        }

        List<Constant> currentTuple = new ArrayList<>();
        for(String field :fields){
            if(selectFields.contains(field)){
                currentTuple.add(getVal(field));
            }
        }

        //while the next tuple already exists
        while(uniqueRecords.contains(currentTuple)){
            if(!s.next()){
                return false;
            } else {
                currentTuple = new ArrayList<>();
                for(String field :fields){
                    if(selectFields.contains(field)){
                        currentTuple.add(getVal(field));
                    }
                }
            }
        }
        uniqueRecords.add(currentTuple);

        return true;

    }

    /**
     * Close the scan by closing the underlying scan.
     * @see simpledb.query.Scan#close()
     */
    public void close() {
        s.close();
    }

    /**
     * Get the Constant value of the specified field.
     * If the field is a group field, then its value can
     * be obtained from the saved group value.
     * Otherwise, the value is obtained from the
     * appropriate aggregation function.
     * @see simpledb.query.Scan#getVal(java.lang.String)
     */
    public Constant getVal(String fldname) { // all fields in the schema must be in groupBy or be aggregated
        if (fields.contains(fldname))
            return s.getVal(fldname);

        throw new RuntimeException("field " + fldname + " not found.");
    }

    /**
     * Get the integer value of the specified field.
     * If the field is a group field, then its value can
     * be obtained from the saved group value.
     * Otherwise, the value is obtained from the
     * appropriate aggregation function.
     * @see simpledb.query.Scan#getVal(java.lang.String)
     */
    public int getInt(String fldname) {
        return getVal(fldname).asInt();
    }

    /**
     * Get the string value of the specified field.
     * If the field is a group field, then its value can
     * be obtained from the saved group value.
     * Otherwise, the value is obtained from the
     * appropriate aggregation function.
     * @see simpledb.query.Scan#getVal(java.lang.String)
     */
    public String getString(String fldname) {
        return getVal(fldname).asString();
    }

    /** Return true if the specified field is either a
     * grouping field or created by an aggregation function.
     * @see simpledb.query.Scan#hasField(java.lang.String)
     */
    public boolean hasField(String fldname) {
        if (fields.contains(fldname))
            return true;
        return false;
    }
}

