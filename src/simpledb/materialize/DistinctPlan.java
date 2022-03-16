package simpledb.materialize;

import java.util.*;
import simpledb.tx.Transaction;
import simpledb.record.Schema;
import simpledb.plan.Plan;
import simpledb.query.*;

/**
 * The Plan class for the <i>distinct</i> operator.
 * @author Edward Sciore
 */
public class DistinctPlan implements Plan {
    private Plan p;
    private Schema sch ;
    private List<String> fields;

    /**
     * Create a distinct plan for the underlying query.
     * The distinct is based on the fields that
     * are present in the tables
     * @param p a plan for the underlying query
     * @param tx the calling transaction
     */
    public DistinctPlan(Transaction tx, Plan p, List<String> fields) {
        this.p = p;
        this.sch = p.schema();
        this.fields = fields;

    }

    /**
     * This method opens a distinct plan for the specified plan.
     * The distinct plan ensures that the underlying records
     * will be unique.
     * @see simpledb.plan.Plan#open()
     */
    public Scan open() {
        Scan s = p.open(); // this should sort the table before grouping
        return new DistinctScan(s, sch.fields(), fields);
    }

    /**
     * Return the number of blocks required to
     * compute the distinct,
     * which is one pass through the unique table.
     * It does <i>not</i> include the one-time cost
     * of materializing and sorting the records.
     * @see simpledb.plan.Plan#blocksAccessed()
     */
    public int blocksAccessed() {
        return p.blocksAccessed();
    }

    /**
     * Return the number of groups.  Assuming equal distribution,
     * this is the product of the distinct values
     * for each grouping field.
     * @see simpledb.plan.Plan#recordsOutput()
     */
    public int recordsOutput() {
        return p.recordsOutput();
    }

    /**
     * Return the number of distinct values for the
     * specified field.  If the field is a grouping field,
     * then the number of distinct values is the same
     * as in the underlying query.
     * If the field is an aggregate field, then we
     * assume that all values are distinct.
     * @see simpledb.plan.Plan#distinctValues(java.lang.String)
     */
    public int distinctValues(String fldname) {
        if (p.schema().hasField(fldname))
            return p.distinctValues(fldname);
        else
            return recordsOutput();
    }

    /**
     * Returns the schema of the output table.
     * The schema consists of the group fields,
     * plus one field for each aggregation function.
     * @see simpledb.plan.Plan#schema()
     */
    public Schema schema() {
        return sch;
    }

    public String toString() { return "Distinct on ";}
}
