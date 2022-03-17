package simpledb.materialize;

import simpledb.query.*;

/**
 * The <i>min</i> aggregation function.
 * @author Ashley Lau
 */
public class MinFn implements AggregationFn {
    private String fldname;
    private Constant val;

    /**
     * Create a min aggregation function for the specified field.
     * @param fldname the name of the aggregated field
     */
    public MinFn(String fldname) {
        this.fldname = fldname;
    }

    /**
     * Start a new minimum to be the
     * field value in the current record.
     * @see simpledb.materialize.AggregationFn#processFirst(simpledb.query.Scan)
     */
    public void processFirst(Scan s) {
        val = s.getVal(fldname);
    }

    /**
     * Replace the current minimum by the field value
     * in the current record, if it is lower.
     * @see simpledb.materialize.AggregationFn#processNext(simpledb.query.Scan)
     */
    public void processNext(Scan s) {
        Constant newval = s.getVal(fldname);
        if (newval.compareTo(val) < 0)
            val = newval;
    }

    /**
     * Return the field's name, prepended by "min(" and appended with ")".
     * @see simpledb.materialize.AggregationFn#fieldName()
     */
    public String fieldName() {
        return "min(" + fldname + ")";
    }

    /**
     * Return the current minimum.
     * @see simpledb.materialize.AggregationFn#value()
     */
    public Constant value() {
        return val;
    }

    @Override
    public boolean isAggregate() {
        return true;
    }
}
