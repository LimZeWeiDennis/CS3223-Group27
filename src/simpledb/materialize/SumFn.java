package simpledb.materialize;

import simpledb.query.*;

/**
 * The <i>sum</i> aggregation function.
 * @author Ashley Lau
 */
public class SumFn implements AggregationFn {
    private String fldname;
    private int sum;

    /**
     * Create a sum aggregation function for the specified field.
     * @param fldname the name of the aggregated field
     */
    public SumFn(String fldname) {
        this.fldname = fldname;
    }

    /**
     * Start a new sum.
     * The current sum is thus set to the value of the field in the first tuple.
     * @see simpledb.materialize.AggregationFn#processFirst(simpledb.query.Scan)
     */
    public void processFirst(Scan s) {
        sum = s.getInt(fldname);
    }

    /**
     * Since SimpleDB does not support null values,
     * this method always increments the sum,
     * regardless of the field.
     * @see simpledb.materialize.AggregationFn#processNext(simpledb.query.Scan)
     */
    public void processNext(Scan s) {
        sum += s.getInt(fldname);
    }

    /**
     * Return the field's name, prepended by "sum(" and appended with ")".
     * @see simpledb.materialize.AggregationFn#fieldName()
     */
    public String fieldName() {
        return "sum(" + fldname + ")";
    }

    /**
     * Return the field's original name
     * @see simpledb.materialize.AggregationFn#fieldName()
     */
    public String originalFieldName() {return fldname;}

    /**
     * Return the current sum.
     * @see simpledb.materialize.AggregationFn#value()
     */
    public Constant value() {
        return new Constant(sum);
    }

    @Override
    public boolean isAggregate() {
        return true;
    }

    public String toString(){ return "Sum {" +  originalFieldName() + "}";}

    public String toStringDistinct(){ return "Sum { distinct" +  originalFieldName() + "}";}
}
