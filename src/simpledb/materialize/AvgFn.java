package simpledb.materialize;

import simpledb.query.*;

/**
 * The <i>average</i> aggregation function.
 * @author Edward Sciore
 */
public class AvgFn implements AggregationFn {
    private String fldname;
    private int count;
    private int sum;

    /**
     * Create an average aggregation function for the specified field.
     * @param fldname the name of the aggregated field
     */
    public AvgFn(String fldname) {
        this.fldname = fldname;
    }

    /**
     * Start a new average.
     * Since SimpleDB does not support null values,
     * every record will be counted,
     * regardless of the field.
     * The current count is thus set to 1.
     * @see simpledb.materialize.AggregationFn#processFirst(simpledb.query.Scan)
     */
    public void processFirst(Scan s) {
        count = 1;
        sum = s.getInt(fldname);
    }

    /**
     * Since SimpleDB does not support null values,
     * this method always increments the count,
     * regardless of the field.
     * @see simpledb.materialize.AggregationFn#processNext(simpledb.query.Scan)
     */
    public void processNext(Scan s) {
        count++;
        sum += s.getInt(fldname);
    }

    /**
     * Return the field's name, prepended by "avgof".
     * @see simpledb.materialize.AggregationFn#fieldName()
     */
    public String fieldName() {
        return "avgof" + fldname;
    }


    /**
     * Return the field's original name
     * @see simpledb.materialize.AggregationFn#fieldName()
     */
    public String originalFieldName() {return fldname;}

    /**
     * Return the current average score.
     * @see simpledb.materialize.AggregationFn#value()
     */
    public Constant value() {
        return new Constant(sum / count);
    }

    public boolean isAggregate() {
        return true;
    }

    public String toString(){ return "Avg {" +  originalFieldName() + "}";}

    public String toStringDistinct(){ return "Avg { distinct" +  originalFieldName() + "}";}
}

