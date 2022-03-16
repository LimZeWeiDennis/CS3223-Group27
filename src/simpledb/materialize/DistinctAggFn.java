package simpledb.materialize;

import java.util.HashSet;
import java.util.Set;
import simpledb.query.*;


/**
 * The <i>distinct</i> aggregation function.
 * @author Edward Sciore
 */
public class DistinctAggFn implements AggregationFn {
    private AggregationFn fn;
    private Set<Constant> uniqueValues;

    /**
     * Create an distinct on the specified field, before aggregating.
     * @param fn the aggregate fn that needs to be distinct
     */
    public DistinctAggFn(AggregationFn fn) {
        this.fn = fn;
        uniqueValues = new HashSet<>();
    }

    /**
     * Start a new distinct.
     * Since SimpleDB does not support null values,
     * every record will be counted,
     * regardless of the field.
     * The current count is thus set to 1.
     * @see simpledb.materialize.AggregationFn#processFirst(simpledb.query.Scan)
     */
    public void processFirst(Scan s) {
        Constant now = s.getVal(originalFieldName());
        uniqueValues.add(now);
        fn.processFirst(s);


    }

    /**
     * Since SimpleDB does not support null values,
     * this method always increments the count,
     * regardless of the field.
     * @see simpledb.materialize.AggregationFn#processNext(simpledb.query.Scan)
     */
    public void processNext(Scan s) {
        Constant constant = s.getVal(originalFieldName());
//        if (uniqueValues.contains(constant)) {
//            return;
//        }
//        uniqueValues.add(constant);
//        fn.processNext(s);

        if (!uniqueValues.contains(constant)) {
            uniqueValues.add(constant);
            fn.processNext(s);
        }
    }

    /**
     * Return the field's name, prepended by "avgof".
     * @see simpledb.materialize.AggregationFn#fieldName()
     */
    public String fieldName() {
        return fn.fieldName();
    }

    /**
     * Return the name of the original field
     * @return the name of the original field
     */
    public String originalFieldName() { return fn.originalFieldName();};

    /**
     * Return the current average score.
     * @see simpledb.materialize.AggregationFn#value()
     */
    public Constant value() {
        return fn.value();
    }

    public boolean isAggregate() {
        return true;
    }

    public String toString(){ return fn.toStringDistinct();}

    public String toStringDistinct(){ return toString();}
}

