package simpledb.materialize;

import simpledb.query.*;

/**
 * The interface implemented by aggregation functions.
 * Aggregation functions are used by the <i>groupby</i> operator.
 * @author Edward Sciore
 */
public interface AggregationFn extends Field {
   
   /**
    * Use the current record of the specified scan
    * to be the first record in the group.
    * @param s the scan to aggregate over.
    */
   void processFirst(Scan s);
   
   /**
    * Use the current record of the specified scan
    * to be the next record in the group.
    * @param s the scan to aggregate over.
    */
   void processNext(Scan s);

   /**
    * Return the name of the new aggregation field.
    * @return the name of the new aggregation field
    */
   String fieldName();

   /**
    * Return the name of the original field
    * @return the name of the original field
    */
   String originalFieldName();

   /**
    * Return the computed aggregation value.
    * @return the computed aggregation value
    */
   Constant value();

   /**
    * Return a string containing the type of aggregate function
    * @return a string containing the type of aggregate function
    */
   String toString();

   /**
    * Return a string containing the type of aggregate function if distinct is used in the field
    * @return a string containing the type of aggregate function
    */
   String toStringDistinct();
}
