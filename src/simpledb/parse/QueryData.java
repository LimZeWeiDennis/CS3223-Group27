package simpledb.parse;

import java.util.*;

import simpledb.materialize.AggregationFn;
import simpledb.materialize.Field;
import simpledb.query.*;

/**
 * Data for the SQL <i>select</i> statement.
 * @author Edward Sciore
 */
public class QueryData {
   private List<String> fields;
   private Collection<String> tables;
   private Predicate pred;
   private List<String> groupByFields;
   private List<AggregationFn> aggFns;
   private List<Field> originalSelect;
   private Sort sort;
   private boolean isDistinct;
   
   /**
    * Saves the field and table list and predicate.
    */
   public QueryData(List<String> fields, Collection<String> tables, Predicate pred, List<String> groupByFields,
                    List<AggregationFn> aggFns, Sort sort, List<Field> originalSelect, boolean isDistinct) {
      this.fields = fields;
      this.tables = tables;
      this.pred = pred;
      this.groupByFields = groupByFields;
      this.aggFns = aggFns;
      this.sort = sort;
      this.originalSelect = originalSelect;
      this.isDistinct = isDistinct;

   }
   
   /**
    * Returns the fields mentioned in the select clause.
    * @return a list of field names
    */
   public List<String> fields() {
      return fields;
   }
   
   /**
    * Returns the tables mentioned in the from clause.
    * @return a collection of table names
    */
   public Collection<String> tables() {
      return tables;
   }
   
   /**
    * Returns the predicate that describes which
    * records should be in the output table.
    * @return the query predicate
    */
   public Predicate pred() {
      return pred;
   }

   /**
    * Returns the list of fields in the Group By clause.
    * @return the list of fields in the Group By clause.
    */
   public List<String> groupByFields() {
      return groupByFields;
   }

   /**
    * Returns the list of agg functions in the select clause.
    * @return the list of fields in the select clause.
    */
   public List<AggregationFn> aggFnsFields() {
      return aggFns;
   }

   /**
    * Returns if the select result is distinct.
    * @return true if select result is disctinct.
    */
   public boolean isDistinct() {
      return isDistinct;
   }

   /**
    * Returns the sort that describes which
    * which order by which field.
    * @return the query sort
    */
   public Sort sort() {
      return sort;
   }

   
   public String toString() {
      String result = "select";
      for (Field orgSelect: originalSelect) {
         result += orgSelect.fieldName() + ", ";
      }
      result = result.substring(0, result.length()-2); //remove final comma
      result += " from ";
      for (String tblname : tables)
         result += tblname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma
      String predstring = pred.toString();
      if (!predstring.equals(""))
         result += " where " + predstring;
      if (!groupByFields.isEmpty()) { // to look at now
         result += " group by ";
         for (String field : groupByFields)
            result += field + ", ";
         result = result.substring(0, result.length()-2); //remove final comma
      }
      String sortString = sort.toString();
      result += sortString;

      return result;
   }
}
