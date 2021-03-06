package simpledb.materialize;

import java.util.*;

import simpledb.query.*;

/**
 * A comparator for scans.
 * @author Edward Sciore
 */
public class RecordComparator implements Comparator<Scan> {
   private Sort sort;
   /**
    * Create a comparator using the specified fields,
    * using the ordering implied by its iterator.
    * @param sort a list of field names and the sort order
    */
   public RecordComparator(Sort sort) {
      this.sort = sort;
   }
   
   /**
    * Compare the current records of the two specified scans.
    * The sort fields are considered in turn.
    * When a field is encountered for which the records have
    * different values, those values are used as the result
    * of the comparison.
    * If the two records have the same values for all
    * sort fields, then the method returns 0.
    * @param s1 the first scan
    * @param s2 the second scan
    * @return the result of comparing each scan's current record according to the field list
    */
   public int compare(Scan s1, Scan s2) {
      List<Expression> sortFields = sort.getFlds();
      List<String> sortTypes= sort.getSortTypes();

      for(int i = 0; i < sortFields.size() ; i ++){
         Constant val1 = s1.getVal(sortFields.get(i).toString());
         Constant val2 = s2.getVal(sortFields.get(i).toString());

         int result = 0;

         if(sortTypes.get(i) == "desc"){
            result = val2.compareTo(val1);
         } else {
            result = val1.compareTo(val2);
         }

         if (result != 0)
            return result;
      }
      return 0;
   }
}
