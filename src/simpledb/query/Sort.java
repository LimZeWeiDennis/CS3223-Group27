package simpledb.query;

import java.util.*;

import simpledb.plan.Plan;
import simpledb.record.*;


/**
 * A sort is the order in which the records appear.
 *
 */
public class Sort {
    private List<Expression> flds = new ArrayList<>();
    private List<String> sortTypes = new ArrayList<>();

    public Sort (){};

    public Sort(Expression fld, String sortType){
        flds.add(fld);
        sortTypes.add(sortType);
    }

    /**
     * Modifies the sort to be the conjunction of
     * itself and the specified sort.
     * @param sort the other sort
     */
    public void conjoinWith(Sort sort) {
        flds.addAll(sort.flds);
        sortTypes.addAll(sort.sortTypes);
    }

    /**
     * Return true if the expression is a field reference.
     * @return true if the expression denotes a field
     */
    public boolean hasField() {
        return !flds.isEmpty();
    }


//    /**
//     * Return the field name corresponding to a constant expression,
//     * or null if the expression does not
//     * denote a field.
//     * @return the expression as a field name
//     */
//    public String asFieldName() {
//        return fld.asFieldName();
//    }

    /**
     * Return true if the expression is a field reference.
     * @return true if the expression denotes a field
     */
    public boolean isSortOrder() {
        return !sortTypes.isEmpty();
    }


//    /**
//     * Return the field name corresponding to a constant expression,
//     * or null if the expression does not
//     * denote a field.
//     * @return the expression as a field name
//     */
//    public String asSortOrder() {
//        return sortType;
//    }

    public List<Expression> getFlds(){
        return flds;
    }

    public List<String> getSortTypes(){
        return sortTypes;
    }



    public String toString() {
        Iterator<Expression> iter = flds.iterator();
        if(!iter.hasNext()) {
            return "";
        }
        String result = iter.next().toString();
        while(iter.hasNext()){
            result += "," + iter.next().toString();
        }
        return result;
//        return fld.asFieldName() + " " + sortType;
    }
}
