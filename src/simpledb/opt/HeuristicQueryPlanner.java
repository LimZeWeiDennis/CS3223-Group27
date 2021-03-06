package simpledb.opt;

import java.util.*;

import simpledb.query.Expression;
import simpledb.query.Sort;
import simpledb.record.Schema;
import simpledb.tx.Transaction;
import simpledb.materialize.*;
import simpledb.metadata.MetadataMgr;
import simpledb.parse.QueryData;
import simpledb.plan.*;

import javax.swing.*;

/**
 * A query planner that optimizes using a heuristic-based algorithm.
 * @author Edward Sciore
 */
public class HeuristicQueryPlanner implements QueryPlanner {
   private Collection<TablePlanner> tableplanners = new ArrayList<>();
   private MetadataMgr mdm;
   
   public HeuristicQueryPlanner(MetadataMgr mdm) {
      this.mdm = mdm;
   }
   
   /**
    * Creates an optimized left-deep query plan using the following
    * heuristics.
    * H1. Choose the smallest table (considering selection predicates)
    * to be first in the join order.
    * H2. Add the table to the join order which
    * results in the smallest output.
    */
   public Plan createPlan(QueryData data, Transaction tx) {

      
      // Step 1:  Create a TablePlanner object for each mentioned table
      for (String tblname : data.tables()) {
         TablePlanner tp = new TablePlanner(tblname, data.pred(), tx, mdm);
         tableplanners.add(tp);
      }
      
      // Step 2:  Choose the lowest-size plan to begin the join order
      Plan currentplan = getLowestSelectPlan();

      
      // Step 3:  Repeatedly add a plan to the join order
      while (!tableplanners.isEmpty()) {
         Plan p = getLowestJoinPlan(currentplan);
         if (p != null)
            currentplan = p;
         else  // no applicable join
            currentplan = getLowestProductPlan(currentplan);
      }

      // Optional step: Do Group By (if not empty)
      if (!data.groupByFields().isEmpty()) {
         List<Expression> exprs = new ArrayList<>();
         List<String> sortTypes = new ArrayList<>();
         for (String field : data.groupByFields()) {
            exprs.add(new Expression(field));
            sortTypes.add("asc");
         }
         // Sort the table before performing group by
         Sort s = new Sort(exprs, sortTypes);

         currentplan = new GroupByPlan(tx, currentplan, data.groupByFields(), data.aggFnsFields(), s);

      }

      if (data.groupByFields().isEmpty() && data.aggFnsFields().size() > 0) { // aggFn without groupBy clause
         Sort s = new Sort(new ArrayList<>(), new ArrayList<>());
         currentplan = new GroupByPlan(tx, currentplan, data.groupByFields(), data.aggFnsFields(), s);
      }

      if(data.sort().isSortOrder()){
         // Step 4.  Sort the table if there is an order by clause
         currentplan = new SortPlan(tx, currentplan, data.sort());
      }

      if(data.isDistinct()){
         currentplan = new DistinctPlan(tx, currentplan, data.fields());
      }

      // Step 5.  Project on the field names and return
      currentplan = new ProjectPlan(currentplan, data.fields());
      System.out.println(currentplan.toString());
      return currentplan;
   }

   private Plan getLowestJoinPlan(Plan current) {
      TablePlanner besttp = null;
      Plan bestplan = null;
      for (TablePlanner tp : tableplanners) {
         Plan plan = tp.makeJoinPlan(current);
         if (plan != null && (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput())) {
            besttp = tp;
            bestplan = plan;
         }
      }
      if (bestplan != null){
         tableplanners.remove(besttp);
      }

      return bestplan;
   }
   
   private Plan getLowestSelectPlan() {
      TablePlanner besttp = null;
      Plan bestplan = null;
      for (TablePlanner tp : tableplanners) {
         Plan plan = tp.makeSelectPlan();
         if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
            besttp = tp;
            bestplan = plan;
         }
      }
      tableplanners.remove(besttp);

      return bestplan;
   }
   

   
   private Plan getLowestProductPlan(Plan current) {
      TablePlanner besttp = null;
      Plan bestplan = null;
      for (TablePlanner tp : tableplanners) {
         Plan plan = tp.makeProductPlan(current);
         if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
            besttp = tp;
            bestplan = plan;
         }
      }
      tableplanners.remove(besttp);
      return bestplan;
   }

   public void setPlanner(Planner p) {
      // for use in planning views, which
      // for simplicity this code doesn't do.
   }
}
