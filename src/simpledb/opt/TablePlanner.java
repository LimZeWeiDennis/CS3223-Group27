package simpledb.opt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import simpledb.materialize.MergeJoinPlan;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.metadata.*;
import simpledb.index.planner.*;
import simpledb.multibuffer.MultibufferProductPlan;
import simpledb.plan.*;

/**
 * This class contains methods for planning a single table.
 * @author Edward Sciore
 */
class TablePlanner {
   private TablePlan myplan;
   private Predicate mypred;
   private Schema myschema;
   private Map<String,IndexInfo> indexes;
   private Transaction tx;
   private Sort sort;
   
   /**
    * Creates a new table planner.
    * The specified predicate applies to the entire query.
    * The table planner is responsible for determining
    * which portion of the predicate is useful to the table,
    * and when indexes are useful.
    * @param tblname the name of the table
    * @param mypred the query predicate
    * @param tx the calling transaction
    */
   public TablePlanner(String tblname, Predicate mypred, Transaction tx, MetadataMgr mdm) {
      this.mypred  = mypred;
      this.tx  = tx;
      myplan   = new TablePlan(tx, tblname, mdm);
      myschema = myplan.schema();
      indexes  = mdm.getIndexInfo(tblname, tx);
   }


   public TablePlanner(String tblname, Predicate mypred, Transaction tx, MetadataMgr mdm, Sort sort) {
      this.mypred  = mypred;
      this.tx  = tx;
      myplan   = new TablePlan(tx, tblname, mdm);
      myschema = myplan.schema();
      indexes  = mdm.getIndexInfo(tblname, tx);
      this.sort = sort;
   }
   
   /**
    * Constructs a select plan for the table.
    * The plan will use an indexselect, if possible.
    * @return a select plan for the table.
    */
   public Plan makeSelectPlan() {
      Plan p = makeIndexSelect();
      if (p == null)
         p = myplan;
      return addSelectPred(p);
   }
   
   /**
    * Constructs a join plan of the specified plan
    * and the table.  The plan will use an indexjoin, if possible.
    * (Which means that if an indexselect is also possible,
    * the indexjoin operator takes precedence.)
    * The method returns null if no join is possible.
    * @param current the specified plan
    * @return a join plan of the plan and this table
    */
   public Plan makeJoinPlan(Plan current) {
      System.out.println("making a joing plan");
      Schema currsch = current.schema();
      Predicate joinpred = mypred.joinSubPred(myschema, currsch);
      if (joinpred == null)
         return null;
      Plan p = null;
      List<Plan> planList = new ArrayList<>();
      Plan indexJoinPlan = makeIndexJoin(current, currsch, joinpred);
      Plan mergeJoinPlan = makeMergeJoin(current, currsch, joinpred);
//      planList.add(indexJoinPlan);
      planList.add(mergeJoinPlan);

      for(Plan plan : planList){
         if(plan == null) continue;
         if (p == null || plan.blocksAccessed() < p.blocksAccessed()) {
            p = plan;
         }
      }
      p = mergeJoinPlan;

      if (planList.isEmpty() || p == null)
         p = makeProductJoin(current, currsch);

      System.out.println("returning a join plan");
      return p;
   }
   
   /**
    * Constructs a product plan of the specified plan and
    * this table.
    * @param current the specified plan
    * @return a product plan of the specified plan and this table
    */
   public Plan makeProductPlan(Plan current) {
      Plan p = addSelectPred(myplan);
      return new MultibufferProductPlan(tx, current, p);
   }
   
   private Plan makeIndexSelect() {
      for (String fldname : indexes.keySet()) {
         Constant val = mypred.equatesWithConstant(fldname);
         if (val != null) {
            IndexInfo ii = indexes.get(fldname);
            System.out.println("index on " + fldname + " used");
            return new IndexSelectPlan(myplan, ii, val);
         }
      }
      return null;
   }
   
   private Plan makeIndexJoin(Plan current, Schema currsch, Predicate pred) {
      for (String fldname : indexes.keySet()) {
         String outerfield = pred.equatesWithField(fldname);
         if (outerfield != null && currsch.hasField(outerfield)) {
            IndexInfo ii = indexes.get(fldname);
            Plan p = new IndexJoinPlan(current, myplan, ii, outerfield);
            p = addSelectPred(p);
            return addJoinPred(p, currsch);
         }
      }
      return null;
   }

   private Plan makeMergeJoin(Plan current ,Schema currsch, Predicate pred) {
      //get the fieldname from the predicate
      //get the list of fldnames from the schemas, loop through and see if fits into the predicate
      // currently working with only one join column

      for(String fldname: myschema.fields()){
         String outerfield = pred.equatesWithField(fldname);
         if(outerfield != null && currsch.hasField(outerfield)){
            Plan p = new MergeJoinPlan(tx, myplan, current, fldname, outerfield, sort);
            p = addSelectPred(p);
//            p = addJoinPred(p, currsch);

            System.out.println("mergeJoin plan being used now");
            System.out.println(p.blocksAccessed());
            return p;
         }
      }

      return null;
   }
   
   private Plan makeProductJoin(Plan current, Schema currsch) {
      System.out.println("using product plan now");
      Plan p = makeProductPlan(current);
      return addJoinPred(p, currsch);
   }
   
   private Plan addSelectPred(Plan p) {
      Predicate selectpred = mypred.selectSubPred(myschema);
      if (selectpred != null)
         return new SelectPlan(p, selectpred);
      else
         return p;
   }

   /*
   *returns a plan if the predicate holds on both schemas
    */
   private Plan addJoinPred(Plan p, Schema currsch) {
      //checks and creates a predicate based on the schema of the plan and myscheme
      Predicate joinpred = mypred.joinSubPred(currsch, myschema);
      if (joinpred != null)
         return new SelectPlan(p, joinpred);
      else
         return p;
   }
}
