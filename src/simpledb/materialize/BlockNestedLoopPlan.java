package simpledb.materialize;

import simpledb.plan.Plan;
import simpledb.query.Predicate;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class BlockNestedLoopPlan implements Plan {

    private Transaction tx;
    private Plan lhs, rhs;
    private Schema schema = new Schema();
    private Predicate pred;

    public BlockNestedLoopPlan(Transaction tx, Plan lhs, Plan rhs, Predicate pred) {
        this.tx = tx;
        this.lhs = lhs;
        this.rhs = rhs;
        this.pred = pred;
        schema.addAll(lhs.schema());
        schema.addAll(rhs.schema());
    }

    public Scan open() {
        TempTable tt = copyRecordsFrom(lhs);
        Scan rhsScan = rhs.open();
        return new BlockNestedLoopScan(tx, rhsScan, tt.tableName(), tt.getLayout(), pred);
    }

    /**
     * Formula = |LHS| + (ceil(|LHS|/) * |RHS|)
     * @return number of page (block) accesses
     */
    public int blocksAccessed() {
        int avail = tx.availableBuffs();
        int size = new MaterializePlan(tx, lhs).blocksAccessed();
        int numchunks = (int) Math.ceil(size / avail);
        return lhs.blocksAccessed() + (numchunks * rhs.blocksAccessed());
    }

    public int recordsOutput() {
        return lhs.recordsOutput() * rhs.recordsOutput() / pred.reductionFactor(this);
    }

    public int distinctValues(String fldname) {
        if (lhs.schema().hasField(fldname)) {
            return lhs.distinctValues(fldname);
        } else {
            return rhs.distinctValues(fldname);
        }
    }

    public Schema schema() {
        return schema;
    }

    /**
     * Materialises the target plan.
     * @param p the plan to materialise.
     * @return a TempTable of the materialised plan.
     */
    private TempTable copyRecordsFrom(Plan p) {
        Scan   src = p.open();
        Schema sch = p.schema();
        TempTable t = new TempTable(tx, sch);
        UpdateScan dest = t.open();
        while (src.next()) {
            dest.insert();
            for (String fldname : sch.fields())
                dest.setVal(fldname, src.getVal(fldname));
        }
        src.close();
        dest.close();
        return t;
    }
}
