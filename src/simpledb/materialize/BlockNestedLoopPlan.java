package simpledb.materialize;

import simpledb.plan.Plan;
import simpledb.query.Predicate;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

/**
 * The Plan class for the <i>nestedloopjoin</i> operator.
 * @author Tan Jia Wei Joe
 */
public class BlockNestedLoopPlan implements Plan {

    private Transaction tx;
    private Plan lhs, rhs;
    private Schema schema = new Schema();
    private Predicate pred;
    private int numBuffers;

    /**
     * Creates a nested loop join plan for the two specified queries.
     * @param lhs the LHS query plan
     * @param rhs the RHS query plan
     * @param pred the joining predicate
     * @param numBuffers the number of buffers to use to store each block
     * @param tx the calling transaction
     */
    public BlockNestedLoopPlan(Transaction tx, Plan lhs, Plan rhs, Predicate pred, int numBuffers) {
        this.tx = tx;
        this.lhs = lhs;
        this.rhs = rhs;
        this.pred = pred;
        this.numBuffers = numBuffers;
        schema.addAll(lhs.schema());
        schema.addAll(rhs.schema());
    }

    /**
     * A scan for this query is created and returned, as follows.
     * First, the method materializes its LHS queries.
     * It creates a chunk plan for each block based on the number of buffers to use, saving them in a list.
     * Finally, it creates a multiscan for this list of plans and returns that scan.
     * @see Plan#open()
     */
    public Scan open() {
        TempTable tt = copyRecordsFrom(lhs);
        Scan rhsScan = rhs.open();
        return new BlockNestedLoopScan(tx, rhsScan, tt.tableName(), tt.getLayout(), pred, numBuffers);
    }

    /**
     * Returns the number of page accesses required to perform the join.
     * Formula = |LHS| + (ceil(|LHS|/block_size) * |RHS|)
     * @return number of page (block) accesses
     */
    public int blocksAccessed() {
        int size = new MaterializePlan(tx, lhs).blocksAccessed();
        int numchunks = (int) Math.ceil(size / numBuffers);
        return lhs.blocksAccessed() + (numchunks * rhs.blocksAccessed());
    }

    /**
     * Returns the number of records output by the join.
     * Assuming uniform distribution of data,
     * Formula = R(p1)*R(p2)/max{V(p1,F1),V(p2,F2)}
     *
     * @see Plan#recordsOutput()
     * @return number of records output by the join.
     */
    public int recordsOutput() {
        return lhs.recordsOutput() * rhs.recordsOutput() / pred.reductionFactor(this);
    }

    /**
     * Estimates the distinct number of field values in the product.
     * Since the product does not increase or decrease field values,
     * the estimate is the same as in the appropriate underlying query.
     * @see simpledb.plan.Plan#distinctValues(java.lang.String)
     */
    public int distinctValues(String fldname) {
        if (lhs.schema().hasField(fldname)) {
            return lhs.distinctValues(fldname);
        } else {
            return rhs.distinctValues(fldname);
        }
    }

    /**
     * Return the schema of the product,
     * which is the union of the schemas of the underlying queries.
     *
     * @see Plan#schema()
     */
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

    public String toString(){
        return String.format("[{%s} block-nested loop join ](%s)",
                lhs.toString(), rhs.toString(), pred.toString());
    }
}