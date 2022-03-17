package simpledb.materialize;

import simpledb.multibuffer.BufferNeeds;
import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

import java.util.*;

/**
 * The scan class corresponding to the hashjoin relational
 * algebra operator.
 * @author Ashley Lau
 */
public class HashJoinPlan implements Plan {
    private final Transaction tx;
    private final Plan lhs, rhs;
    private final String fldname1, fldname2;
    private final Schema sch = new Schema();

    /**
     * Implements the join operator,
     * using the specified LHS and RHS plans.
     * @param p1 the left-hand plan
     * @param p2 the right-hand plan
     * @param fldname1 information about the right-hand index
     * @param fldname2 information about the left-hand index
     */
    public HashJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2) {
        this.tx = tx;
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;
        this.lhs = p1;
        this.rhs = p2;
        sch.addAll(p1.schema());
        sch.addAll(p2.schema());
    }

    /**
     * Opens an hashjoin scan for this query
     * @see simpledb.plan.Plan#open()
     */
    public Scan open() {
        Scan s1 = lhs.open();
        // Determines the max amount of partitions for this hash join based on the buffer needs
        int numOfBlocks = (int) Math.ceil(lhs.recordsOutput() / (double) tx.blockSize());
        int numOfPartitions = BufferNeeds.bestRoot(tx.availableBuffs(), numOfBlocks + 1);;
        List<TempTable> partition1 = partitionPlan(lhs.schema(), s1, fldname1, numOfPartitions);
        s1.close();
        Scan s2 = rhs.open();
        List<TempTable> partition2 = partitionPlan(rhs.schema(), s2, fldname2, numOfPartitions);
        s2.close();
        return new HashJoinScan(partition1, partition2, fldname1, fldname2);
    }

    /**
     * Partitions the plans based on the hashcode method of constant and the join fldname
     */
    private List<TempTable> partitionPlan(Schema sch, Scan s, String partfldname, int numOfPartitions) {
        s.beforeFirst();
        List<TempTable> tempPartitions = new ArrayList<>();
        UpdateScan[] openedScans = new UpdateScan[numOfPartitions];
        for (int i = 0; i < numOfPartitions; i++) { // creating of the partitions
            TempTable currentTemp = new TempTable(tx, sch);
            tempPartitions.add(currentTemp);
            openedScans[i] = currentTemp.open();
        }
        while (s.next()) {
            int idx = s.getVal(partfldname).hashCode() % numOfPartitions;
            UpdateScan currScan = openedScans[idx];
            currScan.insert();
            for (String fld : sch.fields()) { // Populating the temp table to be processed by the next hash
                currScan.setVal(fld, s.getVal(fld));
            }
        }
        for (Scan scan : openedScans) {
            scan.close();
        }
        return tempPartitions;
    }

    /**
     * Returns an estimate of the number of block accesses
     * required to execute the query. The formula is:
     * <pre> B(hashjoin(p1,p2)) = 3 * (B(p1), B(p2)) </pre>
     * where B(p1) is the number of blocks of p1.
     * @see simpledb.plan.Plan#blocksAccessed()
     */
    public int blocksAccessed() {
        return 3 * (lhs.blocksAccessed() + rhs.blocksAccessed());
    }

    /**
     * Return the number of records in the join.
     * Assuming uniform distribution, the formula is:
     * <pre> R(hashjoin(p1,p2)) = R(p1)*R(p2)/max{V(p1,F1),V(p2,F2)}</pre>
     * @see simpledb.plan.Plan#recordsOutput()
     */
    public int recordsOutput() {
        int maxvals = Math.max(lhs.distinctValues(fldname1),
                rhs.distinctValues(fldname2));
        return (lhs.recordsOutput() * rhs.recordsOutput()) / maxvals;
    }

    /**
     * Estimate the distinct number of field values in the join.
     * Since the join does not increase or decrease field values,
     * the estimate is the same as in the appropriate underlying query.
     * @see simpledb.plan.Plan#distinctValues(java.lang.String)
     */
    public int distinctValues(String fldname) {
        if (lhs.schema().hasField(fldname))
            return lhs.distinctValues(fldname);
        else
            return rhs.distinctValues(fldname);
    }

    /**
     * Return the schema of the join,
     * which is the union of the schemas of the underlying queries.
     * @see simpledb.plan.Plan#schema()
     */
    public Schema schema() {
        return sch;
    }
}
