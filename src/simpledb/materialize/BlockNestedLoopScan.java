package simpledb.materialize;

import simpledb.multibuffer.ChunkScan;
import simpledb.query.Constant;
import simpledb.query.Predicate;
import simpledb.query.ProductScan;
import simpledb.query.Scan;
import simpledb.record.Layout;
import simpledb.tx.Transaction;

/**
 * The Scan class for the <i>nestedloopjoin</i> operator.
 *
 * @author Tan Jia Wei Joe
 */
public class BlockNestedLoopScan implements Scan {
    private Transaction tx;
    private Scan rhsScan; // Scan for the rhs table
    private Scan lhsBlockScan; // Scan for the current block
    private Scan joinedScan; // Scan for the cross pdt between lhs block and rhs table
    private String lhsTableName;
    private Layout lhsLayout;
    private int lhsTableSize;
    private int nextBlock;
    private Predicate joinPredicate;

    private final int CHUNK_SIZE;

    /**
     * Creates the scan class for the nested loop join of the LHS scan and a table.
     * @param rhsScan the RHS scan
     * @param lhsTableName the name of the LHS table
     * @param lhsLayout the metadata for the LHS table
     * @param joinPredicate the joining predicate
     * @param tx the current transaction
     * @param numBuffers the number of buffers to use to store the block of the LHS table
     */
    public BlockNestedLoopScan(Transaction tx, Scan rhsScan, String lhsTableName, Layout lhsLayout, Predicate joinPredicate, int numBuffers) {
        this.tx = tx;
        this.rhsScan = rhsScan;
        this.lhsTableName = lhsTableName;
        this.lhsLayout = lhsLayout;
        this.lhsTableSize = tx.size(this.lhsTableName + ".tbl");
        this.joinPredicate = joinPredicate;

        CHUNK_SIZE = numBuffers;

        beforeFirst();
    }

    /**
     * Positions the pointer on the LHS table to the start of the first block.
     * Positions the pointer on the RHS table to the start of the table.
     */
    public void beforeFirst() {
        nextBlock = 0;
        useNextBlock();
    }

    /**
     * Moves to the next record in the current LHS block.
     * If there are no more records in the current block,
     * then move to the next LHS block and the beginning of the RHS scan.
     * @see Scan#next()
     */
    public boolean next() {
        // If this crosspdt finished, move to next chunk
        while (!joinedScan.next()) {
            if (!useNextBlock()) { // Finished looping over left table
                return false;
            }
        }
        if (!joinPredicate.isSatisfied(this)) {
            return next(); // Recurse until find a tuple that satisfies conditions
        }
        return true;
    }

    /**
     * Returns the integer value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     * @see simpledb.query.Scan#getInt(java.lang.String)
     */
    public int getInt(String fldname) {
        return joinedScan.getInt(fldname);
    }

    /**
     * Returns the string value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     * @see simpledb.query.Scan#getString(java.lang.String)
     */
    public String getString(String fldname) {
        return joinedScan.getString(fldname);
    }

    /**
     * Returns the value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     * @see simpledb.query.Scan#getVal(java.lang.String)
     */
    public Constant getVal(String fldname) {
        System.out.println(fldname);
        return joinedScan.getVal(fldname);
    }

    /**
     * Returns true if the specified field is in
     * either of the underlying scans.
     * @see simpledb.query.Scan#hasField(java.lang.String)
     */
    public boolean hasField(String fldname) {
        return joinedScan.hasField(fldname);
    }

    /**
     * Closes the scan by closing the underlying product scan.
     */
    public void close() {
        joinedScan.close();
    }

    /**
     * Moves to the next block in the LHS table and opens a new ChunkScan.
     * The new ChunkScan is then used as part of the ProductScan with the RHS table.
     * Closes the scan for the previous block, if it exists.
     */
    private boolean useNextBlock() {
        if (nextBlock >= lhsTableSize)
            return false;
        if (lhsBlockScan != null) {
            lhsBlockScan.close();
        }
        lhsBlockScan = new ChunkScan(tx, lhsTableName + ".tbl", lhsLayout, nextBlock, nextBlock + CHUNK_SIZE - 1); // get next block (chunk)
        rhsScan.beforeFirst(); // reposition right pointer to start of right table
        joinedScan = new ProductScan(lhsBlockScan, rhsScan);
        nextBlock += CHUNK_SIZE;
        return true;
    }
}