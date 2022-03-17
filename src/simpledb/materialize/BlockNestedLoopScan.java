package simpledb.materialize;

import simpledb.multibuffer.ChunkScan;
import simpledb.query.Constant;
import simpledb.query.Predicate;
import simpledb.query.ProductScan;
import simpledb.query.Scan;
import simpledb.record.Layout;
import simpledb.tx.Transaction;

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

    public BlockNestedLoopScan(Transaction tx, Scan rhsScan, String lhsTableName, Layout lhsLayout, Predicate joinPredicate) {
        this.tx = tx;
        this.rhsScan = rhsScan;
        this.lhsTableName = lhsTableName;
        this.lhsLayout = lhsLayout;
        this.lhsTableSize = tx.size(lhsTableName + ".tbl");
        this.joinPredicate = joinPredicate;
//        System.out.println("Available buffers: " + tx.availableBuffs());
        CHUNK_SIZE = 1; // -2 for output buffer and buffer to hold S

        beforeFirst();
    }

    /**
     * Positions the pointer on the LHS table to the start of the first block.
     */
    public void beforeFirst() {
        nextBlock = 0;
        useNextBlock();
    }

    /**
     * Moves to the next record in the current scan.
     * If there are no more records in the current chunk,
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

    public int getInt(String fldname) {
        return joinedScan.getInt(fldname);
    }

    public String getString(String fldname) {
        return joinedScan.getString(fldname);
    }

    public Constant getVal(String fldname) {
        return joinedScan.getVal(fldname);
    }

    public boolean hasField(String fldname) {
        return joinedScan.hasField(fldname);
    }

    public void close() {
        System.out.println("JoinedScan closed");
        joinedScan.close();
        if (lhsBlockScan != null) {
            lhsBlockScan.close();
        }
        if (rhsScan != null) {
            rhsScan.close();
        }
        System.out.println("JoinedScan fin closing");
    }

    private boolean useNextBlock() {
        if (nextBlock >= lhsTableSize)
            return false;
        if (lhsBlockScan != null) {
            lhsBlockScan.close();
            System.out.println("LHS Scan closed");
        }
        lhsBlockScan = new ChunkScan(tx, lhsTableName, lhsLayout, nextBlock, nextBlock + CHUNK_SIZE - 1); // get next block (chunk)
        rhsScan.beforeFirst(); // reposition right pointer to start of right table
        joinedScan = new ProductScan(lhsBlockScan, rhsScan);
        nextBlock += CHUNK_SIZE + 1;
        return true;
    }
}