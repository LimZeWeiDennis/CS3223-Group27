package simpledb.materialize;

import java.util.*;

import simpledb.query.*;
import simpledb.record.*;

/**
 * The Scan class for the <i>hashjoin</i> operator.
 * @author Ashley Lau
 */
public class HashJoinScan implements Scan {
    private UpdateScan s1, s2;
    private final List<TempTable> partition1, partition2;
    private final String fldname1, fldname2;
    private HashMap<Integer, List<RID>> map;
    private int currPartIdx;
    private int currBucketElementIdx = 0; // To keep track of the current position element within the partition
    private boolean hasMoreInPart2 = true;
    private final int secondaryModulo;
    private RID startingS1Position;

    /**
     * Create a hashjoin scan for the two underlying sorted scans.
     * @param partition1 the partitions of LHS scan
     * @param partition2 the partitions of RHS scan
     * @param fldname1 the LHS join field
     * @param fldname2 the RHS join field
     */
    public HashJoinScan(List<TempTable> partition1, List<TempTable> partition2, String fldname1, String fldname2) {
        this.partition1 = partition1;
        this.partition2 = partition2;
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;
        this.secondaryModulo = (int) (partition1.size() * 1.5);
        beforeFirst();
    }

    /**
     * Position the scan before the first partition and record for partitions 1 and 2
     * @see simpledb.query.Scan#beforeFirst()
     */
    public void beforeFirst() {
        this.currPartIdx = 0;
        s1 = partition1.get(currPartIdx).open();
        s2 = partition2.get(currPartIdx).open();
        s1.beforeFirst();
        s2.beforeFirst();
    }

    /**
     * Close the scan by closing the two underlying scans.
     * @see simpledb.query.Scan#close()
     */
    public void close() {
        s1.close();
        s2.close();
    }

    /**
     * Move to the s2 next record.  This is where the action is.
     * <P>
     * If the next current s2 record has the same join value and the current s1 bucket, moves the pointer of the element
     * in curr s1 bucket to the next element in the bucket if possible, else moves to the next s2 record
     * @see simpledb.query.Scan#next()
     */
    public boolean next() {
        if (!hasMoreInPart2) { // move to the next bucket of partition2
            map = null;
            currPartIdx++;
        }
        if (currPartIdx >= partition1.size()) { // All buckets have been processed
            return false;
        }

        if (map == null) { // Need to create hashmap for the currentPartition of s1
            if (currPartIdx > 0) {
                close();
                s1 = partition1.get(currPartIdx).open();
                s2 = partition2.get(currPartIdx).open();
            }
            createHashTableForPartition();
            hasMoreInPart2 = s2.next();
        }

        while (hasMoreInPart2) {
            int secondaryHash = getSecondaryHash(s2, fldname2);
            List<RID> bucket = map.get(secondaryHash);

            if (currBucketElementIdx >= bucket.size()) {
                // If code reaches here, there are no more values in the s1 partition map for the current s2, hence
                // reposition s1 to receive the next s2 value
                s1.moveToRid(startingS1Position);
                currBucketElementIdx = 0;
                hasMoreInPart2 = s2.next();
                continue;
            }
            if (currBucketElementIdx == 0) {
                startingS1Position = s1.getRid();
            }
            s1.moveToRid(bucket.get(currBucketElementIdx));
            currBucketElementIdx++;
            return true;
        }
        return next();
    }

    /**
     * Secondary hash so that Constants will be mapped to different buckets
     */
    private int getSecondaryHash(Scan s, String fieldName) {
        return s.getVal(fieldName).hashCode() % secondaryModulo % partition1.size();
    }

    /**
     * Recreates the hashmap for new idx and populates it with the current s1 partition
     */
    private void createHashTableForPartition() {
        HashMap<Integer, List<RID>> map = new HashMap<>();
        for (int i = 0; i < partition2.size(); i++) {
            map.computeIfAbsent(i, k -> new ArrayList<>());
        }
        while(s1.next()) {
            int hashVal = getSecondaryHash(s1, fldname1);
            List<RID> row = map.get(hashVal);
            row.add(s1.getRid());
        }
        this.map = map;
    }

    /**
     * Return the integer value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     * @see simpledb.query.Scan#getInt(java.lang.String)
     */
    public int getInt(String fldname) {
        if (s1.hasField(fldname))
            return s1.getInt(fldname);
        else
            return s2.getInt(fldname);
    }

    /**
     * Return the value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     * @see simpledb.query.Scan#getVal(java.lang.String)
     */
    public Constant getVal(String fldname) {
        if (s1.hasField(fldname))
            return s1.getVal(fldname);
        else
            return s2.getVal(fldname);
    }

    /**
     * Returns the string value of the specified field.
     * @see simpledb.query.Scan#getVal(java.lang.String)
     */
    public String getString(String fldname) {
        if (s1.hasField(fldname))
            return s1.getString(fldname);
        else
            return s2.getString(fldname);
    }

    /**
     * Return true if the specified field is in
     * either of the underlying scans.
     * @see simpledb.query.Scan#hasField(java.lang.String)
     */
    public boolean hasField(String fldname) {
        return s1.hasField(fldname) || s2.hasField(fldname);
    }
}
