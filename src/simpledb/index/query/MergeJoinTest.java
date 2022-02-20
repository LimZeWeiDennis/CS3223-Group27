package simpledb.index.query;

import java.util.Map;

import simpledb.materialize.MergeJoinPlan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.metadata.*;
import simpledb.plan.*;
import simpledb.query.*;

// Find the grades of all students.

public class MergeJoinTest {
    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("studentdb");
        MetadataMgr mdm = db.mdMgr();
        Transaction tx = db.newTx();

        // Find the index on StudentId.
        Map<String,IndexInfo> indexes = mdm.getIndexInfo("enroll", tx);
        IndexInfo sidIdx = indexes.get("studentid");

        // Get plans for the Student and Enroll tables
        Plan studentplan = new TablePlan(tx, "student", mdm);
        Plan enrollplan = new TablePlan(tx, "enroll", mdm);

        useIndexScan(tx, studentplan, enrollplan, "sid", "studentid");

        tx.commit();
    }

    private static void useIndexScan(Transaction tx, Plan p1, Plan p2, String field1, String field2) {
        // Open an index join scan on the table.
        Plan idxplan = new MergeJoinPlan(tx, p1, p2, field1, field2);
        Scan s = idxplan.open();

        while (s.next()) {
            System.out.println(s.getString("grade"));
        }
        s.close();
    }
}

