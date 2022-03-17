package simpledb.parse;

import java.sql.Array;
import java.util.*;

import simpledb.materialize.*;
import simpledb.query.*;
import simpledb.record.*;

/**
 * The SimpleDB parser.
 * @author Edward Sciore
 */
public class Parser {
   private Lexer lex;
   
   public Parser(String s) {
      System.out.println(s);
      lex = new Lexer(s);
   }
   
// Methods for parsing predicates, terms, expressions, constants, and fields
   public String field() {
      return lex.eatId();
   }

   public Field selectField() { // previous checks ensures
      if (lex.matchAggFn()) {
         String aggType = lex.eatAgg();
         lex.eatDelim('(');
         String fldname = lex.eatId();
         lex.eatDelim(')');
         if (aggType.equals("sum")) { // previous checks ensures that a valid group by exists
            return new SumFn(fldname);
         } else if (aggType.equals("count")) {
            return new CountFn(fldname);
         } else if (aggType.equals("avg")) {
            return new AvgFn(fldname);
         } else if (aggType.equals("min")) {
            return new MinFn(fldname);
         } else { // max
            return new MaxFn(fldname);
         }
      }
      return new DefaultField(lex.eatId());
   }
   
   public Constant constant() {
      if (lex.matchStringConstant())
         return new Constant(lex.eatStringConstant());
      else
         return new Constant(lex.eatIntConstant());
   }
   
   public Expression expression() {

      if(lex.matchAggFn()){
         String aggType = lex.eatAgg();
         lex.eatDelim('(');
         String fldname = field();
         lex.eatDelim(')');
         String fieldName = aggType + "(" + fldname + ")";
         return new Expression(fieldName);
      }

      if (lex.matchId())
         return new Expression(field());
      else
         return new Expression(constant());
   }
   
   public Term term() {
      Expression lhs = expression();

      String comparator = lex.eatEquality();

      Expression rhs = expression();

      return new Term(lhs, rhs, comparator);
   }
   
   public Predicate predicate() {
      Predicate pred = new Predicate(term());
      if (lex.matchKeyword("and")) {
         lex.eatKeyword("and");
         pred.conjoinWith(predicate());
      }
      return pred;
   }

   public List<String> groupBy() {
      if (lex.matchKeyword("group")) {
         return lex.eatGroupBy();
      }
      // Return empty list if no group by fields
      return new ArrayList<>();
   }

   //method that creates a sort object
   public Sort sort() {

      System.out.println("made into parser sort method");
      Expression fld = expression();

      String sortType = lex.eatSort();

      Sort sort = new Sort(fld, sortType);

      //combine sort with the next sort type and field
      if(lex.matchDelim(',')){
         lex.eatDelim(',');
         sort.conjoinWith(sort());
      }
      System.out.println("made to the end of parser sort method");
      return sort;
   }
   
// Methods for parsing queries
   
   public QueryData query() {
      lex.eatKeyword("select");
      List<Field> fields = selectList(); // Can either be an aggregate function or normal field (must appear in group by)
      lex.eatKeyword("from");
      Collection<String> tables = tableList();
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }

      // "group by" after "where" and before "order by"
      List<String> groupByFields = groupBy();

      List<String> selectFields = new ArrayList<>();
      List<AggregationFn> aggFns = new ArrayList<>();
      boolean hasAggregate = false;

      for (Field field : fields) { // processing the aggregate functions
         selectFields.add(field.fieldName());
         if (field.isAggregate()) {
            hasAggregate = true;
            aggFns.add((AggregationFn) field);
            continue;
         }

         if (groupByFields.size() == 0 && hasAggregate) { // has aggregate and another non aggregate field
            throw new BadSyntaxException(); // Throws error if there is a violation
         }

         if (groupByFields.size() > 0) { // only does the checks if group by clause exists and for non-aggregate fields
            // not correct since field.fieldName prepends the fn name infront of the field
            boolean defaultSelectExistInGB = groupByFields.stream().anyMatch(x -> x.equals(field.fieldName()));
            if (!defaultSelectExistInGB) { // all non aggregate select fields must appear in group by fields,
               throw new BadSyntaxException(); // Throws error if there is a violation
            }
         }
      }

      // must "where" first before "order by"
      // think about how to include multiple sorts
      Sort sort = new Sort();
      if(lex.matchKeyword("order")) {
         lex.eatKeyword("order");
         lex.eatKeyword("by");
         sort = sort();

      }
      return new QueryData(selectFields, tables, pred, groupByFields, aggFns, sort, fields);
   }
   
   private List<Field> selectList() {
      List<Field> L = new ArrayList<>();
      L.add(selectField());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(selectList());
      }
      return L;
   }
   
   private Collection<String> tableList() {
      Collection<String> L = new ArrayList<String>();
      L.add(lex.eatId());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(tableList());
      }
      return L;
   }
   
// Methods for parsing the various update commands
   
   public Object updateCmd() {
      if (lex.matchKeyword("insert"))
         return insert();
      else if (lex.matchKeyword("delete"))
         return delete();
      else if (lex.matchKeyword("update"))
         return modify();
      else
         return create();
   }
   
   private Object create() {
      lex.eatKeyword("create");
      if (lex.matchKeyword("table"))
         return createTable();
      else if (lex.matchKeyword("view"))
         return createView();
      else
         return createIndex();
   }
   
// Method for parsing delete commands
   
   public DeleteData delete() {
      lex.eatKeyword("delete");
      lex.eatKeyword("from");
      String tblname = lex.eatId();
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }
      return new DeleteData(tblname, pred);
   }
   
// Methods for parsing insert commands
   
   public InsertData insert() {
      lex.eatKeyword("insert");
      lex.eatKeyword("into");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      List<String> flds = fieldList();
      lex.eatDelim(')');
      lex.eatKeyword("values");
      lex.eatDelim('(');
      List<Constant> vals = constList();
      lex.eatDelim(')');
      return new InsertData(tblname, flds, vals);
   }
   
   private List<String> fieldList() {
      List<String> L = new ArrayList<String>();
      L.add(field());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(fieldList());
      }
      return L;
   }
   
   private List<Constant> constList() {
      List<Constant> L = new ArrayList<Constant>();
      L.add(constant());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(constList());
      }
      return L;
   }
   
// Method for parsing modify commands
   
   public ModifyData modify() {
      lex.eatKeyword("update");
      String tblname = lex.eatId();
      lex.eatKeyword("set");
      String fldname = field();
      lex.eatDelim('=');
      Expression newval = expression();
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }
      return new ModifyData(tblname, fldname, newval, pred);
   }
   
// Method for parsing create table commands
   
   public CreateTableData createTable() {
      lex.eatKeyword("table");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      Schema sch = fieldDefs();
      lex.eatDelim(')');
      return new CreateTableData(tblname, sch);
   }
   
   private Schema fieldDefs() {
      Schema schema = fieldDef();
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         Schema schema2 = fieldDefs();
         schema.addAll(schema2);
      }
      return schema;
   }
   
   private Schema fieldDef() {
      String fldname = field();
      return fieldType(fldname);
   }
   
   private Schema fieldType(String fldname) {
      Schema schema = new Schema();
      if (lex.matchKeyword("int")) {
         lex.eatKeyword("int");
         schema.addIntField(fldname);
      }
      else {
         lex.eatKeyword("varchar");
         lex.eatDelim('(');
         int strLen = lex.eatIntConstant();
         lex.eatDelim(')');
         schema.addStringField(fldname, strLen);
      }
      return schema;
   }
   
// Method for parsing create view commands
   
   public CreateViewData createView() {
      lex.eatKeyword("view");
      String viewname = lex.eatId();
      lex.eatKeyword("as");
      QueryData qd = query();
      return new CreateViewData(viewname, qd);
   }
   
   
//  Method for parsing create index commands
   
   public CreateIndexData createIndex() {
      lex.eatKeyword("index");
      String idxname = lex.eatId();
      lex.eatKeyword("on");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      String fldname = field();
      lex.eatDelim(')');
      lex.eatKeyword("using");
      String idxType = lex.eatIndexType();
      return new CreateIndexData(idxname, tblname, fldname, idxType);
   }
}

