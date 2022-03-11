package simpledb.parse;

import java.util.*;
import java.io.*;

/**
 * The lexical analyzer.
 * @author Edward Sciore
 */
public class Lexer {
   private Collection<String> keywords;
   private StreamTokenizer tok;
   
   /**
    * Creates a new lexical analyzer for SQL statement s.
    * @param s the SQL statement
    */
   public Lexer(String s) {
      initKeywords();
      tok = new StreamTokenizer(new StringReader(s));
      tok.ordinaryChar('.');   //disallow "." in identifiers
      tok.wordChars('_', '_'); //allow "_" in identifiers
      tok.lowerCaseMode(true); //ids and keywords are converted
      nextToken();
   }
   
//Methods to check the status of the current token
   
   /**
    * Returns true if the current token is
    * the specified delimiter character.
    * @param d a character denoting the delimiter
    * @return true if the delimiter is the current token
    */
   public boolean matchDelim(char d) {
      return d == (char)tok.ttype;
   }

   
   /**
    * Returns true if the current token is an integer.
    * @return true if the current token is an integer
    */
   public boolean matchIntConstant() {
      return tok.ttype == StreamTokenizer.TT_NUMBER;
   }
   
   /**
    * Returns true if the current token is a string.
    * @return true if the current token is a string
    */
   public boolean matchStringConstant() {
      return '\'' == (char)tok.ttype;
   }
   
   /**
    * Returns true if the current token is the specified keyword.
    * @param w the keyword string
    * @return true if that keyword is the current token
    */
   public boolean matchKeyword(String w) {
      return tok.ttype == StreamTokenizer.TT_WORD && tok.sval.equals(w);
   }
   
   /**
    * Returns true if the current token is a legal identifier.
    * @return true if the current token is an identifier
    */
   public boolean matchId() {
      return  tok.ttype==StreamTokenizer.TT_WORD && !keywords.contains(tok.sval);
   }

   /**
    * Returns true if the current token is a legal index type (hash/ b+tree).
    * @return true if the current token is an index type
    */
   public boolean matchIndexType() {

      return  tok.ttype==StreamTokenizer.TT_WORD
              && !keywords.contains(tok.sval)
              && (tok.sval.equals("hash") || tok.sval.equals("btree"));

   }

   /**
    * Returns true if the current token is a legal aggregate keyword.
    * @return true if the current token is a legal aggregate keyword
    */
   public boolean matchAggFn() {
      String[] aggTypes = new String[] { "sum", "count", "avg", "min", "max" };
      return Arrays.stream(aggTypes).anyMatch(this::matchKeyword);
   }

   // Methods to "eat" the current token
   
   /**
    * Throws an exception if the current token is not the
    * specified delimiter. 
    * Otherwise, moves to the next token.
    * @param d a character denoting the delimiter
    */
   public void eatDelim(char d) {
      if (!matchDelim(d))
         throw new BadSyntaxException();
      nextToken();
   }

   /**
    * Throws an exception if the current token is not
    * an operator
    * Otherwise, moves to the next token.
    * returns a string of the comparator
    */
   public String eatEquality() {

      String comparator;
      if(matchDelim('<')){

         eatDelim('<');

         if(matchDelim('=')){

            eatDelim('=');
            comparator = "<=";

         } else if(matchDelim('>')){

            eatDelim('>');
            comparator = "<>";

         } else {
            comparator = "<";
         }


      } else if(matchDelim('>')){

         eatDelim('>');

         if(matchDelim('=')){
            eatDelim('=');
            comparator = ">=";
         } else {
            comparator = ">";
         }

      } else if(matchDelim('!')){

         eatDelim('!');

         // no need to check if the match '='
         // since after ! must be =
         eatDelim('=');

         comparator = "!=";


      } else if (matchDelim('=')) {

         eatDelim('=');

         comparator = "=";
      } else {

         throw new BadSyntaxException();
      }

      return comparator;
   }
   
   /**
    * Throws an exception if the current token is not 
    * an integer. 
    * Otherwise, returns that integer and moves to the next token.
    * @return the integer value of the current token
    */
   public int eatIntConstant() {
      if (!matchIntConstant()) {
         throw new BadSyntaxException();
      }
      int i = (int) tok.nval;
      nextToken();
      return i;
   }
   
   /**
    * Throws an exception if the current token is not 
    * a string. 
    * Otherwise, returns that string and moves to the next token.
    * @return the string value of the current token
    */
   public String eatStringConstant() {
      if (!matchStringConstant())
         throw new BadSyntaxException();
      String s = tok.sval; //constants are not converted to lower case
      nextToken();
      return s;
   }
   
   /**
    * Throws an exception if the current token is not the
    * specified keyword. 
    * Otherwise, moves to the next token.
    * @param w the keyword string
    */
   public void eatKeyword(String w) {
      if (!matchKeyword(w))
         throw new BadSyntaxException();
      nextToken();
   }
   
   /**
    * Throws an exception if the current token is not 
    * an identifier. 
    * Otherwise, returns the identifier string 
    * and moves to the next token.
    * @return the string value of the current token
    */
   public String eatId() {
      if (!matchId())
         throw new BadSyntaxException();
      String s = tok.sval;
      nextToken();
      return s;
   }

   /**
    * Throws an exception if the current token is not
    * an index type.
    * Otherwise, returns the index type string
    * and moves to the next token.
    * @return the string value of the current token
    */
   public String eatIndexType() {
      if (!matchIndexType())
         throw new BadSyntaxException();
      String s = tok.sval;
      nextToken();
      return s;
   }

   /**
    * Throws an exception if the current token is not "by".
    * Otherwise, eats the current token
    * and moves to the next token.
    */
   public String eatSort() {
      String res;
      if(matchKeyword("desc")){
         eatKeyword("desc");
         res = "desc";
      } else {
         // still have to account for case where they want to put asc
         if(matchKeyword("asc")){
            eatKeyword("asc");
         }
         res = "asc";
      }

      return res;
   }

   public String eatAgg() {
      String[] aggTypes = new String[] { "sum", "count", "avg", "min", "max" };
      for (String agg: aggTypes) {
         if (matchKeyword(agg)) {
            return agg;
         }
      }
      throw new BadSyntaxException();
   }

   public List<String> eatGroupBy() {
      List<String> groupByList = new ArrayList<>();
      eatKeyword("group");
      eatKeyword("by");
      groupByList.add(eatId());
      while (matchDelim(',')) {
         eatDelim(',');
         groupByList.add(eatId());
      }
      return groupByList;
   }

   private void nextToken() {
      try {
         tok.nextToken();
      }
      catch(IOException e) {
         throw new BadSyntaxException();
      }
   }


   //initialises keywords with array of keywords
   private void initKeywords() {
      keywords = Arrays.asList("select", "from", "where", "and",
                               "insert", "into", "values", "delete", "update", "set", 
                               "create", "table", "int", "varchar", "view", "as", "index",
                               "on", "using", "order", "by", "asc", "desc", "sum", "count",
                               "avg", "min", "max", "group");
   }
}