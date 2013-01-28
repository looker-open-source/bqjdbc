package BQJDBC.QueryResultTest;


import net.starschema.clouddb.jdbc.JdbcGrammarLexer;
import net.starschema.clouddb.jdbc.JdbcGrammarParser;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.Tree;

public class Test {
    
    /**
     * @param args
     * @throws RecognitionException 
     */
    public static void main(String[] args) throws RecognitionException {
        // TODO Auto-generated method stub

        CharStream stream = new ANTLRStringStream("SELECT\r\n" + 
        		"      \"starschema.net:clouddb\":efashion.ARTICLE_LOOKUP.ARTICLE_CODE,\r\n" + 
        		"      \"starschema.net:clouddb\":efashion.ARTICLE_LOOKUP.FAMILY_NAME,\r\n" + 
        		"      ARTICLE_COLOR_LOOKUP.COLOR_CODE,\r\n" + 
        		"      ARTICLE_COLOR_LOOKUP.ARTICLE_LABEL\r\n" + 
        		"FROM\r\n" + 
        		"      \"starschema.net:clouddb\":efashion.ARTICLE_LOOKUP,\r\n" + 
        		"      \"starschema.net:clouddb\":efashion.ARTICLE_COLOR_LOOKUP ARTICLE_COLOR_LOOKUP\r\n" + 
        		"WHERE\r\n" + 
        		"      \"starschema.net:clouddb\":efashion.ARTICLE_LOOKUP.ARTICLE_CODE = \r\n" + 
        		"      ARTICLE_COLOR_LOOKUP.ARTICLE_CODE");
        /*stream = new ANTLRStringStream("SELECT al.CATEGORY,COUNT(acl.ARTICLE_CODE) FROM efashion.ARTICLE_LOOKUP al \r\n" + 
        		" , efashion.ARTICLE_COLOR_LOOKUP acl \r\n" + 
        		"WHERE al.ARTICLE_CODE = acl.ARTICLE_CODE \r\n" + 
        		" GROUP BY al.CATEGORY");*/
        JdbcGrammarLexer lexer = new JdbcGrammarLexer(stream);
        CommonTokenStream tokenstream = new CommonTokenStream(lexer);
        JdbcGrammarParser parser = new JdbcGrammarParser(tokenstream);
        Tree tree = (Tree) parser.selectstatement().getTree();
        
        System.err.println(tree.toStringTree());
    }
    
}
