/**
 * Copyright (c) 2015, STARSCHEMA LTD.
 * All rights reserved.

 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:

 * 1. Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.

 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
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
