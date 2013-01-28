/**
 * Starschema Big Query JDBC Driver
 * Copyright (C) 2012, Starschema Ltd.
 * <p>
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * any later version.
 * </p><p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * </p><p>
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * </p><p>
 * This grammar implement the sql select statement
 * </p>
 *    @author Attila Horvath
 *    @author Balazs Gunics
 */

grammar JdbcGrammar;

options {
  language = Java;
  output = AST;
}
import JDBCTokens;

@header {/**
 * Starschema Big Query JDBC Driver
 * Copyright (C) 2012, Starschema Ltd.
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * 
 * This grammar implement the sql select statement
 *    @author Horvath Attila
 *    @author Balazs Gunics
 */
  package net.starschema.clouddb.jdbc;
}
@lexer::header{/**
 * Starschema Big Query JDBC Driver
 * Copyright (C) 2012, Starschema Ltd.
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * 
 * This grammar implement the sql select statement
 *    @author Horvath Attila
 *    @author Balazs Gunics
 */
  package net.starschema.clouddb.jdbc;
}

/**
Rule for entry point for the parser makes the selectstatement as the root for the Tree
*/
statement
:
  e=selectstatement^ (EOF | ';')! 
;

/**
Rule for parsing an sql select
*/
selectstatement
:   SELECTKEYWORD DISTINCT? expression fromexpression whereexpression? groupbyexpression? havingexpression? orderbyexpression? limitexpression?
    ->^(SELECTSTATEMENT SELECTKEYWORD expression fromexpression whereexpression? groupbyexpression? havingexpression? orderbyexpression? limitexpression?)
    
;

fragment LIMITEXPRESSION:;
/**
BIGQUERY LIMIT expression
*/
limitexpression
:
    LIMITKEYWORD NUMBER ->^(LIMITEXPRESSION NUMBER)
;

/**
Rule for parsing a whole order by expression, creates Tree node ORDERBYEXPRESSION
*/
orderbyexpression
:
    (
    ORDERKEYWORD BYKEYWORD orderbycondition(COMMA orderbycondition)*
    )->^(ORDERBYEXPRESSION orderbycondition*)
;

fragment ORDERBYCLAUSE:;
fragment ORDERBYORDER:;
/**
Rule for parsing a single condition for an order by expression
*/
orderbycondition
:
    column (DESC|ASC)? ->^(ORDERBYCLAUSE column* ^(ORDERBYORDER DESC? ASC?))
;

/**
Rule for parsing the expression followed by the FROMKEYWORD, creates Tree node FROMEXPRESSION
*/
fromexpression
:
    FROMKEYWORD
    datasource
    (
    COMMA     
        datasource
    )*
    ->^(FROMEXPRESSION datasource* ^(TEXT TEXT[$fromexpression.text]))
;

/**
Rule for parsing logical operators
*/
logicaloperator
:
    (andoperator|oroperator)->^(LOGICALOPERATOR andoperator? oroperator? ^(TEXT TEXT[$logicaloperator.text]))
;

andoperator
:
  AND
;

oroperator
:
  OR
;


/**
Rule to parse a where condition expression, creates Tree node WHEREEXPRESSION and nested BOOLEANEXPRESSIONS
*/
whereexpression
:
    (WHEREKEYWORD '1' '=' '1') //do nothing
    | (WHEREKEYWORD '1' '=' '0') ->^(LIMITEXPRESSION)
    | (WHEREKEYWORD expr
    )->^(WHEREEXPRESSION
                    expr
            ^(TEXT TEXT[$whereexpression.text])
        )
;

expr
:
  (disjunction)=>disjunction
  |(conjunction)=>conjunction
  |(subconjunction)=>subconjunction
  |(subdisjunction)=>subdisjunction
  |(exprlvl0)=>exprlvl0
;

subdisjunction
:
  (NOTKEYWORD LPARAM subdisjunction RPARAM)=>(NOTKEYWORD LPARAM subdisjunction RPARAM)->^(NEGATION subdisjunction)
  |(NOTKEYWORD LPARAM disjunction RPARAM)=>(NOTKEYWORD LPARAM disjunction RPARAM)->^(NEGATION disjunction)
  |(LPARAM! subdisjunction RPARAM!)=>(LPARAM! subdisjunction RPARAM!)
  |(LPARAM! disjunction RPARAM!)=>(LPARAM! disjunction RPARAM!)
;

disjunctionpart1
:
  (conjunction)=>conjunction
  |(subconjunction)=>subconjunction
  |(subdisjunction)=>subdisjunction
  |exprlvl0
;

fragment DISJUNCTION:;
disjunction
:
  (disjunctionpart1 disjunctionpart2+)->^(DISJUNCTION disjunctionpart1 disjunctionpart2*)
;

disjunctionpart2
:
  oroperator disjunctionpart1
;

conjunctionpart1
:
  (subconjunction)=>subconjunction
  |(subdisjunction)=>subdisjunction
  |exprlvl0
;

subconjunction
:
  (NOTKEYWORD LPARAM subconjunction RPARAM)=>(NOTKEYWORD LPARAM subconjunction RPARAM)->^(NEGATION subconjunction)
  |(NOTKEYWORD LPARAM conjunction RPARAM)=>(NOTKEYWORD LPARAM conjunction RPARAM)->^(NEGATION conjunction)
  |(LPARAM! subconjunction RPARAM!)=>(LPARAM! subconjunction RPARAM!)
  |(LPARAM! conjunction RPARAM!)=>(LPARAM! conjunction RPARAM!)
;
fragment CONJUNCTION:;
conjunction
:
  (conjunctionpart1 conjunctionpart2+)->^(CONJUNCTION conjunctionpart1 conjunctionpart2*)
;

conjunctionpart2
:
  andoperator conjunctionpart1
;

fragment NEGATION:;
exprlvl0
:
  (NOTKEYWORD LPARAM exprlvl0 RPARAM)=>(NOTKEYWORD LPARAM exprlvl0 RPARAM)->^(NEGATION exprlvl0)
  |(NOTKEYWORD LPARAM exprlvl1 RPARAM)=>(NOTKEYWORD LPARAM exprlvl1 RPARAM)->^(NEGATION exprlvl1)
  |(LPARAM! exprlvl0 RPARAM!)=>(LPARAM! exprlvl0 RPARAM!)
  |(LPARAM! exprlvl1 RPARAM!)=>(LPARAM! exprlvl1 RPARAM!)
  |(exprlvl1)
;

exprlvl1
:
   exprcondition
;

fragment BOOLEANEXPRESSIONITEMLEFT:;
fragment BOOLEANEXPRESSIONITEMRIGHT:;
fragment LIKEEXPRESSION:;
fragment BOOLEANEXPRESSIONITEM:;
/**
Rule to parse a condition expression used in HAVING and WHERE, creates Tree node BOOLEANEXPRESSION and nested column? subquery? comparisonoperator? column? subquery?
STILL NEED TO IMPLEMENT LOT
*/
exprcondition
: 
    (
      s1=exprconditioncore
      (
          (LIKEKEYWORD likeclause)
      |   (comparisonoperator
              (s2=exprconditioncore|s3=number)
              
          )
      )
  )->^(BOOLEANEXPRESSIONITEM ^(BOOLEANEXPRESSIONITEMLEFT $s1) comparisonoperator?  ^(BOOLEANEXPRESSIONITEMRIGHT likeclause? $s2? $s3?) ^(TEXT TEXT[$exprcondition.text]))
;

exprconditioncore:
subquery|columnforexpression|stringliteral
;

columnforexpression
:
  (
   (razoralias COLON)?
   (scopealias (COLON | PUNCTUATION))*
   (name) 
   ( alias)?
  ) ->^(COLUMN scopealias* name alias? ^(TEXT TEXT[$columnforexpression.text]))
;

/**
Rule to parse a having condition expression, creates Tree node HAVINGEXPRESSION and nested BOOLEANEXPRESSIONS
*/
havingexpression
:
    (
    HAVINGKEYWORD expr
    )->^(HAVINGEXPRESSION
                    expr
                    ^(TEXT TEXT[$havingexpression.text])
        )
;



/**
Rule to parse a Group By clause, creates Tree node GROUPBYEXPRESSION
*/
groupbyexpression
:
   (GROUPKEYWORD BYKEYWORD column (COMMA column)*) ->^(GROUPBYEXPRESSION column+ ^(TEXT TEXT[$groupbyexpression.text]))
;

fragment CONDITIONLEFT:;
fragment CONDITIONRIGHT:;


/**
Rule for parsing condition of a JOIN, creates Tree node CONDITION
*/
condition
:
 (
    s1=column comparisonoperator s2=column
 )->^(CONDITION ^(CONDITIONLEFT $s1) comparisonoperator ^(CONDITIONRIGHT $s2) ^(TEXT TEXT[$condition.text]))
;

/**
Rule for parsing a subselect
*/

subquery
:
   (
    LPARAM 
      selectstatement
    RPARAM 
    (alias)?
   )
   ->^(SUBQUERY selectstatement alias? ^(TEXT TEXT[$subquery.text]))
;

fragment LEFTEXPR:;
fragment RIGHTEXPR:;
fragment MULTIJOINEXPRESSION:;
fragment PARENJOIN:;

datasourceelement
:
  (subquery | sourcetable)
;

datasource
:
  datasourcenoparen
  |
  //(LPARAM+ datasourceelement joinclause) => 
  datasourceparen
;

/**
*Rule for parsing datasources after FROMKEYWORD
*/
datasourcenoparen
@init{boolean joinexpr = false;}
:   
   (s1=datasourceelement (joinclause1=joinclause joinelement1=datasourceelement onclause1=onclause (multijoinexpression)* {joinexpr=true;})?)
   -> {joinexpr}?  ^(JOINEXPRESSION 
                        ^(LEFTEXPR $s1?) 
                        $joinclause1?
                        ^(RIGHTEXPR $joinelement1?) $onclause1? multijoinexpression*
                    )                    
   -> $s1                   
;

/**Because of SAP crystal report */
datasourceparen
:
(
  //1-2
                (LPARAM simplejoin RPARAM) multijoinexpression?
 |//2-3
        (LPARAM (LPARAM simplejoin RPARAM) multijoinexpression RPARAM)  multijoinexpression? 
 |//3-4
(LPARAM (LPARAM (LPARAM simplejoin RPARAM) multijoinexpression RPARAM)  multijoinexpression RPARAM) 
multijoinexpression?
 |//4-5
(LPARAM 
(LPARAM (LPARAM (LPARAM simplejoin RPARAM) multijoinexpression RPARAM)  multijoinexpression RPARAM) 
multijoinexpression RPARAM) multijoinexpression?
 |//5-6
(LPARAM (LPARAM 
(LPARAM (LPARAM (LPARAM simplejoin RPARAM) multijoinexpression RPARAM)  multijoinexpression RPARAM) 
multijoinexpression RPARAM) multijoinexpression RPARAM) multijoinexpression?
 |//6-7
(LPARAM (LPARAM (LPARAM 
(LPARAM (LPARAM (LPARAM simplejoin RPARAM) multijoinexpression RPARAM)  multijoinexpression RPARAM) 
multijoinexpression RPARAM) multijoinexpression RPARAM) multijoinexpression RPARAM) multijoinexpression?
 |//7-8
(LPARAM (LPARAM (LPARAM (LPARAM 
(LPARAM (LPARAM (LPARAM simplejoin RPARAM) multijoinexpression RPARAM)  multijoinexpression RPARAM) 
multijoinexpression RPARAM) multijoinexpression RPARAM) multijoinexpression RPARAM) multijoinexpression RPARAM) 
multijoinexpression?
 |//8-9
(LPARAM (LPARAM (LPARAM (LPARAM (LPARAM 
(LPARAM (LPARAM (LPARAM simplejoin RPARAM) multijoinexpression RPARAM)  multijoinexpression RPARAM) 
multijoinexpression RPARAM) multijoinexpression RPARAM) multijoinexpression RPARAM) multijoinexpression RPARAM) 
multijoinexpression RPARAM) multijoinexpression?
 |//9-10
(LPARAM (LPARAM (LPARAM (LPARAM (LPARAM (LPARAM 
(LPARAM (LPARAM (LPARAM simplejoin RPARAM) multijoinexpression RPARAM)  multijoinexpression RPARAM) 
multijoinexpression RPARAM) multijoinexpression RPARAM) multijoinexpression RPARAM) multijoinexpression RPARAM) 
multijoinexpression RPARAM) multijoinexpression RPARAM) multijoinexpression?
 |//10-11
(LPARAM (LPARAM (LPARAM (LPARAM (LPARAM (LPARAM (LPARAM 
(LPARAM (LPARAM (LPARAM simplejoin RPARAM) multijoinexpression RPARAM)  multijoinexpression RPARAM) 
multijoinexpression RPARAM) multijoinexpression RPARAM) multijoinexpression RPARAM) multijoinexpression RPARAM) 
multijoinexpression RPARAM) multijoinexpression RPARAM) multijoinexpression RPARAM) multijoinexpression?
 
 
 )  ->^(JOINEXPRESSION simplejoin multijoinexpression* )                    
;

simplejoin
:
  (s1=datasourceelement joinclause1=joinclause joinelement1=datasourceelement onclause1=onclause )
  -> ^(LEFTEXPR $s1?) $joinclause1? ^(RIGHTEXPR $joinelement1?) $onclause1?        
;

datasourcerecur
:
  datasourceelement  //table | subquery
  |
  LPARAM datasourcerecur multijoinexpression RPARAM
;


multijoinexpression
:
  joinclause datasourceelement onclause 
  -> 
  ^(MULTIJOINEXPRESSION
                        joinclause
                        ^(RIGHTEXPR datasourceelement) onclause
                    )
;

/**
Rule for parsing the Type of a join creates Tree node JOINTYPE (Keyword)
*/
joinclause
:
    (jointypes? JOINKEYWORD)->^(JOINTYPE jointypes? JOINKEYWORD ^(TEXT TEXT[$joinclause.text])) 
;


/**
Rule for parsing the Condition of a join
*/
onclause
:
(ONKEYWORD 
            (
                (LPARAM condition RPARAM) | condition
            )
            )->^(ONCLAUSE condition ^(TEXT TEXT[$onclause.text]))
;



/**
Rule for parsing dataset name in datasource of referred column or table, creates Tree node DATASETNAME
*/
dataset
:
  (  (BACKQUOTE name BACKQUOTE)
   | (SINGLEQUOTE name SINGLEQUOTE) 
   | (DOUBLEQUOTE name DOUBLEQUOTE) 
   | (ESCAPEDDOUBLEQUOTE name ESCAPEDDOUBLEQUOTE) 
   | name
  )
  ->^(DATASETNAME name)
;

/**
Rule for parsing Project name in datasource of referred column or table, creates Tree node PROJECTNAME
*/
project
:
  ( options {greedy=true;} :
    (SINGLEQUOTE (name projectdivider? )+ SINGLEQUOTE)
   | (DOUBLEQUOTE (name projectdivider? )+ DOUBLEQUOTE) 
   | (ESCAPEDDOUBLEQUOTE (name projectdivider? )+ ESCAPEDDOUBLEQUOTE)
   | name
  )
    ->^(PROJECTNAME name (projectdivider name)* )
;
/**
Rule for string dividers. When strings surronded by quotes, they may contain special characters like:
<li> .
<li> :
*/
projectdivider
:
  ( COLON | PUNCTUATION ) ->^(DIVIDER DIVIDER[$projectdivider.text])   
;

/**
Rule for parsing Source Tables Creates Tree node SOURCETABLE with nested project,dataset,name,alias
*/
sourcetable
:
  
    ('['?
     (((project columndivider)? dataset columndivider)? srctablename) 
      ( alias)? ']'?
    )->^(SOURCETABLE '['? project? dataset? srctablename alias? columndivider* ']'?  ^(TEXT TEXT[$sourcetable.text]))
;

srctablename:
(SINGLEQUOTE name SINGLEQUOTE)
| (DOUBLEQUOTE name DOUBLEQUOTE)
| (ESCAPEDDOUBLEQUOTE name ESCAPEDDOUBLEQUOTE)
| (BACKQUOTE name BACKQUOTE)
| name 
;


/**
Rule for naming structures such as columns,tables etc creates Tree node NAME
*/
name
:
 IDENTIFIER ->^(NAME IDENTIFIER)
;


/** Contains 1 or more number(0..9) */
number
:       
   NUMBER->^(INTEGERPARAM NUMBER)
;


fragment JOKERCALL:;
/**
Rule for parsing the expression before from after select, creates Tree node EXPRESSION and nested column,functioncall
*/
expression
:
   '*' ->^(EXPRESSION ^(JOKERCALL '*') ^(TEXT TEXT[$expression.text]))
   | 
   (    
        ( expressionpart ) 
        ( COMMA expressionpart )*
   )->^(EXPRESSION expressionpart* ^(TEXT TEXT[$expression.text]))
;

expressionpart:
column | functioncall | multiplecolumn
;


/**
Rule for identifying a * call from any tables it creates a Tree node named MULTIPLECALL
*/
multiplecolumn
:
    (
     (scopealias PUNCTUATION)+
     '*'
    )->^(MULTIPLECALL (scopealias)* PUNCTUATION* '*' ^(TEXT TEXT[$multiplecolumn.text]))
;
fragment SCOPE:;
scopealias
:
(IDENTIFIER | 
(BACKSINGLEQUOTE IDENTIFIER BACKSINGLEQUOTE) |
(DOUBLEQUOTE IDENTIFIER DOUBLEQUOTE)
 )->^(SCOPE IDENTIFIER) 
;

//for razorSQLs projectname which we won't need
razoralias
:
DOUBLEQUOTE IDENTIFIER ((COLON | PUNCTUATION) IDENTIFIER)+ DOUBLEQUOTE
;

BACKSINGLEQUOTE:
'`'
;

/**
    Rule for identifying an sql function creates tree elements FUNCTIONCALL and nested FUNCTIONPARAMETERS,name,alias and TEXT
*/
functioncall
:
  (
   name LPARAM DISTINCT? functionparameters RPARAM
   ( alias)?
  )->^(FUNCTIONCALL  name functionparameters alias? ^(TEXT TEXT[$functioncall.text]))
;


/**   Paramaters for sql functions    */
functionparameters
:
    (
         //nothing (empty parameter list)
        |( 
            (functionparameterresume) 
            (COMMA functionparameterresume)* 
        ) 
        | joker 
    )->^(FUNCTIONPARAMETERS functionparameterresume* joker? ^(TEXT TEXT[$functionparameters.text]))
;

fragment JOKER:;
joker:
('*')->^(JOKER JOKER)
;

functionparameterresume
:
(mystringliteral | column | number)
;


/** SingelQuote stringliteral for functionparamteres */
mystringliteral
:
literal->^(STRINGLIT literal ^(TEXT TEXT[$mystringliteral.text]))
;
literal
:
(SINGLEQUOTE! (~(SINGLEQUOTE | NL ) | (ESCAPEDSINGLEQUOTE))* SINGLEQUOTE!)
;

/**    Java style Stringliteral and singlequoted*/
stringliteral
:
  stringliteralnode ->^(STRINGLIT stringliteralnode)
;

stringliteralnode:
((SINGLEQUOTE)! (~(SINGLEQUOTE | NL) | (ESCAPEDSINGLEQUOTE))* (SINGLEQUOTE)!)
|
((DOUBLEQUOTE)! (~(DOUBLEQUOTE | NL) | (ESCAPEDDOUBLEQUOTE))* (DOUBLEQUOTE)!)
;

/** types of join */
jointypes: ( INNERKEYWORD  | LEFT_KEYWORD | RIGHT_KEYWORD | FULl_KEYWORD ) ;

/**
Rule for parsing comparison operators
*/
comparisonoperator:
  comparisonoperatorbase ->^(COMPARISONOPERATOR comparisonoperatorbase)
; 

comparisonoperatorbase:
(
  '='  |
  '<>' | 
  '!=' |
  '>'  |
  '<'  |
  '>=' |
  '<='
  )
;




//--------------------------------------------------------------------------------------------
//Hidden things
/**    WhiteSpace Characters (Hidden)*/
WS
:
    (' ' | '\t' | '\n' | '\r' | '\f' | '%n' )+ { $channel = HIDDEN;}
;

/** We don't care about comments */
COMMENT
    :   (   Start_Comment ( options {greedy=false;} : . )* End_Comment )+ 
    {
      $channel=HIDDEN;
    } 
    ;
/** We don't care abut line comments */
LINE_COMMENT
    :   (   ( Line_Comment | '--' ) ~('\n'|'\r')* '\r'? '\n')+ 
    {
      $channel=HIDDEN;
    } 
    ;
fragment KEYWORD:;
    /**
Used for renames for example when you refer a column by not on its original name, creates Tree node ALIAS
*/
alias
:
    
	    (
	        
	        ASKEYWORD IDENTIFIER ->^(ALIAS IDENTIFIER ^(KEYWORD ASKEYWORD) ^(TEXT TEXT[$alias.text]))
	    |   ASKEYWORD aliasliteral ->^(ALIAS aliasliteral ^(KEYWORD ASKEYWORD) ^(TEXT TEXT[$alias.text]))
	    ) 
    |
	    (
	        IDENTIFIER ->^(ALIAS IDENTIFIER ^(TEXT TEXT[$alias.text]))
	    |   aliasliteral ->^(ALIAS aliasliteral ^(TEXT TEXT[$alias.text]))
	    )
;

aliasliteral
:
(SINGLEQUOTE s1=~(SINGLEQUOTE)* SINGLEQUOTE)
| (DOUBLEQUOTE IDENTIFIER DOUBLEQUOTE)
;

/**
Rule for identifying any columns in the syntax, creates Tree node COLUMN and nested project,dataset,table,name,alias
<p>is it starts with 
<li> project.dataset.table.
<li> dataset.table.
<li> table.
</p><p> followed by
<li> name
<li> optionally with alias

*/
column
:
 (
    (scopealias (COLON | PUNCTUATION) )*
   (name | (DOUBLEQUOTE name DOUBLEQUOTE))// | (ESCAPEDDOUBLEQUOTE name ESCAPEDDOUBLEQUOTE)) 
   (alias)?
  ) ->^(COLUMN scopealias* name alias? ^(TEXT TEXT[$column.text]))
;

/** String: '.' or ':' */
columndivider: COLON | PUNCTUATION ;


/**
Rule to parse an sql like expression
*/
likeclause
:
    likesubclause -> ^(LIKEEXPRESSION likesubclause)
;

likesubclause:
SINGLEQUOTE! (~(SINGLEQUOTE | DOUBLEQUOTE | NL ) | (ESCAPEDSINGLEQUOTE|ESCAPEDDOUBLEQUOTE))* SINGLEQUOTE!
;

/**
Rule for parsing table name in datasource of referred column or table, creates Tree node TABLENAME
*/
table
:
   (   (BACKQUOTE IDENTIFIER BACKQUOTE) |
      (SINGLEQUOTE IDENTIFIER SINGLEQUOTE)
    |  (DOUBLEQUOTE IDENTIFIER DOUBLEQUOTE) 
    |  (ESCAPEDDOUBLEQUOTE IDENTIFIER ESCAPEDDOUBLEQUOTE) 
    |   IDENTIFIER
   )
   ->^(TABLENAME SINGLEQUOTE? DOUBLEQUOTE? ESCAPEDDOUBLEQUOTE? BACKQUOTE? ^(NAME IDENTIFIER) SINGLEQUOTE? DOUBLEQUOTE? ESCAPEDDOUBLEQUOTE? BACKQUOTE?)
;