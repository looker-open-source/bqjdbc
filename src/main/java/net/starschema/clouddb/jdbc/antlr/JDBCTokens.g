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
 *
 * This grammar implement the sql select statement
 *    @author Horvath Attila
 *    @author Balazs Gunics
 */

lexer grammar JDBCTokens;

//--------------------------------------------------------------------------------------
//Strings keywords

/** AND keyword, case insensitive */
AND:    A N D;
/** AS keyword, case insensitive */
ASKEYWORD:  A S ;
/** ASC or ASCENDING keyword, case insensitive */
ASC:            (A S C) | (A S C E N D I N G);

/** BY keyword, case insensitive */
BYKEYWORD:      B Y;

/** DESC or DESCENDING keyword, case insensitive */
DESC:           (D E S C)| (D E S C E N D I N G);

/** DISTINCT keyword, case insensitive */
DISTINCT: D I S T I N C T ;

/** EACH keyword, case insensitive */
EACH: E A C H ;

/** FULL keyword, case insensitive  */
FULl_KEYWORD: F U L L ;
/** FROM keyword, case insensitive */
FROMKEYWORD:    F R O M ;

/** GROUP keyword, case insensitive */
GROUPKEYWORD:   G R O U P;

/** HAVING keyword, case insensitive */
HAVINGKEYWORD:  H A V I N G;

/** INNER keyword, case insensitive */
INNERKEYWORD:   I N N E R;

/** JOIN keyword, case insensitive */
JOINKEYWORD:    J O I N;

/** LEFT keyword, case insensitive  */
LEFT_KEYWORD: L E F T ;
/** LIKE keyword, case insensitive */
LIKEKEYWORD:    L I K E ;
/** LIMIT keyword, case insensitive */
LIMITKEYWORD: L I M I T;

/** NOT keyword, case insensitive */
NOTKEYWORD:     N O T;

/** ON keyword, case insensitive */
ONKEYWORD:      O N;
/** OR keyword, case insensitive */
OR:     O R;
/** ORDER keyword, case insensitive */
ORDERKEYWORD:   O R D E R;

/** RIGHT keyword, case insensitive  */
RIGHT_KEYWORD: R I G H T ;

/** SELECT keyword, case insensitive */
SELECTKEYWORD:  S E L E C T ;

/** WHERE keyword, case insensitive */
WHEREKEYWORD:   W H E R E;



//Other strings

/** String: '`' */
BACKQUOTE :   '`' ;

/** String: ':' */
COLON:        ':' ;
/** String: ',' */
COMMA:        ',';

/** String: '\"' */
DOUBLEQUOTE:  '\"'    ;

/** String: '\\"' */
ESCAPEDDOUBLEQUOTE :   '\\"'   ;
/** String:  \\\' */
ESCAPEDSINGLEQUOTE: '\\\'';

/** String for '(' */
LPARAM:       '(';

/** String: '.' */
PUNCTUATION:  '.';

/** String for ')' */
RPARAM:       ')';

/** String: ';' */
SEMICOLON:    ';';
/** String: \' */
SINGLEQUOTE:  '\'';

//Identifiers for string, number etc.

/** Contains 1 or more number(0..9) */
NUMBER
:
DIGIT+
;

/**    Any Identifier (Used for function names, column names etc)*/
IDENTIFIER
:
    (LOWCHAR | HIGHCHAR | '_' | DIGIT)(LOWCHAR | HIGHCHAR | DIGIT | '_' | '%')*
;

fragment NL
:
    '\r' | '\n'
;

//--------------------------------------------------------------------------------------------
//Hidden things
/**    WhiteSpace Characters (Hidden)*/
WS
:
    (' ' | '\t' | '\n' | '\r' | '\f')+ {$channel = HIDDEN;}
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


/** lowercase letters */
fragment LOWCHAR
:   'a'..'z';
/** uppercase letters */
fragment HIGHCHAR
:   'A'..'Z';
/** numbers */
fragment DIGIT
:   '0'..'9';


// fragments for the tree
fragment ALIAS:;
fragment BOOLEANEXPRESSION:;
fragment COLUMN:;
fragment COMPARISONOPERATOR:;
fragment CONDITION:;
fragment DATASETNAME:;
fragment DATASOURCE:;
fragment DIVIDER:;
fragment EXPRESSION:;
fragment FROMEXPRESSION:;
fragment FUNCTIONCALL:;
fragment FUNCTIONPARAMETERS:;
fragment GROUPBYEXPRESSION:;
fragment HAVINGEXPRESSION:;
fragment INTEGERPARAM:;
fragment JOINEXPRESSION:;
fragment JOINTYPE:;
fragment LOGICALOPERATOR:;
fragment MULTIPLECALL:;
fragment NAME:;
fragment ONCLAUSE:;
fragment ORDERBYEXPRESSION:;
fragment PROJECTNAME:;
fragment SELECTSTATEMENT:;
fragment SOURCETABLE:;
fragment STRINGLIT:;
fragment SUBQUERY:;
fragment TABLENAME:;
fragment TEXT:;
fragment WHEREEXPRESSION:;
fragment EXPRESSIONTEXT:;



fragment A:('a'|'A');
fragment B:('b'|'B');
fragment C:('c'|'C');
fragment D:('d'|'D');
fragment E:('e'|'E');
fragment F:('f'|'F');
fragment G:('g'|'G');
fragment H:('h'|'H');
fragment I:('i'|'I');
fragment J:('j'|'J');
fragment K:('k'|'K');
fragment L:('l'|'L');
fragment M:('m'|'M');
fragment N:('n'|'N');
fragment O:('o'|'O');
fragment P:('p'|'P');
fragment Q:('q'|'Q');
fragment R:('r'|'R');
fragment S:('s'|'S');
fragment T:('t'|'T');
fragment U:('u'|'U');
fragment V:('v'|'V');
fragment W:('w'|'W');
fragment X:('x'|'X');
fragment Y:('y'|'Y');
fragment Z:('z'|'Z');


// C o m m e n t   T o k e n s
fragment
Start_Comment   : '/*';

fragment
End_Comment     : '*/';

fragment
Line_Comment    : '//';
