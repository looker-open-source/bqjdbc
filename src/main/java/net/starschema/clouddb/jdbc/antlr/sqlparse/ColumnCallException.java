/**
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
 */
package net.starschema.clouddb.jdbc.antlr.sqlparse;

import java.io.PrintWriter;
import java.io.StringWriter;

import net.starschema.clouddb.jdbc.JdbcGrammarParser;

import org.apache.log4j.Logger;

// import net.starschema.clouddb.bqjdbc.logging.Logger;

/**
 * <p>
 * An exception that provides information on ColumnCall error.<br>
 * With logging them to the external logfile.
 * <p>
 * 
 * @author Balazs Gunics, Horvath Attila
 * 
 */
public class ColumnCallException extends Exception {
    
    /**
     * Generated Serial
     */
    private static final long serialVersionUID = 1951923923275407604L;
    // Logger logger = new Logger(TreeParsingException.class.getName());
    Logger logger = Logger.getLogger(ColumnCallException.class.getName());
    
    /**
     * <p>
     * Constructs an Exception object.
     * </p>
     */
    public ColumnCallException() {
        super();
        StringWriter sw = new StringWriter();
        super.printStackTrace(new PrintWriter(sw));
    }
    
    /**
     * <p>
     * Constructs an Exception object.
     * </p>
     * 
     * @param expected
     *            - the token we expected to get
     * @param reality
     *            - the token we got
     */
    public ColumnCallException(int expected, int reality) {
        super("This tree is not an " + JdbcGrammarParser.tokenNames[expected]
                + " ,but " + JdbcGrammarParser.tokenNames[reality] + " .");
        StringWriter sw = new StringWriter();
        super.printStackTrace(new PrintWriter(sw));
    }
    
    /**
     * <p>
     * Constructs an Exception object with a given reason.
     * </p>
     * 
     * @param reason
     *            - a description of the exception
     */
    public ColumnCallException(String reason) {
        super(reason);
        StringWriter sw = new StringWriter();
        super.printStackTrace(new PrintWriter(sw));
    }
    
    /**
     * <p>
     * Constructs an Exception object with a given reason and cause.
     * </p>
     * 
     * @param reason
     *            - a description of the exception
     * @param cause
     *            - the underlying reason for this Exception
     */
    public ColumnCallException(String reason, Throwable cause) {
        super(reason, cause);
    }
    
    /**
     * <p>
     * Constructs an Exception object with a given cause.
     * </p>
     * 
     * @param cause
     *            - the underlying reason for this Exception
     */
    public ColumnCallException(Throwable cause) {
        super(cause);
    }
    
}
