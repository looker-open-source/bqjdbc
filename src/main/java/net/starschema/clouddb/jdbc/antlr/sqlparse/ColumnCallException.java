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
package net.starschema.clouddb.jdbc.antlr.sqlparse;

import java.io.PrintWriter;
import java.io.StringWriter;

import net.starschema.clouddb.jdbc.JdbcGrammarParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    Logger logger = LoggerFactory.getLogger(ColumnCallException.class);

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
