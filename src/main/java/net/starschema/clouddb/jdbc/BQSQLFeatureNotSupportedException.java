/**
 *  Starschema Big Query JDBC Driver
 *  Copyright (C) 2012, Starschema Ltd.
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 2 of the License, or
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.starschema.clouddb.jdbc;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLFeatureNotSupportedException;

import org.apache.log4j.Logger;

/**
 * <p>
 * An exception that provides information on a database access error or other
 * errors.<br>
 * With logging them to the external logfile.
 * <p>
 * Each SQLException provides several kinds of information:
 * <li>a string describing the error. This is used as the Java Exception
 * message, available via the method getMesasge.
 * <li>a "SQLstate" string, which follows either the XOPEN SQLstate conventions
 * or the SQL:2003 conventions. The values of the SQLState string are described
 * in the appropriate spec. The DatabaseMetaData method getSQLStateType can be
 * used to discover whether the driver returns the XOPEN type or the SQL:2003
 * type.
 * <li>an integer error code that is specific to each vendor. Normally this will
 * be the actual error code returned by the underlying database.
 * <li>a chain to a next Exception. This can be used to provide additional error
 * information.
 * <li>the causal relationship, if any for this SQLException.
 * 
 * @author Balazs Gunics
 * 
 */
public class BQSQLFeatureNotSupportedException extends
        SQLFeatureNotSupportedException {

    private static final long serialVersionUID = 5713619285089813617L;
    Logger logger = Logger.getLogger(getClass());

    /**
     * <p>
     * Constructs a SQLFeatureNotSupportedException object.
     * </p>
     * The reason, SQLState are initialized to null and the vendor code is
     * initialized to 0. The cause is not initialized, and may subsequently be
     * initialized by a call to the Throwable.initCause(java.lang.Throwable)
     * method.
     * 
     */
    public BQSQLFeatureNotSupportedException() {
        super();
        StringWriter sw = new StringWriter();
        super.printStackTrace(new PrintWriter(sw));
        String stacktrace = sw.toString();
        logger.debug("SQLFeatureNotSupportedException " + stacktrace);
    }

    /**
     * <p>
     * Constructs a SQLFeatureNotSupportedException object with a given reason.
     * </p>
     * The SQLState is initialized to null and the vender code is initialized to
     * 0. The cause is not initialized, and may subsequently be initialized by a
     * call to the Throwable.initCause(java.lang.Throwable) method.
     * 
     * @param reason
     *            - a description of the exception
     */
    public BQSQLFeatureNotSupportedException(String reason) {
        super(reason);
        StringWriter sw = new StringWriter();
        super.printStackTrace(new PrintWriter(sw));
        String stacktrace = sw.toString();
        logger.debug(reason + stacktrace);
    }

    /**
     * <p>
     * Constructs a SQLFeatureNotSupportedException object with a given cause.
     * </p>
     * The SQLState is initialized to null and the vendor code is initialized to
     * 0. The reason is initialized to null if cause==null or to
     * cause.toString() if cause!=null.
     * 
     * @param cause
     *            - the underlying reason for this SQLException
     */
    public BQSQLFeatureNotSupportedException(Throwable cause) {
        super(cause);
        logger.debug("SQLexception ", cause);
    }

    /**
     * <p>
     * Constructs a SQLFeatureNotSupportedException object with a given reason
     * and SQLState.
     * </p>
     * The cause is not initialized, and may subsequently be initialized by a
     * call to the Throwable.initCause(java.lang.Throwable) method. The vendor
     * code is initialized to 0.
     * 
     * @param reason
     *            - a description of the exception
     * @param sqlState
     *            - an XOPEN or SQL:2003 code identifying the exception
     */
    public BQSQLFeatureNotSupportedException(String reason, String sqlState) {
        super(reason, sqlState);
        StringWriter sw = new StringWriter();
        super.printStackTrace(new PrintWriter(sw));
        String stacktrace = sw.toString();
        logger.debug("SQLexception " + reason + " ;; " + sqlState + " ;; "
                + stacktrace);
    }

    /**
     * <p>
     * Constructs a SQLFeatureNotSupportedException object with a given reason
     * and cause.
     * </p>
     * The SQLState is initialized to null and the vendor code is initialized to
     * 0.
     * 
     * @param reason
     *            - a description of the exception
     * @param cause
     *            - the underlying reason for this SQLException
     */
    public BQSQLFeatureNotSupportedException(String reason, Throwable cause) {
        super(reason, cause);
        logger.debug("SQLexception " + reason, cause);
    }

    /**
     * <p>
     * Constructs a SQLFeatureNotSupportedException object with a given reason,
     * SQLState and vendorCode.
     * </p>
     * The cause is not initialized, and may subsequently be initialized by a
     * call to the Throwable.initCause(java.lang.Throwable) method.
     * 
     * @param reason
     *            - a description of the exception
     * @param sqlState
     *            - an XOPEN or SQL:2003 code identifying the exception
     * @param vendorCode
     *            - a database vendor-specific exception code
     */
    public BQSQLFeatureNotSupportedException(String reason, String sqlState,
            int vendorCode) {
        super(reason, sqlState, vendorCode);
        StringWriter sw = new StringWriter();
        super.printStackTrace(new PrintWriter(sw));
        String stacktrace = sw.toString();
        logger.debug("SQLexception " + reason + " " + sqlState + " "
                + String.valueOf(vendorCode) + stacktrace);
    }

    /**
     * <p>
     * Constructs a SQLFeatureNotSupportedException object with a given reason,
     * SQLState and cause. The vendor code is initialized to 0.
     * </p>
     * 
     * @param reason
     *            - a description of the exception
     * @param sqlState
     *            - an XOPEN or SQL:2003 code identifying the exception
     * @param cause
     *            - the underlying reason for this SQLException
     */
    public BQSQLFeatureNotSupportedException(String reason, String sqlState,
            Throwable cause) {
        super(reason, sqlState, cause);
        logger.debug("SQLexception " + reason + " " + sqlState, cause);
    }

    /**
     * <p>
     * Constructs a SQLFeatureNotSupportedException object with a given reason,
     * SQLState, vendorCode and cause.
     * </p>
     * 
     * @param reason
     *            - a description of the exception
     * @param sqlState
     *            - an XOPEN or SQL:2003 code identifying the exception
     * @param vendorCode
     *            - a database vendor-specific exception code
     * @param cause
     *            - the underlying reason for this SQLException
     */
    public BQSQLFeatureNotSupportedException(String reason, String sqlState,
            int vendorCode, Throwable cause) {
        super(reason, sqlState, vendorCode, cause);
        logger.debug(
                "SQLexception " + reason + " " + sqlState + " "
                        + String.valueOf(vendorCode), cause);
    }

}
