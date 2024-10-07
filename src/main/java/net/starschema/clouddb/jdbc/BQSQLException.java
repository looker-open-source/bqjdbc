/**
 * Copyright (c) 2015, STARSCHEMA LTD. All rights reserved.
 *
 * <p>Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:
 *
 * <p>1. Redistributions of source code must retain the above copyright notice, this list of
 * conditions and the following disclaimer. 2. Redistributions in binary form must reproduce the
 * above copyright notice, this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * <p>THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package net.starschema.clouddb.jdbc;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An exception that provides information on a database access error or other errors.<br>
 * With logging them to the external logfile.
 *
 * <p>Each SQLException provides several kinds of information:
 * <li>a string describing the error. This is used as the Java Exception message, available via the
 *     method getMesasge.
 * <li>a "SQLstate" string, which follows either the XOPEN SQLstate conventions or the SQL:2003
 *     conventions. The values of the SQLState string are described in the appropriate spec. The
 *     DatabaseMetaData method getSQLStateType can be used to discover whether the driver returns
 *     the XOPEN type or the SQL:2003 type.
 * <li>an integer error code that is specific to each vendor. Normally this will be the actual error
 *     code returned by the underlying database.
 * <li>a chain to a next Exception. This can be used to provide additional error information.
 * <li>the causal relationship, if any for this SQLException.
 *
 * @author Balazs Gunics
 */
public class BQSQLException extends SQLException {

  private static final long serialVersionUID = -3669725541475950504L;
  Logger logger = LoggerFactory.getLogger(BQSQLException.class);

  /**
   * Constructs a SQLException object. The reason, SQLState are initialized to null and the vendor
   * code is initialized to 0. The cause is not initialized, and may subsequently be initialized by
   * a call to the Throwable.initCause(java.lang.Throwable) method.
   */
  public BQSQLException() {
    super();
    StringWriter sw = new StringWriter();
    super.printStackTrace(new PrintWriter(sw));
    String stacktrace = sw.toString();
    this.logger.debug("SQLexception " + stacktrace);
  }

  /**
   * Constructs a SQLException object with a given reason. The SQLState is initialized to null and
   * the vender code is initialized to 0. The cause is not initialized, and may subsequently be
   * initialized by a call to the Throwable.initCause(java.lang.Throwable) method.
   *
   * @param reason - a description of the exception
   */
  public BQSQLException(String reason) {
    super(reason);
    StringWriter sw = new StringWriter();
    super.printStackTrace(new PrintWriter(sw));
    String stacktrace = sw.toString();
    this.logger.debug(reason + stacktrace);
  }

  /**
   * Constructs a SQLException object with a given reason and SQLState. The cause is not
   * initialized, and may subsequently be initialized by a call to the
   * Throwable.initCause(java.lang.Throwable) method. The vendor code is initialized to 0.
   *
   * @param reason - a description of the exception
   * @param sqlState - an XOPEN or SQL:2003 code identifying the exception
   */
  public BQSQLException(String reason, String sqlState) {
    super(reason, sqlState);
    StringWriter sw = new StringWriter();
    super.printStackTrace(new PrintWriter(sw));
    String stacktrace = sw.toString();
    this.logger.debug("SQLexception " + reason + " ;; " + sqlState + " ;; " + stacktrace);
  }

  /**
   * Constructs a SQLException object with a given reason, SQLState and vendorCode. The cause is not
   * initialized, and may subsequently be initialized by a call to the
   * Throwable.initCause(java.lang.Throwable) method.
   *
   * @param reason - a description of the exception
   * @param sqlState - an XOPEN or SQL:2003 code identifying the exception
   * @param vendorCode - a database vendor-specific exception code
   */
  public BQSQLException(String reason, String sqlState, int vendorCode) {
    super(reason, sqlState, vendorCode);
    StringWriter sw = new StringWriter();
    super.printStackTrace(new PrintWriter(sw));
    String stacktrace = sw.toString();
    this.logger.debug(
        "SQLexception " + reason + " " + sqlState + " " + String.valueOf(vendorCode) + stacktrace);
  }

  /**
   * Constructs a SQLException object with a given reason, SQLState, vendorCode and cause.
   *
   * @param reason - a description of the exception
   * @param sqlState - an XOPEN or SQL:2003 code identifying the exception
   * @param vendorCode - a database vendor-specific exception code
   * @param cause - the underlying reason for this SQLException
   */
  public BQSQLException(String reason, String sqlState, int vendorCode, Throwable cause) {
    super(reason, sqlState, vendorCode, cause);
    enrichReason(reason, cause);
    this.logger.debug(
        "SQLexception " + reason + " " + sqlState + " " + String.valueOf(vendorCode), cause);
  }

  /**
   * Constructs a SQLException object with a given reason, SQLState and cause. The vendor code is
   * initialized to 0.
   *
   * @param reason - a description of the exception
   * @param sqlState - an XOPEN or SQL:2003 code identifying the exception
   * @param cause - the underlying reason for this SQLException
   */
  public BQSQLException(String reason, String sqlState, Throwable cause) {
    super(enrichReason(reason, cause), sqlState, cause);
    this.logger.debug("SQLexception " + reason + " " + sqlState, cause);
  }

  /**
   * Constructs a SQLException object with a given reason and cause. The SQLState is initialized to
   * null and the vendor code is initialized to 0.
   *
   * @param reason - a description of the exception
   * @param cause - the underlying reason for this SQLException
   */
  public BQSQLException(String reason, Throwable cause) {
    super(enrichReason(reason, cause), cause);
    this.logger.debug("SQLexception " + reason, cause);
  }

  /**
   * Constructs a SQLException object with a given cause. The SQLState is initialized to null and
   * the vendor code is initialized to 0. The reason is initialized to null if cause==null or to
   * cause.toString() if cause!=null.
   *
   * @param cause - the underlying reason for this SQLException
   */
  public BQSQLException(Throwable cause) {
    super(cause);
    this.logger.debug("SQLexception ", cause);
  }

  /**
   * Append error details in the reason (if some details are available). Most of error details are
   * in the Google exception.
   */
  private static String enrichReason(String reason, Throwable cause) {
    String prefix = reason != null && !reason.isEmpty() ? reason + " - " : "";
    if (cause instanceof GoogleJsonResponseException) {
      GoogleJsonResponseException googleJsonResponseException = (GoogleJsonResponseException) cause;
      if (googleJsonResponseException.getDetails() != null) {
        return prefix + googleJsonResponseException.getDetails().getMessage();
      }
    } else {
      String throwableDetails = cause.toString();
      // if the `cause` itself does not have a detailed message, ask for the message of its cause.
      if (throwableDetails.isEmpty()) {
        Throwable parentCause = cause.getCause();
        if (parentCause != null) throwableDetails = parentCause.toString();
      }
      return prefix + throwableDetails;
    }
    return reason;
  }
}
