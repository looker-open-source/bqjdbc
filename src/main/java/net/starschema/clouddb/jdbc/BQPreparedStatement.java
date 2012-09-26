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
 * 
 * This class implements the
 * java.sql.PreparedStatement interface
 */

package net.starschema.clouddb.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import com.google.api.services.bigquery.model.Job;

/**
 * This class implements java.sql.PreparedStatement
 * 
 * @author Horváth Attila
 * @author Balazs Gunics
 * 
 */
public class BQPreparedStatement extends BQStatementRoot implements
        PreparedStatement {
    
    /** Storage for implementing PreparedStatement */
    String PrecompiledSQL = null;
    String RunnableStatement = null;
    
    /**
     * Reference for the Conatainer that contains parameters for
     * preparedstatement
     */
    String[] Parameters = null;
    
    /**
     * Constructor for BQStatement object just initializes local variables as
     * preparedstatement
     * 
     * @param querysql
     * @param projectid
     * @param bqConnection
     */
    public BQPreparedStatement(String querysql, String projectid,
            BQConnection bqConnection) {
        this.logger.debug("Constructor of PreparedStatement Running");
        
        this.ProjectId = projectid;
        this.connection = bqConnection;
        this.resultSetType = ResultSet.TYPE_SCROLL_INSENSITIVE;
        this.resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
        this.PrecompiledSQL = querysql;
        if (!this.PrecompiledSQL.contains("?")) {
            this.RunnableStatement = this.PrecompiledSQL;
        }
        
        int count = 0;
        for (int i = 0; i < querysql.length(); i++) {
            if (querysql.charAt(i) == '?') {
                count++;
            }
        }
        
        if (count != 0) {
            this.Parameters = new String[count];
        }
        
        this.logger.debug("Constructor of PreparedStatement Ended with "
                + projectid);
    }
    
    /**
     * Constructor for BQStatement object just initializes local variables as a
     * PreparedStatement
     * 
     * @param querysql
     * @param projectid
     * @param bqConnection
     * @param resultSetType
     * @param resultSetConcurrency
     * @throws BQSQLException
     */
    public BQPreparedStatement(String querysql, String projectid,
            BQConnection bqConnection, int resultSetType,
            int resultSetConcurrency) throws BQSQLException {
        if (resultSetConcurrency == ResultSet.CONCUR_UPDATABLE) {
            throw new BQSQLException(
                    "The Resultset Concurrency can't be ResultSet.CONCUR_UPDATABLE");
        }
        
        this.ProjectId = projectid;
        this.connection = bqConnection;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.PrecompiledSQL = querysql;
        
        int count = 0;
        for (int i = 0; i < querysql.length(); i++) {
            if (querysql.charAt(i) == '?') {
                count++;
            }
        }
        
        if (count != 0) {
            this.Parameters = new String[count];
        }
        
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public void addBatch() throws SQLException {
        throw new BQSQLException("Not implemented." + "addBatch()");
    }
    
    @Override
    public void clearParameters() throws SQLException {
        
        int count = 0;
        for (int i = 0; i < this.PrecompiledSQL.length(); i++) {
            if (this.PrecompiledSQL.charAt(i) == '?') {
                count++;
            }
        }
        
        if (count != 0) {
            this.Parameters = new String[count];
        }
        else {
            this.Parameters = null;
        }
        
        this.RunnableStatement = this.PrecompiledSQL;
    }
    
    /**
     * Reconstructs the sql string which gets queried when for example
     * executequery is called this function is called every time a paramater is
     * changed
     */
    private void constructsql() {
        String[] data = this.PrecompiledSQL.split("\\?");
        String newsql = data[0];
        
        int countofsema = 0;
        for (int i = 0; i < this.Parameters.length; i++) {
            // if parameter is not yet set
            if (this.Parameters[i] == null) {
                // if there's data after the parameter
                if (++countofsema < data.length) {
                    newsql += "?" + data[countofsema];
                    // parameter is at the end
                }
                else {
                    newsql += "?";
                }
            }
            else
                // there's data after the parameter
                if (++countofsema < data.length) {
                    newsql += this.Parameters[i] + data[countofsema];
                    // parameter is at the end
                }
                else {
                    newsql += this.Parameters[i];
                }
        }
        this.RunnableStatement = newsql;
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Executes the PreCompiledSQL statement on BigQuery (note: it returns only
     * 1 resultset). This function directly uses executeQuery() function
     * </p>
     */
    @Override
    public boolean execute() throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("This Statement is Closed");
        }
        this.resset = this.executeQuery();
        this.logger.info("Executing Query: " + this.RunnableStatement);
        if (this.resset != null) {
            return true;
        }
        else {
            return false;
        }
    }
    
    /** {@inheritDoc} */
    @Override
    public ResultSet executeQuery() throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("This Statement is Closed");
        }
        if (this.RunnableStatement == null) {
            throw new BQSQLException("Parameters are not set");
        }
        if (this.RunnableStatement.contains("?")) {
            throw new BQSQLException("Not all parameters set");
        }
        this.starttime = System.currentTimeMillis();
        Job referencedJob;
        try {
            // Gets the Job reference of the completed job with give Query
            referencedJob = BQSupportFuncts.startQuery(
                    this.connection.getBigquery(), this.ProjectId,
                    this.RunnableStatement);
            this.logger.info("Executing Query: " + this.RunnableStatement);
        }
        catch (IOException e) {
            throw new BQSQLException(e);
        }
        try {
            do {
                if (BQSupportFuncts.getQueryState(referencedJob,
                        this.connection.getBigquery(), this.ProjectId).equals(
                        "DONE")) {
                    return new BQResultSet(BQSupportFuncts.GetQueryResults(
                            this.connection.getBigquery(), this.ProjectId,
                            referencedJob), this);
                }
                // Pause execution for half second before polling job status
                // again, to
                // reduce unnecessary calls to the BigQUery API and lower
                // overall
                // application bandwidth.
                Thread.sleep(500);
                this.logger.debug("slept for 500" + "ms, querytimeout is: "
                        + this.querytimeout + "s");
            }
            while (System.currentTimeMillis() - this.starttime <= (long) this.querytimeout * 1000);
            // it runs for a minimum of 1 time
        }
        catch (IOException e) {
            throw new BQSQLException(e);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        // here we should kill/stop the running job, but bigquery doesn't
        // support that :(
        throw new BQSQLException(
                "Query run took more than the specified timeout");
    }
    
    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     * 
     * @throws BQSQLException
     */
    @Override
    public int executeUpdate() throws SQLException {
        throw new BQSQLFeatureNotSupportedException("executeUpdate()");
    }
    
    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        // TODO IMPLEMENT
        return null;
    }
    
    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new BQSQLException(new SQLFeatureNotSupportedException());
        
    }
    
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x)
            throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("The Statement is Closed");
        }
        if (this.Parameters == null) {
            throw new BQSQLException("Index is not valid");
        }
        // The First Parameter is 1
        if (parameterIndex < 1 || parameterIndex > this.Parameters.length) {
            throw new BQSQLException("Index is not valid");
        }
        else {
            StringWriter writer = new StringWriter();
            
            try {
                org.apache.commons.io.IOUtils.copy(x, writer, "UTF-8");
            }
            catch (IOException e) {
                throw new BQSQLException(e);
            }
            String theString = writer.toString();
            this.SetParameter(parameterIndex, "\"" + theString + "\"");
        }
        
    }
    
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length)
            throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("The Statement is Closed");
        }
        if (this.Parameters == null) {
            throw new BQSQLException("Index is not valid");
        }
        // The First Parameter is 1
        if (parameterIndex < 1 || parameterIndex > this.Parameters.length) {
            throw new BQSQLException("Index is not valid");
        }
        else {
            StringWriter writer = new StringWriter();
            
            try {
                org.apache.commons.io.IOUtils.copy(x, writer, "UTF-8");
            }
            catch (IOException e) {
                throw new BQSQLException(e);
            }
            String theString = writer.toString();
            this.SetParameter(parameterIndex, "\"" + theString + "\"");
        }
        
    }
    
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length)
            throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("The Statement is Closed");
        }
        if (this.Parameters == null) {
            throw new BQSQLException("Index is not valid");
        }
        // The First Parameter is 1
        if (parameterIndex < 1 || parameterIndex > this.Parameters.length) {
            throw new BQSQLException("Index is not valid");
        }
        else {
            StringWriter writer = new StringWriter();
            
            try {
                org.apache.commons.io.IOUtils.copy(x, writer, "UTF-8");
            }
            catch (IOException e) {
                throw new BQSQLException(e);
            }
            String theString = writer.toString();
            this.SetParameter(parameterIndex, "\"" + theString + "\"");
        }
        
    }
    
    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x)
            throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("The Statement is Closed");
        }
        if (this.Parameters == null) {
            throw new BQSQLException("Index is not valid");
        }
        // The First Parameter is 1
        if (parameterIndex < 1 || parameterIndex > this.Parameters.length) {
            throw new BQSQLException("Index is not valid");
        }
        else {
            this.SetParameter(parameterIndex, x.toString());
        }
        
    }
    
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
        
    }
    
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
        
    }
    
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
        
    }
    
    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
        
    }
    
    @Override
    public void setBlob(int parameterIndex, InputStream inputStream)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
        
    }
    
    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length)
            throws SQLException {
        throw new BQSQLFeatureNotSupportedException();
        
    }
    
    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("The Statement is Closed");
        }
        if (this.Parameters == null) {
            throw new BQSQLException("Index is not valid");
        }
        // The First Parameter is 1
        if (parameterIndex < 1 || parameterIndex > this.Parameters.length) {
            throw new BQSQLException("Index is not valid");
        }
        else {
            this.SetParameter(parameterIndex, Boolean.toString(x));
        }
        
    }
    
    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("The Statement is Closed");
        }
        if (this.Parameters == null) {
            throw new BQSQLException("Index is not valid");
        }
        // The First Parameter is 1
        if (parameterIndex < 1 || parameterIndex > this.Parameters.length) {
            throw new BQSQLException("Index is not valid");
        }
        else {
            this.SetParameter(parameterIndex, Integer.toString(x & 0xff));
        }
    }
    
    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw new BQSQLException(new SQLFeatureNotSupportedException());
        
    }
    
    @Override
    public void setCharacterStream(int parameterIndex, Reader reader)
            throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("The Statement is Closed");
        }
        if (this.Parameters == null) {
            throw new BQSQLException("Index is not valid");
        }
        // The First Parameter is 1
        if (parameterIndex < 1 || parameterIndex > this.Parameters.length) {
            throw new BQSQLException("Index is not valid");
        }
        else {
            char[] arr = new char[8 * 1024]; // 8K at a time
            StringBuffer buf = new StringBuffer();
            int numChars;
            
            try {
                while ((numChars = reader.read(arr, 0, arr.length)) > 0) {
                    buf.append(arr, 0, numChars);
                }
            }
            catch (IOException e) {
                throw new BQSQLException(e);
            }
            this.SetParameter(parameterIndex, "\"" + buf.toString() + "\"");
        }
    }
    
    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length)
            throws SQLException {
        this.setCharacterStream(parameterIndex, reader);
    }
    
    @Override
    public void setCharacterStream(int parameterIndex, Reader reader,
            long length) throws SQLException {
        this.setCharacterStream(parameterIndex, reader);
    }
    
    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new BQSQLException(new SQLFeatureNotSupportedException());
        
    }
    
    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new BQSQLException(new SQLFeatureNotSupportedException());
        
    }
    
    @Override
    public void setClob(int parameterIndex, Reader reader, long length)
            throws SQLException {
        throw new BQSQLException(new SQLFeatureNotSupportedException());
        
    }
    
    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("The Statement is Closed");
        }
        if (this.Parameters == null) {
            throw new BQSQLException("Index is not valid");
        }
        // The First Parameter is 1
        if (parameterIndex < 1 || parameterIndex > this.Parameters.length) {
            throw new BQSQLException("Index is not valid");
        }
        else {
            Date s = new Date(x.getTime()
                    + Calendar.getInstance().getTimeZone().getRawOffset());
            this.SetParameter(parameterIndex, "\"" + s.toString() + "\"");
        }
        
    }
    
    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal)
            throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("The Statement is Closed");
        }
        if (this.Parameters == null) {
            throw new BQSQLException("Index is not valid");
        }
        // The First Parameter is 1
        if (parameterIndex < 1 || parameterIndex > this.Parameters.length) {
            throw new BQSQLException("Index is not valid");
        }
        else {
            Date s = new Date(x.getTime()
                    + ((cal == null) ? Calendar.getInstance().getTimeZone()
                            .getRawOffset() : cal.getTimeZone().getRawOffset()));
            this.SetParameter(parameterIndex, "\"" + s.toString() + "\"");
        }
        
    }
    
    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("The Statement is Closed");
        }
        if (this.Parameters == null) {
            throw new BQSQLException("Index is not valid");
        }
        // The First Parameter is 1
        if (parameterIndex < 1 || parameterIndex > this.Parameters.length) {
            throw new BQSQLException("Index is not valid");
        }
        else {
            this.SetParameter(parameterIndex, Double.toString(x));
        }
    }
    
    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("The Statement is Closed");
        }
        if (this.Parameters == null) {
            throw new BQSQLException("Index is not valid");
        }
        // The First Parameter is 1
        if (parameterIndex < 1 || parameterIndex > this.Parameters.length) {
            throw new BQSQLException("Index is not valid");
        }
        else {
            this.SetParameter(parameterIndex, Float.toString(x));
        }
        
    }
    
    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("The Statement is Closed");
        }
        if (this.Parameters == null) {
            throw new BQSQLException("Index is not valid");
        }
        // The First Parameter is 1
        if (parameterIndex < 1 || parameterIndex > this.Parameters.length) {
            throw new BQSQLException("Index is not valid");
        }
        else {
            this.SetParameter(parameterIndex, Integer.toString(x));
        }
        
    }
    
    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("The Statement is Closed");
        }
        if (this.Parameters == null) {
            throw new BQSQLException("Index is not valid");
        }
        // The First Parameter is 1
        if (parameterIndex < 1 || parameterIndex > this.Parameters.length) {
            throw new BQSQLException("Index is not valid");
        }
        else {
            this.SetParameter(parameterIndex, "\"" + Long.toString(x) + "\"");
        }
        
    }
    
    @Override
    public void setNCharacterStream(int parameterIndex, Reader reader)
            throws SQLException {
        this.setCharacterStream(parameterIndex, reader);
    }
    
    @Override
    public void setNCharacterStream(int parameterIndex, Reader reader,
            long length) throws SQLException {
        this.setCharacterStream(parameterIndex, reader);
    }
    
    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new BQSQLException(new SQLFeatureNotSupportedException());
        
    }
    
    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new BQSQLException(new SQLFeatureNotSupportedException());
        
    }
    
    @Override
    public void setNClob(int parameterIndex, Reader reader, long length)
            throws SQLException {
        throw new BQSQLException(new SQLFeatureNotSupportedException());
        
    }
    
    @Override
    public void setNString(int parameterIndex, String value)
            throws SQLException {
        this.setString(parameterIndex, value);
    }
    
    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("The Statement is Closed");
        }
        if (this.Parameters == null) {
            throw new BQSQLException("Index is not valid");
        }
        // The First Parameter is 1
        if (parameterIndex < 1 || parameterIndex > this.Parameters.length) {
            throw new BQSQLException("Index is not valid");
        }
        else {
            this.SetParameter(parameterIndex, "NULL");
        }
        
    }
    
    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName)
            throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("The Statement is Closed");
        }
        if (this.Parameters == null) {
            throw new BQSQLException("Index is not valid");
        }
        // The First Parameter is 1
        if (parameterIndex < 1 || parameterIndex > this.Parameters.length) {
            throw new BQSQLException("Index is not valid");
        }
        else {
            this.SetParameter(parameterIndex, "NULL");
        }
        
    }
    
    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("The Statement is Closed");
        }
        if (this.Parameters == null) {
            throw new BQSQLException("Index is not valid");
        }
        // The First Parameter is 1
        if (parameterIndex < 1 || parameterIndex > this.Parameters.length) {
            throw new BQSQLException("Index is not valid");
        }
        else
            // TODO Test and implicitly expand
            if (x.getClass().equals(Integer.class)) {
                this.setInt(parameterIndex, Integer.class.cast(x));
            }
            else
                if (x.getClass().equals(String.class)) {
                    this.setString(parameterIndex, String.class.cast(x));
                }
                else
                    if (x.getClass().equals(Reader.class)) {
                        this.setCharacterStream(parameterIndex,
                                Reader.class.cast(x));
                    }
                    else
                        if (x.getClass().equals(Long.class)) {
                            this.setLong(parameterIndex, Long.class.cast(x));
                        }
                        else
                            if (x.getClass().equals(Float.class)) {
                                this.setFloat(parameterIndex,
                                        Float.class.cast(x));
                            }
                            else
                                if (x.getClass().equals(Double.class)) {
                                    this.setDouble(parameterIndex,
                                            Double.class.cast(x));
                                }
                                else
                                    if (x.getClass().equals(Date.class)) {
                                        this.setDate(parameterIndex,
                                                Date.class.cast(x));
                                    }
                                    else
                                        if (x.getClass().equals(Time.class)) {
                                            this.setTime(parameterIndex,
                                                    Time.class.cast(x));
                                        }
                                        else
                                            if (x.getClass().equals(
                                                    Timestamp.class)) {
                                                this.setTimestamp(
                                                        parameterIndex,
                                                        Timestamp.class.cast(x));
                                            }
                                            else
                                                if (x.getClass().equals(
                                                        Byte.class)) {
                                                    this.setByte(
                                                            parameterIndex,
                                                            Byte.class.cast(x));
                                                }
                                                else
                                                    if (x.getClass().equals(
                                                            BigDecimal.class)) {
                                                        this.setBigDecimal(
                                                                parameterIndex,
                                                                BigDecimal.class
                                                                        .cast(x));
                                                    }
                                                    else
                                                        if (x.getClass()
                                                                .equals(RowId.class)) {
                                                            this.setRowId(
                                                                    parameterIndex,
                                                                    RowId.class
                                                                            .cast(x));
                                                        }
                                                        else
                                                            if (x.getClass()
                                                                    .equals(Short.class)) {
                                                                this.setShort(
                                                                        parameterIndex,
                                                                        Short.class
                                                                                .cast(x));
                                                            }
                                                            else
                                                                if (x.getClass()
                                                                        .equals(SQLXML.class)) {
                                                                    this.setSQLXML(
                                                                            parameterIndex,
                                                                            SQLXML.class
                                                                                    .cast(x));
                                                                }
                                                                else
                                                                    if (x.getClass()
                                                                            .equals(URL.class)) {
                                                                        this.setURL(
                                                                                parameterIndex,
                                                                                URL.class
                                                                                        .cast(x));
                                                                    }
                                                                    else
                                                                        if (x.getClass()
                                                                                .equals(Boolean.class)) {
                                                                            this.setBoolean(
                                                                                    parameterIndex,
                                                                                    Boolean.class
                                                                                            .cast(x));
                                                                        }
                                                                        else
                                                                            if (x.getClass()
                                                                                    .equals(InputStream.class)) {
                                                                                this.setAsciiStream(
                                                                                        parameterIndex,
                                                                                        InputStream.class
                                                                                                .cast(x));
                                                                            }
                                                                            else {
                                                                                throw new BQSQLException(
                                                                                        new SQLFeatureNotSupportedException());
                                                                            }
        
    }
    
    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType)
            throws SQLException {
        // setObject(parameterIndex, x);
        // TODO Implement
        throw new BQSQLException(new SQLFeatureNotSupportedException());
        
    }
    
    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType,
            int scaleOrLength) throws SQLException {
        // setObject(parameterIndex, x);
        // TODO Implement
        throw new BQSQLException(new SQLFeatureNotSupportedException());
    }
    
    /**
     * Sets the given paramter for the prepared statement (its a string value to
     * change the ? for)
     */
    private void SetParameter(int paramterNumber, String value) {
        this.Parameters[paramterNumber - 1] = value;
        this.constructsql();
    }
    
    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new BQSQLException(new SQLFeatureNotSupportedException());
    }
    
    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("The Statement is Closed");
        }
        if (this.Parameters == null) {
            throw new BQSQLException("Index is not valid");
        }
        // The First Parameter is 1
        if (parameterIndex < 1 || parameterIndex > this.Parameters.length) {
            throw new BQSQLException("Index is not valid");
        }
        else {
            this.SetParameter(parameterIndex, "\"" + x.toString() + "\"");
        }
        
    }
    
    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        int myint = new Integer(x);
        this.setInt(parameterIndex, myint);
    }
    
    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject)
            throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("The Statement is Closed");
        }
        if (this.Parameters == null) {
            throw new BQSQLException("Index is not valid");
        }
        // The First Parameter is 1
        if (parameterIndex < 1 || parameterIndex > this.Parameters.length) {
            throw new BQSQLException("Index is not valid");
        }
        else {
            this.SetParameter(parameterIndex, "\"" + xmlObject.getString()
                    + "\"");
            // TODO Check conversion
        }
        
    }
    
    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("The Statement is Closed");
        }
        if (this.Parameters == null) {
            throw new BQSQLException("Index is not valid");
        }
        // The First Parameter is 1
        if (parameterIndex < 1 || parameterIndex > this.Parameters.length) {
            throw new BQSQLException("Index is not valid");
        }
        else {
            this.SetParameter(parameterIndex, "\"" + x + "\"");
        }
    }
    
    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("The Statement is Closed");
        }
        if (this.Parameters == null) {
            throw new BQSQLException("Index is not valid");
        }
        // The First Parameter is 1
        if (parameterIndex < 1 || parameterIndex > this.Parameters.length) {
            throw new BQSQLException("Index is not valid");
        }
        else {
            Time s = new Time(x.getTime()
                    + Calendar.getInstance().getTimeZone().getRawOffset());
            this.SetParameter(parameterIndex, "\"" + s.toString() + "\"");
        }
        
    }
    
    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal)
            throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("The Statement is Closed");
        }
        if (this.Parameters == null) {
            throw new BQSQLException("Index is not valid");
        }
        // The First Parameter is 1
        if (parameterIndex < 1 || parameterIndex > this.Parameters.length) {
            throw new BQSQLException("Index is not valid");
        }
        else {
            Time s = new Time(x.getTime()
                    + ((cal == null) ? Calendar.getInstance().getTimeZone()
                            .getRawOffset() : cal.getTimeZone().getRawOffset()));
            this.SetParameter(parameterIndex, "\"" + s.toString() + "\"");
        }
        
    }
    
    @Override
    public void setTimestamp(int parameterIndex, Timestamp x)
            throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("The Statement is Closed");
        }
        if (this.Parameters == null) {
            throw new BQSQLException("Index is not valid");
        }
        // The First Parameter is 1
        if (parameterIndex < 1 || parameterIndex > this.Parameters.length) {
            throw new BQSQLException("Index is not valid");
        }
        else {
            Timestamp s = new Timestamp(x.getTime()
                    + Calendar.getInstance().getTimeZone().getRawOffset());
            this.SetParameter(parameterIndex, "\"" + s.toString() + "\"");
        }
        
    }
    
    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
            throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("The Statement is Closed");
        }
        if (this.Parameters == null) {
            throw new BQSQLException("Index is not valid");
        }
        // The First Parameter is 1
        if (parameterIndex < 1 || parameterIndex > this.Parameters.length) {
            throw new BQSQLException("Index is not valid");
        }
        else {
            Timestamp s = new Timestamp(x.getTime()
                    + ((cal == null) ? Calendar.getInstance().getTimeZone()
                            .getRawOffset() : cal.getTimeZone().getRawOffset()));
            this.SetParameter(parameterIndex, "\"" + s.toString() + "\"");
        }
        
    }
    
    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length)
            throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("The Statement is Closed");
        }
        if (this.Parameters == null) {
            throw new BQSQLException("Index is not valid");
        }
        // The First Parameter is 1
        if (parameterIndex < 1 || parameterIndex > this.Parameters.length) {
            throw new BQSQLException("Index is not valid");
        }
        else {
            StringWriter writer = new StringWriter();
            
            try {
                org.apache.commons.io.IOUtils.copy(x, writer, "UTF-8");
            }
            catch (IOException e) {
                throw new BQSQLException(e);
            }
            String theString = writer.toString();
            this.SetParameter(parameterIndex, "\"" + theString + "\"");
        }
        
    }
    
    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("The Statement is Closed");
        }
        if (this.Parameters == null) {
            throw new BQSQLException("Index is not valid");
        }
        // The First Parameter is 1
        if (parameterIndex < 1 || parameterIndex > this.Parameters.length) {
            throw new BQSQLException("Index is not valid");
        }
        else {
            this.SetParameter(parameterIndex, "\"" + x.toString() + "\"");
        }
        
    }
}
