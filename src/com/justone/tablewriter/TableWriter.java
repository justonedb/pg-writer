/*

MIT License
 
Copyright (c) 2016 JustOne Database Inc

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

*/

package com.justone.tablewriter;

import java.io.Closeable;
import java.io.Flushable;
import java.io.StringReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.postgresql.core.BaseConnection;//PostgreSQL database connection
import org.postgresql.copy.CopyManager;//PostgreSQL copy manager

/**
 * <P>
 * A writer class for appending rows to a database table in a PostgreSQL or compatible database. 
 * </P>
 * <P>
 * A row is constructed by appending column values to it and when the row is full a new row is implicitly started. 
 * Each column value is appended as a string representation using conventional PostgreSQL number, datetime and array string formats. 
 * Column values are appended to a row in the column order specified when the writer is constructed. 
 * </P>
 * <P>
 * The append methods append to the current column value and the next() method is used to advance to the next column 
 * or row. Convenience methods are provided for returning the current column number in a row and an indication
 * of whether the current column is the first or last column in the row.
 * </P>
 * <P>
 * Rows are buffered and are written to the database either when the flush() method is called or when
 * the buffer becomes full. Only complete rows are written to the table and each write operation
 * commits the rows that have been written. Note that the last column in a row is only complete after next() has been
 * called to move to the next row.
 * </P>
 * <br>
 * Code example:
 * <pre><code>
 * 
 *   TableWriter writer=new TableWriter("localhost", //host name
 *                                      "myDB", //database name
 *                                      "me", //user to connect with
 *                                      "pwd", //password to authenticate with
 *                                      "myTable", //name of table to write to 
 *                                      new String[]{"id","datetime"}, //names and order of columns to write to
 *                                      64*1024); //buffer size
 *   
 *   writer.append("1"); //append "1" to the first column in the first row
 *   writer.next(); //advance to the next column
 *   writer.append(new java.util.Date().toString()); //append current date time to the second column 
 *   writer.next(); //advance to the next row
 *   writer.flush(); //flush the row to the table
 *   writer.close(); //close the writer
 * 
 * </code></pre>
 *    
 * @author Duncan Pauly
 * @version 1.0
 * 
 */
public class TableWriter implements Appendable, Flushable, Closeable {
  
  /**
   * Database driver name
   */
  private final static String DRIVER="org.postgresql.Driver";
  /**
   * JDBC driver prefix for database connections
   */
  private final static String JDBC_PREFIX="jdbc:postgresql://";
  /**
   * Default host name
   */
  private final static String DEFAULT_HOST="localhost:5432";
  /**
   * Column delimiter character (STX)
   */
  private final static char COLUMN_DELIMITER=2;
  /**
   * Row delimiter character (NL)
   */
  private final static char ROW_DELIMITER='\n';
  /**
   * Quote character (ETX)
   */
  private final static char QUOTE_CHARACTER=3;
  /**
   * Table name token in COPY command template
   */
  private final static String TABLE_TOKEN="<T>";
  /**
   * Column list token in COPY command template
   */
  private final static String COLUMNS_TOKEN="<C>";
  /**
   * COPY command template
   */
  private final static String COPY_TEMPLATE="COPY "+
                                            TABLE_TOKEN+" "+
                                            "("+COLUMNS_TOKEN+") "+
                                            "FROM STDIN CSV NULL '' "+
                                            "DELIMITER E'"+COLUMN_DELIMITER+"' "+
                                            "QUOTE E'"+QUOTE_CHARACTER+"' "+
                                            "ESCAPE E'\\\\' ";//template COPY command
  
  /**
   * Database connection
   */
  private final Connection fConnection;
  /**
   * COPY command for flushing data
   */
  private final String fCopyCommand;
  /**
   * COPY manager
   */
  private final CopyManager fCopyManager;
  /**
   * Column number of last column
   */
  private final int fLastColumnNo;
  /**
   * Write buffer capacity (bytes)
   */
  private final int fCapacity;
  /**
   * Write buffer
   */
  private final StringBuilder fBuffer;
  /**
   * Buffer watermark of last complete row for flushing
   */
  private int iWatermark;
  /**
   * Current column number in current row
   */
  private int iColumnNo;
  

  /**
   * Constructor for table writer
   * @param aHost host server string (optional)
   * @param aDatabase database name
   * @param aUsername connection username (optional)
   * @param aPassword connection password  (optional)
   * @param aTableName table to write to
   * @param aColumnNames columns to write to
   * @param aCapacity buffer capacity (bytes)
   * @throws IOException if database connection error
   */
  public TableWriter(String aHost, String aDatabase, String aUsername, String aPassword, String aTableName, String[] aColumnNames, int aCapacity) throws IOException {
    assert aDatabase!=null;
    assert aTableName!=null;
    assert aColumnNames!=null;
    assert aCapacity>=0;

    /* load driver */
    try {
      Class.forName(DRIVER);//attempt to load database driver
    } catch (ClassNotFoundException exception) {
      throw new RuntimeException("Driver not found ["+DRIVER+"]");//ruh roh
    }//try{}
    
    /* set default host if not specified */
    if ((aHost==null)||(aHost.length()==0)) {//if no host string specified
      aHost=DEFAULT_HOST;//use local host and default port
    }//if no host string specified
    
    /* convert column name array to comma delimited column string */
    StringBuilder columns=new StringBuilder();//buffer for string construction
    columns.append(aColumnNames[0]);//append first column name
    for (int i=1;i<aColumnNames.length;++i) {//for each subsequent column
      columns.append(',');//append column delimiter
      columns.append(aColumnNames[i]);//append column name
    }//for each subsequent column
    
    /* construct copy command */
    String copyCommand=COPY_TEMPLATE;//start with command template    
    copyCommand=copyCommand.replace(TABLE_TOKEN, aTableName);//replace table name
    copyCommand=copyCommand.replace(COLUMNS_TOKEN, columns.toString());//replace column list
    
    fCapacity=aCapacity;//set buffer capacity
    fBuffer=new StringBuilder(fCapacity);//create buffer    
    fLastColumnNo=aColumnNames.length-1;//set last column number
    iColumnNo=0;//initialise column number
    iWatermark=0;//initialise watermark
    fCopyCommand=copyCommand;//set copy command

    try {
      
      /* open database connection */
      String database=JDBC_PREFIX+aHost+"/"+aDatabase;//construct database connection string
      fConnection = DriverManager.getConnection(database, aUsername, aPassword);//open connection to the database
   
      /* set instance finals */
      fCopyManager=new CopyManager((BaseConnection) fConnection);//construct copy manager
      
    } catch (SQLException exception) {//catch SQL exception
      throw new IOException(exception.getMessage());//convert to IOException
    }//try{}
    
  }//TableWriter()
      
  /**
   * Appends a single character to the current column value
   * @param c character to append
   * @return this writer
   * 
   */
  @Override
  public Appendable append (char c) {
    assert c>0;
    
    fBuffer.append(c);//append character to the buffer
    
    return this;
    
  }//append()

  /**
   * Appends characters to the current column value
   * @param csq characters to append
   * @return this writer
   * */
  @Override
  public Appendable append (CharSequence csq) {
    assert csq!=null;
    
    fBuffer.append(csq);//append characters to the buffer

    return this;
    
  }//append()

  /**
   * Appends characters to the current column value
   * @param csq sequence containing characters to append
   * @param start index of the first character to append 
   * @param end index of the character following the last character to append 
   * @return this writer
   */
  @Override
  public Appendable append (CharSequence csq, int start, int end) {
    assert csq!=null;
    
    fBuffer.append(csq,start,end);//append characters to the buffer
    return this;
   
  }//append()
    
  /**
   * Move to the next column or row. If there is a column after the current column
   * in the same row then this will advance to the next column; otherwise it will advance to
   * the first column in the next row.
   * @return column number of the column advanced to. 0 indicates the first column of the next row.
   * @throws IOException if write error
   */
  public int next() throws IOException {
    
    /* advance to next column or row */
    if (iColumnNo==fLastColumnNo) {//if row is complete
      fBuffer.append(ROW_DELIMITER);//append row delimiter
      iWatermark=fBuffer.length();//set watermark at end of the buffer
      iColumnNo=0;//reset column number
    } else {//else row is incomplete
      fBuffer.append(COLUMN_DELIMITER);//append column delimiter
      ++iColumnNo;//increment column number
    }//if row is complete
    
    /* flush buffer if full */
    if (fBuffer.length()>fCapacity) {//if buffer size exceeds capacity
      flush();//flush buffer
    }//if buffer size exceeds capacity
    
    return iColumnNo;//return column number
          
  }//next()

  /**
   * Indicates if the current column is the first in the row
   * @return true if this is the first column in the row
   */
  public boolean firstColumn() {
    
    return (iColumnNo==0);//is the column number zero?
          
  }//firstColumn()  
  
  /**
   * Indicates if the current column is the last in the row
   * @return true if this is the last column in the row
   */
  public boolean lastColumn() {
    
    return (iColumnNo==fLastColumnNo);//is column number same as the last column?
          
  }//lastColumn()  

  /**
   * Get current column number
   * @return current column number. 0 indicates the first column in the row.
   */
  public int getColumnNo() {
    
    return iColumnNo;//return column number
          
  }//getColumnNo()  
    
  /**
   * Flush buffer content to the database
   * @throws IOException if write error
   */
  @Override
  public void flush () throws IOException {

    if (iWatermark>0) {//if complete rows pending
      try {
        fCopyManager.copyIn(fCopyCommand, new StringReader(fBuffer.substring(0,iWatermark).toString()));//copy complete rows to the database
        fBuffer.delete(0, iWatermark);//remove written rows from the buffer
        iWatermark=0;//reset watermark
      } catch (SQLException exception) {
        throw new IOException(exception.getMessage());
      }//try{} 
    }//if complete rows pending
    
  }//flush()

  /**
   * Indicates if the buffer is empty
   * @return true if the buffer is empty or false if not
   */
  public boolean isEmpty() {
    
    return (fBuffer.length()==0);//indicate if the buffer is empty
          
  }//isEmpty()  

  /**
   * Gets the database connection
   * @return database connection
   */
  public Connection getConnection () {
    
    return fConnection;//return database connection
    
  }//getConnection()

  /**
   * Close the table writer. Any complete rows in the buffer are flushed beforehand
   * and any partial row is discarded.
   * @throws IOException if write error
   */
  @Override
  public void close() throws IOException {
    
    flush();//flush buffer
    
    try {
      fConnection.close();//close database connection
    } catch (SQLException exception) {
      throw new IOException(exception.getMessage());
    }//try{}
    
  }//close()
   
}//TableWriter{}
