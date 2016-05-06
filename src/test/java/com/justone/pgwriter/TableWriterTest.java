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

package com.justone.pgwriter;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.TestCase;

/**
 * Test for TableWriter
 * @author Duncan Pauly
 * @version 1.0
 */
public class TableWriterTest extends TestCase {

  /**
   * Host name for server hosting the database
   */
  private final static String HOST="localhost:5432";
  /**
   * Name of the database to connect to
   */
  private final static String DATABASE="postgres";
  /**
   * Database user to connect with
   */
  private final static String USER="postgres";
  /**
   * Password for database user authentication
   */
  private final static String PASSWORD="postgres";
  
  /**
   * Database connection used for database operations
   */
  private static Connection connection;
  /**
   * Statement used for database queries
   */
  private static Statement statement;
  /**
   * Writer instance used for testing
   */
  private static TableWriter writer;
  
  
  public TableWriterTest(String testName) {
    super(testName);
  }
      
  @Override
  protected void setUp() throws Exception {
    
    super.setUp();
    
        /* load driver */
    Class.forName("org.postgresql.Driver");//attempt to load database driver
 
    /* open database connection */
    connection = DriverManager.getConnection("jdbc:postgresql://"+HOST+"/"+DATABASE, USER, PASSWORD);//open connection to the database
    
    /* create table for tests */
    statement=connection.createStatement();
    statement.executeUpdate("CREATE TABLE IF NOT EXISTS pgwriter (A VARCHAR, B VARCHAR)");
         
    /* truncate the table */
    statement.executeUpdate("TRUNCATE TABLE pgwriter");
 
    /* create writer for test */
    writer=new TableWriter(HOST, //host name
                           DATABASE, //database name
                           USER, //user to connect with
                           PASSWORD, //password to authenticate with
                           "pgwriter", //name of table to write to 
                           new String[]{"A","B"}, //name of columns to write to
                           64*1024); //buffer size
 
        
  }//setUp
  
  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    
    /* close writer */
    writer.close();
    
    /* drop table */
    statement.executeUpdate("DROP TABLE IF EXISTS pgwriter");
    
    /* close database connection */
    connection.close();

    
  }//tearDown

  /**
   * Test of append and next methods, of class TableWriter.
   * @throws java.io.IOException if test fails
   * @throws java.sql.SQLException if test fails
   */
  public void testAppendAndNext() throws IOException, SQLException {
    
    System.out.println("append and next");
    
    /* append row with nulls */
    int column=writer.next();
    assertEquals(1,column);
    column=writer.next();
    assertEquals(0,column);
    
    /* append row with aa,bb */
    writer.append('a');
    writer.append('a');
    writer.next();
    writer.append('b');
    writer.append('b');
    writer.next();
    
    /* append row with aaAA,bbBB */
    writer.append("aa");
    writer.append("AA");
    writer.next();
    writer.append("bb");
    writer.append("BB");
    writer.next();

    /* append row with aaAA,bbBB */
    writer.append("aaAAbbBB",0,2);
    writer.append("aaAAbbBB",2,4);
    writer.next();
    writer.append("aaAAbbBB",4,6);
    writer.append("aaAAbbBB",6,8);
    writer.next();
    writer.flush();
    
    /* check rows in the table */
    ResultSet resultSet = statement.executeQuery("SELECT A,B FROM pgwriter");

    /* check first row */
    boolean found=resultSet.next();
    assertEquals(true, found);
    String a=resultSet.getString(1);
    String b=resultSet.getString(2);
    assertNull(a);
    assertNull(b);
       
    /* check second row */
    found=resultSet.next();
    assertEquals(true, found);
    a=resultSet.getString(1);
    b=resultSet.getString(2);
    assertEquals("aa", a);
    assertEquals("bb", b);

    /* check third row */
    found=resultSet.next();
    assertEquals(true, found);
    a=resultSet.getString(1);
    b=resultSet.getString(2);
    assertEquals("aaAA", a);
    assertEquals("bbBB", b);
    
    /* check fourth row */
    found=resultSet.next();
    assertEquals(true, found);
    a=resultSet.getString(1);
    b=resultSet.getString(2);
    assertEquals("aaAA", a);
    assertEquals("bbBB", b);
                
  }//testAppendAndNext()

  /**
   * Test of flush method, of class TableWriter.
   * @throws java.io.IOException if test fails
   * @throws java.sql.SQLException if test fails
   */
  public void testFlush() throws IOException, SQLException {
    
    System.out.println("flush");
    
    /* prepare row and flush before complete*/
    writer.append('a');
    writer.next();
    writer.append('b');
    writer.flush();
    
    /* check no rows in the table */
    ResultSet resultSet = statement.executeQuery("SELECT A,B FROM pgwriter");
    boolean found=resultSet.next();
    assertEquals(false, found);

    writer.next();
    
    /* check no rows in the table */
    resultSet = statement.executeQuery("SELECT A,B FROM pgwriter");
    found=resultSet.next();
    assertEquals(false, found);
    
    writer.flush();
    
    /* check row is in the table */
    resultSet = statement.executeQuery("SELECT A,B FROM pgwriter");
    found=resultSet.next();
    assertEquals(true, found);
        
    /* append one complete row and one partial row */
    writer.append('a');
    writer.next();
    writer.append('b');
    writer.next();
    writer.append('a');
    writer.next();
    
    writer.flush();

    /* check there are only two rows in the table */
    resultSet = statement.executeQuery("SELECT count(*) FROM pgwriter");
    resultSet.next();
    int count=resultSet.getInt(1);

    assertEquals(2, count);
    
  }//testFlush()
  
  /**
   * Test of isEmpty method, of class TableWriter.
   * @throws java.io.IOException if test fails
   * @throws java.sql.SQLException if test fails
   */
  public void testIsEmpty() throws IOException, SQLException {
    
    System.out.println("isEmpty");
    
    assertEquals(true, writer.isEmpty());
    
    /* append content to buffer */
    writer.append('a');
    assertEquals(false, writer.isEmpty());

    writer.next();
    writer.append('b');
    assertEquals(false, writer.isEmpty());

    writer.next();
    assertEquals(false, writer.isEmpty());
    
    /* flush and empty the buffer */
    writer.flush();
    assertEquals(true, writer.isEmpty());
        
  }//testIsEmpty()
  
  /**
   * Test of firstColumn method, of class TableWriter.
   * @throws java.io.IOException if test fails
   */
  public void testFirstColumn() throws IOException {
    
    System.out.println("firstColumn");
    
    /* at first column */
    assertEquals(true,writer.firstColumn());
    
    /* advance to last column */
    writer.next();
    assertEquals(false,writer.firstColumn());

    /* advance to first column of next row */
    writer.next();
    assertEquals(true,writer.firstColumn());
    
  }//testFirstColumn()  
  
  
  /**
   * Test of lastColumn method, of class TableWriter.
   * @throws java.io.IOException if test fails
   */
  public void testLastColumn() throws IOException {
    
    System.out.println("lastColumn");
    
    /* at first column */
    assertEquals(false,writer.lastColumn());
    
    /* advance to last column */
    writer.next();
    assertEquals(true,writer.lastColumn());

    /* advance to first column of next row */
    writer.next();
    assertEquals(false,writer.lastColumn());
    
  }//testLastColumn()

  /**
   * Test of getColumnNo method, of class TableWriter.
   * @throws java.io.IOException if test fails
   */
  public void testGetColumnNo() throws IOException {
    
    System.out.println("getColumnNo");
    
    /* at first column */
    assertEquals(0,writer.getColumnNo());
    
    /* advance to last column */
    writer.next();
    assertEquals(1,writer.getColumnNo());

    /* advance to first column of next row */
    writer.next();
    assertEquals(0,writer.getColumnNo());
  
  }//testGetColumnNo()

  /**
   * Test of getConnection method, of class TableWriter.
   * @throws java.sql.SQLException if test fails
   */
  public void testGetConnection() throws SQLException {
    
    System.out.println("getConnection");
    
    /* execute query via writer connection */
    ResultSet resultSet = writer.getConnection().createStatement().executeQuery("SELECT count(*) FROM pgwriter");
    boolean found=resultSet.next();
    assertEquals(true, found);

  }//testGetConnection() 
    
}//TableWriterTest()
