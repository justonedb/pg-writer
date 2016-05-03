# pg-writer

## Description

Streaming writer for PostgreSQL tables

## Overview

A writer that implements the apendable, flushable and closeable interfaces for appending rows to a PostgreSQL table.

Columns are appended to a row in the order that columns are declared when the writer is constructed. When a row
is full a new row is implicitly started. Each column value is appended as a string representation using conventional PostgreSQL 
number, datetime and array string formats. 

The append methods append to the current column value and the next() method is used to advance to the next column 
or row. Convenience methods are provided for returning the current column number in a row and an indication
of whether the current column is the last column in the row.

Rows are buffered and are written to the database either when the flush() method is called or when
the buffer becomes full. Only complete rows are written to the table and each write operation
commits the rows that have been written.

## Code Example

    TableWriter writer=new TableWriter("localhost", //host name
                                       "myDB", //database name
                                       "me", //user to connect with
                                       "pwd", //password to authenticate with
                                       "myTable", //name of table to write to 
                                       new String[]{"id","datetime"}, //name of columns to write to
                                       64*1024); //buffer size
             
    writer.append("1"); //append "1" to the first column in the first row
    writer.next(); //advance to the next column
    writer.append(new java.util.Date().toString()); //append current date time to the second column 
    writer.next(); //advance to the next row
    writer.flush(); //flush the row to the table
    writer.close(); //close the writer
 
