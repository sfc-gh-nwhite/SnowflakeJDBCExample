/*
 * Copyright (c) 2012-2019 Snowflake Inc. All rights reserved.
 *
 * - Download the latest version of the driver (snowflake-jdbc-<ver>.jar) from Maven:
 *       https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/<ver>
 * - Download this file (SnowflakeJDBCExample.java) into the same directory.
 * - Edit this file (SnowflakeJDBCExample.java) and set the connection properties correctly.
 * - From the command line, run:
 *     javac SnowflakeJDBCExample.java
 * - From the command line, run:
 *   - Linux/macOS:
 *     java -cp .:snowflake-jdbc-<ver>.jar SnowflakeJDBCExample
 *   - Windows:
 *     java -cp .;snowflake-jdbc-<ver>.jar SnowflakeJDBCExample
 *
 */
package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class SnowflakeJDBCExample {

  public static void main(String[] args) throws Exception {
    // get connection
    System.out.println("Create JDBC connection");
    Connection connection = getConnection();
    System.out.println("Done creating JDBC connection\n");

    // create statement
    System.out.println("Create JDBC statement");
    Statement statement = connection.createStatement();
    System.out.println("Done creating JDBC statement\n");

// Put a file

    String filePath = "/Users/nwhite/Downloads/SampleData/schema-evolution/*";
    filePath = "file://" + filePath; // Enclose the file path in double quotes

    String sql = "PUT " + filePath + " @my_stage overwrite=true"; // Example SQL command for putting a file
    System.out.println("SQL Statement for PUT: " +sql);

    try {
      Statement stmt = connection.createStatement();
      boolean result = stmt.execute(sql); // Execute the SQL command

      // Handle the result or perform additional operations
      System.out.println("File PUT operation result: " + result);

      stmt.close();
    } catch (SQLException e) {
      // Handle any SQL exceptions
      e.printStackTrace();
    }

    // create a table
    System.out.println("Create demo table");
    statement.executeUpdate("create or replace table demo(c1 string)");
    System.out.println("Done creating demo table\n");

    // insert a row
    System.out.println("Insert 'hello world'");
    statement.executeUpdate("insert into demo values ('hello world')");
    System.out.println("Done inserting 'hello world'\n");

    // query the data
    System.out.println("Query demo");
    ResultSet resultSet = statement.executeQuery("select * from demo");
    System.out.println("Metadata:");
    System.out.println("================================");

    // fetch metadata
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    System.out.println("Number of columns=" + resultSetMetaData.getColumnCount());
    for (int colIdx = 0; colIdx < resultSetMetaData.getColumnCount(); colIdx++) {
      System.out.println(
          "Column " + colIdx + ": type=" + resultSetMetaData.getColumnTypeName(colIdx + 1));
    }

    // fetch data
    System.out.println("\nData:");
    System.out.println("================================");
    int rowIdx = 0;
    while (resultSet.next()) {
      System.out.println("row " + rowIdx + ", column 0: " + resultSet.getString(1));
    }
    resultSet.close();
    statement.close();
    connection.close();
  }

  private static Connection getConnection() throws SQLException {

    // build connection properties
    Properties properties = new Properties();
    properties.put("user", System.getenv("SNOWFLAKE_USER")); // replace "" with your user name
    properties.put("password", System.getenv("SNOWFLAKE_PASSWORD")); // replace "" with your password
    properties.put("warehouse", System.getenv("SNOWFLAKE_WAREHOUSE")); // replace "" with target warehouse name
    properties.put("db", System.getenv("SNOWFLAKE_DATABASE")); // replace "" with target database name
    properties.put("schema", System.getenv("SNOWFLAKE_SCHEMA")); // replace "" with target schema name
    properties.put("authenticator", System.getenv("SNOWFLAKE_AUTHENTICATOR"));
    properties.put("tracing", "all"); // optional tracing property
    // Replace <account_identifier> with your account identifier. See
    // https://docs.snowflake.com/en/user-guide/admin-account-identifier.html
    // for details.
    String connectStr = "jdbc:snowflake://sfcsupport-nwhite_awsuseast2_1.us-east-2.aws.snowflakecomputing.com";
    return DriverManager.getConnection(connectStr, properties);
  }
}
