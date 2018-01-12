package com.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;

/*

Prepare the mysql database
1. create database
create database iot;
2. Create a table inside iot database
create table tags(tag varchar(255) primary key not null, count bigint not null);

Add mysql jdbc as maven dependency to the project.

*/

public class CustomForEachSink extends ForeachWriter<Row> {
	
	private static final long serialVersionUID = 1L;
	private Connection conn = null; 
	private PreparedStatement statement = null;
	

    @Override
    public boolean open(long partitionId, long version) {
    	try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/iot","root","cloudera");
			String sql = "insert into tags (tag, count) value (?, ?) ON DUPLICATE KEY UPDATE count=?";
			statement = conn.prepareStatement(sql);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return true;
    }

    @Override
    public void process(Row row) {
    	String tag = row.getString(0);
    	Long count = row.getLong(1);
    	try {
			statement.setString(1, tag);
			statement.setLong(2, count);
			statement.setLong(3, count);
			statement.execute();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	System.out.println(tag + ":" + count);
    }

    @Override
    public void close(Throwable errorOrNull) {
    	try {
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
  }
