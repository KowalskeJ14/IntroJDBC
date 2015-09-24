/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package booksample;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 * @author kowal_000
 */
public class MySqlDb {
    
    private Connection conn;
    
    public void openConnection(String driverClass, String url,
            String userName, String password) throws Exception {
   
        Class.forName (driverClass);
	conn = DriverManager.getConnection(url, userName, password);
    }
    
    public void closeConnection() throws SQLException {
        conn.close();
    }
    
    public List<Map<String, Object>> findAllRecords(String tableName) throws SQLException {
        List<Map<String,Object>> records = new ArrayList<>();
        
        // Select * FROM Author
        String sql = "SELECT * FROM " + tableName;
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        
        while(rs.next()) {
            Map<String,Object> record = new HashMap<>();
            for(int i=1; i<=columnCount; i++) {
                record.put(metaData.getColumnName(i), rs.getObject(i));
            }
            records.add(record);
        }
        
        return records;
    }
    
    public void deleteByPrimaryKey (String tableName, String primaryKeyField, Object recordValue) throws SQLException {
        String sql = "DELETE FROM " +  tableName + " WHERE " + primaryKeyField + " = ";
        if (recordValue instanceof String) {
            sql += "'" + recordValue.toString() + "'";
        }
        else {
            sql += recordValue.toString();
        }
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(sql);
    }
    
    public void psDeleteByPrimaryKey (String tableName, String primaryKeyValue, Object recordValue) throws SQLException {
        String sql = "DELETE FROM " +  tableName + " WHERE " + primaryKeyValue + " = ?";
        
        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setObject(1, recordValue);
        stmt.executeUpdate();
    }
    
    public void insertRecordPreparedStatement (String tableName, List newRecordColumns, List newRecordValues) throws SQLException {
        Object[] recordCols = newRecordColumns.toArray();
        Object[] recordVals = newRecordValues.toArray();
        
        String recordColString = recordCols[0].toString();
        String recordValString = "?";
        
        for(int i = 1; i < recordCols.length; i++) {
            recordColString += ", " + recordCols[i].toString();
            recordValString += ", ?";
        }
        
        String sql = "INSERT INTO " + tableName + " ( " + recordColString + ") VALUES (" + recordValString + ")";
        PreparedStatement pstmt = conn.prepareStatement(sql);
        
        for(int i = 0; i<recordCols.length; i++) {
            pstmt.setObject(i+1, recordVals[i]);
        }
        pstmt.executeUpdate();
    }
    
    public void updateRecordByPrimaryKeyPreparedStatement(String tableName, String primaryKey, Object primaryKeyValue, List updateRecordColumns, List updateRecordValues) throws SQLException {
        Object[] recordKeyArray = updateRecordColumns.toArray();
        Object[] recordObjectArray = updateRecordValues.toArray();
        
        String recordString = recordKeyArray[0].toString() + "= ?";
        for(int i = 1; i<recordKeyArray.length;i++) {
            recordString += "," + recordKeyArray[i].toString() + "= ?";
        }
        
        String sql = "UPDATE " + tableName + " SET " + recordString + " WHERE " + primaryKey + " = ?";
        
        PreparedStatement pstmt = conn.prepareStatement(sql);
        for(int i = 0; i<recordKeyArray.length; i++) {
            pstmt.setObject(i+1, recordObjectArray[i]);
        }
        pstmt.setObject(recordKeyArray.length+1, primaryKeyValue);
        pstmt.executeUpdate();
    }
    
    public static void main(String[] args) throws Exception {
        MySqlDb db = new MySqlDb();
        db.openConnection("com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost:3306/book2", "root", "admin");
        
//        db.deleteByPrimaryKey("author", "author_id", "1");
//        db.psDeleteByPrimaryKey(null, null, db);
        
//        List newRecordCols = new ArrayList();
//        List newRecordVals = new ArrayList();
//        
//        newRecordCols.add("author_name");
//        newRecordCols.add("date_created");
//        
//        newRecordVals.add("Stephan King");
//        newRecordVals.add("1999-02-05");
//        
//        db.insertRecordPreparedStatement("author", newRecordCols, newRecordVals);
        
        List updateRecordCols = new ArrayList();
        List updateRecordVals = new ArrayList();
                
        updateRecordCols.add("author_name");
        updateRecordCols.add("date_created");
        
        updateRecordVals.add("Harper Lee");
        updateRecordVals.add("2000-03-12");
        
        db.updateRecordByPrimaryKeyPreparedStatement("author", "author_id", "3", updateRecordCols, updateRecordVals);
        
        List<Map<String,Object>> records = db.findAllRecords("author");
        
        for(Map record : records) {
            System.out.println(record);
        }
        
        db.closeConnection();
    }
}
