/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package booksample;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
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
    
    public void deleteRecord (String primaryKey, String columnName, String recordValue) throws SQLException {
        String sql = "DELETE FROM " +  primaryKey + " WHERE " + columnName + " = " + recordValue;
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(sql);
    }
    
//    public void createRecord (String tableName, List columnNames, List values) {
////        String sql = "INSERT INTO " + tableName + " ("columnNames.toString() ")" + "VALUES " + (values);
////    }
    
    public static void main(String[] args) throws Exception {
        MySqlDb db = new MySqlDb();
        db.openConnection("com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost:3306/book2", "root", "admin");
        
        db.deleteRecord("author", "author_id", "1");
        
        List<Map<String,Object>> records = db.findAllRecords("author");
        
        for(Map record : records) {
            System.out.println(record);
        }
        
        db.closeConnection();
    }
}
