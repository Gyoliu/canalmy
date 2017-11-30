package com.example.demo.util;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;

import java.sql.*;
import java.util.*;

/**
 * Created by liuxing on 2017/11/30.
 * https://prestodb.io/docs/current/installation/jdbc.html
 * jdbc路径支持：
 * jdbc:presto://host:port
 * jdbc:presto://host:port/catalog
 * jdbc:presto://host:port/catalog/schema
 */
public class PrestoJdbcUtil {

    private static final String URL = "jdbc:presto://192.168.20.7:9999/kafka/test";

    public static Connection getConnection(){
        try{
            Class.forName("com.facebook.presto.jdbc.PrestoDriver");
            Connection connection = DriverManager.getConnection(URL,"test" ,"");
            System.out.println(connection.isClosed());
            return connection;
        }catch (Exception ex){
            ex.printStackTrace();
        }
        return null;
    }

    public static Object queryPresto(String sql, Class cls){
        Connection connection = null;
        try{
            connection = PrestoJdbcUtil.getConnection();
            QueryRunner queryRunner = new QueryRunner();
            Object query = queryRunner.query(connection, sql, new BeanListHandler(cls));
            return query;
        }catch (Exception ex){
            ex.printStackTrace();
        }finally {
            if(connection != null){
                try{connection.close();}catch (Exception ex){}
            }
        }
        return null;
    }

    public static List<Map<String, Object>> queryPresto(String sql){
        List<Map<String, Object>> list = new ArrayList<>();
        Connection connection = null;
        try{
            connection = PrestoJdbcUtil.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()){
                ResultSetMetaData metaData = resultSet.getMetaData();
                HashMap<String, Object> map = new LinkedHashMap<>();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    map.put(metaData.getColumnName(i), resultSet.getObject(i));
                }
                list.add(map);
            }
            return list;
        }catch (Exception ex){
            ex.printStackTrace();
        }finally {
            if(connection != null){
                try{connection.close();}catch (Exception ex){}
            }
        }
        return null;
    }


    public static void main(String[] args){
        String sql = "select count(*) cnt from sku ";
        //PrestoJdbcUtil.queryPresto(sql, Integer.class);
        List<Map<String, Object>> maps = PrestoJdbcUtil.queryPresto(sql);
        for (Map<String, Object> map : maps){
            for (Map.Entry entry : map.entrySet()){
                System.out.println(entry.getKey() + ":" + entry.getValue());
            }
        }
    }

}
