package com.kevin.java;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

/**
 * @author kevin
 * @version 1.0
 * @description
 * @createDate 2019/1/9
 */
public class HiveTest {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://Slave1:10000/default";
    private static String user = "root";
    private static String password = "Hadoop01!";
    private static String sql = "";
    private static boolean res1;
    private static ResultSet res;
    private static final Logger log = Logger.getLogger(HiveTest.class);

    public static void main(String[] args) {
        try {
            Class.forName(driverName); // 注册JDBC驱动

            //默认使用端口10000, 使用默认数据库，用户名密码默认
            Connection conn = DriverManager.getConnection(url, user, password);

            //    Statement用来执行SQL语句
            Statement stmt = conn.createStatement();

            // 创建的表名
            String tableName = "testHiveDriverTable";

            /** 第一步:存在就先删除 **/
            sql = "drop table " + tableName;
            stmt.execute(sql);

            /** 第二步:不存在就创建 **/
            sql = "create table " + tableName +
                    "(userid int , " +
                    "movieid int," +
                    "rating int," +
                    "city string," +
                    "viewTime string)" +
                    "row format delimited " +
                    "fields terminated by '\t' " +
                    "stored as textfile";

            // sql = "create table " + tableName + " (key int, value string) row format delimited fields terminated by '\t'";
            stmt.execute(sql);

            // 执行“show tables”操作
            sql = "show tables '" + tableName + "'";
            System.out.println("Running:" + sql);
            res = stmt.executeQuery(sql);
            System.out.println("执行“show tables”运行结果:");
            if (res.next()) {
                System.out.println(res.getString(1));
            }

            // 执行“describe table”操作
            sql = "describe " + tableName;
            System.out.println("Running:" + sql);
            res = stmt.executeQuery(sql);
            System.out.println("执行“describe table”运行结果:");
            while (res.next()) {
                System.out.println(res.getString(1) + "\t" + res.getString(2));
            }

            // 执行“load data into table”操作
            String filepath = "/input/data/test2_hive.txt"; //因为是load data  inpath，所以是HDFS路径
            sql = "load data inpath '" + filepath + "' into table " + tableName;

            System.out.println("Running:" + sql);
            res1 = stmt.execute(sql);

            // 执行“select * query”操作
            sql = "select * from " + tableName;
            System.out.println("Running:" + sql);
            res = stmt.executeQuery(sql);
            System.out.println("执行“select * query”运行结果:");
            while (res.next()) {
                System.out.println(res.getInt(1) + "\t" + res.getString(2));
            }

            // 执行“regular hive query”操作
            sql = "select count(1) from " + tableName;
            System.out.println("Running:" + sql);
            res = stmt.executeQuery(sql);
            System.out.println("执行“regular hive query”运行结果:");
            while (res.next()) {
                System.out.println(res.getString(1));

            }

            conn.close();
            conn = null;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            log.error(driverName + " not found!", e);
            System.exit(1);
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("Connection error!", e);
            System.exit(1);
        }

    }
}