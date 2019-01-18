package com.kevin.java;

import java.io.Serializable;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @author kevin
 * @version 1.0
 * @description     集群中数据库操作类
 * @createDate 2019/1/16
 */
public class JDBCWrapper implements Serializable {

    private static JDBCWrapper jdbcInstance = null;
    // 创建队列保存数据库的连接
    private static LinkedBlockingDeque<Connection> dbConnectionPool = new LinkedBlockingDeque<Connection>();
    // 静态执行该类时启动该数据库驱动
    static{
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        JDBCWrapper jdbcWrapper = JDBCWrapper.getJDBCInstance();
        Object[] obj = new Object[]{"1", "1", "1", "1"};
        ArrayList<Object[]> list = new ArrayList<>();
        list.add(obj);
        jdbcWrapper.doBatch("insert into result values(?,?,?,?)",list);

    }

    // 创建连接
    public static JDBCWrapper getJDBCInstance() {
        // 判断是否创建过连接
        if(jdbcInstance == null) {
            // 如果没有则将该类锁起来，避免资源出错
            synchronized (JDBCWrapper.class) {
                // 再次判断是否创建过连接
                if(jdbcInstance == null) {
                    // 没有，创建连接
                    jdbcInstance = new JDBCWrapper();
                }
            }
        }
        // 如果创建过连接则直接返回，没有创建过连接则创建连接再返回
        return jdbcInstance;
    }

    // 无参构造函数
    private JDBCWrapper() {
        // 创建10次数据库连接
        for (int i = 0; i < 10; i++) {
            String url = "jdbc:mysql://Slave1:3306/sparkdb";
            String name = "root";
            String password = "Hadoop01!";
            try {
                // 连接数据库
                Connection conn = DriverManager.getConnection(url, name, password);
                // 将创建的连接保存到队列中
                dbConnectionPool.put(conn);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    // 获取队列中已有的连接
    public synchronized Connection getConnection() {
        // 如果队列中不存在连接则睡眠20毫秒，死循环，直到有连接在队列
        while (0 == dbConnectionPool.size()) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        // 取出一个连接
        return dbConnectionPool.poll();
    }

    // 批处理插入数据
    public int[] doBatch(String sqlText, List<Object[]> paramsList) {
        // 获取一个数据库连接作为操作
        Connection conn = getConnection();
        PreparedStatement preparedStatement = null;
        int[] result = null;
        try {
            // 关闭事务的自动提交
            conn.setAutoCommit(false);
            // 将sql语句保存到PreparedStatement中
            preparedStatement = conn.prepareStatement(sqlText);
            // 遍历集合的数据并根据对应的下角标参数保存到PreparedStatement
            for(Object[] parameters : paramsList){
                for (int i = 0; i < parameters.length; i++) {
                    preparedStatement.setObject(i + 1, parameters[i]);
                }
                // 将操作存到批处理
                preparedStatement.addBatch();
            }
            // 执行批处理
            result = preparedStatement.executeBatch();
            // 提交事务
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if(preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(conn != null) {
                try {
                    dbConnectionPool.put(conn);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        return result;
    }

    // 查询数据
    public void doQuery(String sqlText, Object[] paramsList, ExecuteCallBack callBack) {
        // 获取数据库连接
        Connection conn = getConnection();
        PreparedStatement preparedStatement = null;
        ResultSet result = null;
        try {
            // 将需要查询的SQL语句放到PreparedStatement
            conn.prepareStatement(sqlText);
            // 如果对应的参数不为null则对应保存到PreparedStatement
            if(paramsList != null) {
                for (int i = 0; i < paramsList.length; i++) {
                    preparedStatement.setObject(i+1,paramsList[i]);
                }
            }
            // 查询数据
            result = preparedStatement.executeQuery();
            // 返回查询的数据
            callBack.resultCallBack(result);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            // 如果连接不为null则加入队列
            if (conn != null) {
                try {
                    dbConnectionPool.put(conn);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}

// 自定义接口异常
interface ExecuteCallBack {
    void resultCallBack(ResultSet result) throws Exception;
}
