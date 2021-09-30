package com.geek.sql;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.LogManager;

public class PrestoJDBC {
    //获取数据库用户名
    private static String user = "root";
    //获取数据库密码
    private static String password;
    //获取数据库URL
    private static String jdbcUrl = "jdbc:presto://192.168.2.7:8081/hive/code_set";
    //获取连接对象
    private static Connection conn = null;

    //私有化构造函数，确保不被外部实例化
    private PrestoJDBC() {
    }

    //单例模式获取数据库连接
    public static Connection getConnectionInstance() throws IOException, ClassNotFoundException, SQLException {
        if (conn == null) {
            synchronized (PrestoJDBC.class) {
                if (conn == null) {
                    conn = DriverManager.getConnection(jdbcUrl, user, password);
                }
            }
        }
        return conn;
    }

    public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
        Statement stmt = PrestoJDBC.getConnectionInstance().createStatement();
        ResultSet rs = null;
        List<String> nameList = new ArrayList<String>(Arrays.asList("COUNT_DISTINCT", "APPROX_DISTINCT", "APPROX_SET"));
        List<String> sqlList = new ArrayList<String>();
        sqlList.add("SELECT count(DISTINCT a.sevencode) FROM yys_code a");
        sqlList.add("SELECT approx_distinct(a.sevencode,0.0040625) AS CNT FROM yys_code a");
        sqlList.add("WITH temp AS (\n" +
                "SELECT cast(approx_set(a.sevencode,0.0040625) AS varbinary) AS hll\n" +
                "FROM hive.code_set.yys_code a\n" +
                ") \n" +
                "SELECT cardinality(merge(cast(hll AS HyperLogLog))) AS uv\n" +
                "FROM temp");
        try {
            for (int i = 0; i < sqlList.size(); i++) {
                rs = stmt.executeQuery(sqlList.get(i));
                while (rs.next()) {
                    long cnt = rs.getLong(1);
                    System.out.println(String.format("%s=%s", nameList.get(i), cnt));
                }
            }
        } finally {
            free(rs, stmt, conn);
        }
    }


    public static void free(ResultSet rs, Statement ps, Connection conn) {
        try {
            if (rs != null)
                rs.close();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        } finally {
            try {
                if (ps != null)
                    ps.close();
            } catch (SQLException e) {
                System.err.println(e.getMessage());
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                    }
                }
            }
        }
    }
}
