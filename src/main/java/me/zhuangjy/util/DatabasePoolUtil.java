package me.zhuangjy.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.*;
import java.util.*;

/**
 * 数据库连接池
 *
 * @author zhuangjy
 * @create 2019-09-14 08:05
 */
public class DatabasePoolUtil {

    private static Map<String, HikariDataSource> dsMap;

    static {
        String url = Preconditions.checkNotNull(ConfigUtil.getConfiguration().getString(ConstantUtil.JDBC_URL));
        String username = Preconditions.checkNotNull(ConfigUtil.getConfiguration().getString(ConstantUtil.JDBC_USERNAME));
        String password = Preconditions.checkNotNull(ConfigUtil.getConfiguration().getString(ConstantUtil.JDBC_PASSWORD));
        dsMap = new HashMap<>();
        dsMap.put(ConstantUtil.DEFAULT_DS, createDS(url, username, password));
    }

    private static HikariDataSource createDS(String url, String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.addDataSourceProperty("rewriteBatchedStatements", "true");
        config.addDataSourceProperty("characterEncoding", "utf-8");
        return new HikariDataSource(config);
    }

    public static HikariDataSource getDS() {
        return getDS(ConstantUtil.DEFAULT_DS, null);
    }

    public synchronized static HikariDataSource getDS(String name, String databaseInfo) {
        if (!dsMap.containsKey(name)) {
            JSONObject jsonObject = JSON.parseObject(databaseInfo);
            String url = jsonObject.getString(ConstantUtil.JDBC_URL);
            String username = jsonObject.getString(ConstantUtil.JDBC_USERNAME);
            String password = jsonObject.getString(ConstantUtil.JDBC_PASSWORD);
            HikariDataSource ds = createDS(url, username, password);
            dsMap.put(name, ds);
        }
        return dsMap.get(name);
    }

    /**
     * 根据输入SQL执行结果后格式化返回
     *
     * @param sql
     * @return
     * @throws SQLException
     */
    public static List<Map<String, Object>> getResult(HikariDataSource ds, String sql) throws SQLException {
        try (Connection connection = ds.getConnection();
             PreparedStatement pstmt = connection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            return rs2MapList(rs);
        }
    }

    /**
     * Result set covert to map
     *
     * @param rs
     * @return
     * @throws SQLException
     */
    private static List<Map<String, Object>> rs2MapList(ResultSet rs) throws SQLException {
        List<Map<String, Object>> results = new LinkedList<>();
        ResultSetMetaData metaData = rs.getMetaData();
        int colCount = metaData.getColumnCount();
        List<String> colNameList = new ArrayList<>();
        for (int i = 0; i < colCount; i++) {
            colNameList.add(metaData.getColumnLabel(i + 1));
        }

        while (rs.next()) {
            Map<String, Object> map = new HashMap<>(16);
            for (String key : colNameList) {
                Object value = rs.getString(key);
                map.put(key, value);
            }
            results.add(map);
        }
        return results;
    }
}
