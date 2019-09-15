package me.zhuangjy.cache.strategy;

import lombok.extern.slf4j.Slf4j;
import me.zhuangjy.bean.CacheInfo;
import me.zhuangjy.util.DatabasePoolUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static me.zhuangjy.cache.CacheLoader.getDatabaseInfo;

/**
 * MySQL 缓存加载策略
 *
 * @author zhuangjy
 * @create 2019-09-15 15:12
 */
@Slf4j
public class SQLStrategy implements Strategy {

    @Override
    public void fresh(CacheInfo cacheInfo) {
        String cacheName = cacheInfo.getName();
        String database = cacheInfo.getDatabase();
        String sql = cacheInfo.getContent();
        String databaseInfo = getDatabaseInfo(database);
        try (Connection connection = DatabasePoolUtil.getDS(database, databaseInfo).getConnection();
             PreparedStatement pstmt = connection.prepareStatement(sql)) {
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
    }


}
