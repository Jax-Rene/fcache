package me.zhuangjy.cache.strategy;

import lombok.extern.slf4j.Slf4j;
import me.zhuangjy.bean.CacheInfo;
import me.zhuangjy.cache.CacheLoader;
import me.zhuangjy.util.DatabasePoolUtil;

import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * MySQL 缓存加载策略
 *
 * @author zhuangjy
 * @create 2019-09-15 15:12
 */
@Slf4j
public class SQLStrategy implements Strategy {

    @Override
    public void fresh(CacheInfo cacheInfo) throws Exception {
        String cacheName = cacheInfo.getName();
        String database = cacheInfo.getDatabase();
        String sql = cacheInfo.getContent();
        String databaseInfo = CacheLoader.getInstance().getDatabaseInfo(database);

        // 执行SQL
        List<Map<String, Object>> resultDatas = DatabasePoolUtil.getResult(DatabasePoolUtil.getDS(database, databaseInfo), sql);
        if (resultDatas.size() > 0) {
            Set<String> columns = resultDatas.get(0).keySet();
            for (String column : columns) {

            }

        } else {
            log.warn("cache:{} get empty!", cacheName);
        }
    }


}
