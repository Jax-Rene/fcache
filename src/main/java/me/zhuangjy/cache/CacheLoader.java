package me.zhuangjy.cache;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import me.zhuangjy.bean.CacheInfo;
import me.zhuangjy.cache.strategy.Strategy;
import me.zhuangjy.cache.strategy.StrategySelector;
import me.zhuangjy.util.DatabasePoolUtil;
import org.apache.commons.collections.MapUtils;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * 缓存加载类,支持SQL、自定义类型等
 *
 * @author zhuangjy
 * @create 2019-09-14 07:58
 */
@Slf4j
public class CacheLoader {

    /**
     * 维护 CacheName -> CacheType
     **/
    private static Map<String, CacheInfo> cacheView = Collections.emptyMap();
    /**
     * 维护 CacheKey -> ExpiredTimestamp
     */
    private static Map<String, Integer> cacheExpiredView = Collections.emptyMap();
    /**
     * 数据库信息 name -> db_info(json)
     */
    private static Map<String, String> cacheDBInfoView = Collections.emptyMap();

    static {
        try {
            refreshCacheView();
            ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("fresh-task-%d").build();
            ScheduledThreadPoolExecutor poolExecutor = new ScheduledThreadPoolExecutor(1, factory);
            poolExecutor.scheduleAtFixedRate(() -> {
                try {
                    refreshCacheView();
                    refreshCacheDBInfoView();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }, 0L, 15L, TimeUnit.SECONDS);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private CacheLoader() {

    }

    /**
     * 加载缓存任务配置信息
     *
     * @throws SQLException
     */
    private static void refreshCacheView() throws SQLException {
        String sql = "SELECT name,type,content,ttl,database from fcache.cache_task";
        String[] args = {"name", "type", "content", "ttl", "database"};

        Map<String, CacheInfo> tmp = new HashMap<>(cacheView.size());
        List<Map<String, Object>> list = DatabasePoolUtil.getResult(DatabasePoolUtil.getDS(),
                sql, args);
        list.stream()
                .map(CacheInfo::convertFromMap)
                .forEach(c -> tmp.put(c.getName(), c));

        cacheView = tmp;
    }

    /**
     * 加载数据库地址信息
     *
     * @throws SQLException
     */
    private static void refreshCacheDBInfoView() throws SQLException {
        String sql = "SELECT name,db_info FROM source_db_info";
        Map<String, String> tmp = new HashMap<>(cacheDBInfoView.size());
        List<Map<String, Object>> list = DatabasePoolUtil.getResult(DatabasePoolUtil.getDS(), sql, "name", "db_info");
        list.forEach(c -> tmp.put(
                MapUtils.getString(c, "name"),
                MapUtils.getString(c, "db_info")));
        cacheDBInfoView = tmp;
    }

    /**
     * 刷新缓存对外暴露接口
     *
     * @param cacheName
     */
    public static void fresh(String cacheName) {
        Strategy strategy = getStrategy(cacheName);
        CacheInfo cacheInfo = cacheView.get(cacheName);
        strategy.fresh(cacheInfo);
        // 设置缓存过期时间
        int expiredTime = (int) (System.currentTimeMillis() / 1000) + cacheInfo.getTtl();
        cacheExpiredView.put(cacheName, expiredTime);
    }

    /**
     * 获取数据库连接地址信息
     *
     * @param dbName 数据库名称
     * @return
     */
    public static String getDatabaseInfo(String dbName) {
        return Preconditions.checkNotNull(cacheDBInfoView.get(dbName));
    }

    /**
     * 根据缓存名称获取解析策略
     *
     * @param cacheName
     * @return
     */
    private static Strategy getStrategy(String cacheName) {
        return Preconditions.checkNotNull(StrategySelector
                .getStrategy(MapUtils.getString(cacheView, cacheName, "")));
    }
}
