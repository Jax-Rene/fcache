package me.zhuangjy.cache;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import me.zhuangjy.bean.CacheInfo;
import me.zhuangjy.util.DatabasePoolUtil;

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
public enum CacheLoader {

    /**
     * SQL 类型,直接执行对应数据库
     */
    SQL("sql") {
        @Override
        void fresh(CacheInfo cacheInfo) {
            String cacheName = cacheInfo.getName();
            String database = cacheInfo.getDatabase();
            String sql = cacheInfo.getContent();


        }

    };

    /**
     * 维护 CacheName -> CacheType
     **/
    private static Map<String, CacheInfo> cacheView = Collections.emptyMap();
    /**
     * 维护 CacheKey -> ExpiredTimestamp
     */
    private static Map<String, Integer> cacheExpiredView = Collections.emptyMap();

    static {
        try {
            refreshCacheView();
            ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("fresh-task-%d").build();
            ScheduledThreadPoolExecutor poolExecutor = new ScheduledThreadPoolExecutor(1, factory);
            poolExecutor.scheduleAtFixedRate(() -> {
                try {
                    refreshCacheView();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }, 0L, 15L, TimeUnit.SECONDS);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
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
        List<Map<String, Object>> list = DatabasePoolUtil.getResult(sql, args);
        list.stream()
                .map(CacheInfo::convertFromMap)
                .forEach(c -> tmp.put(c.getName(), c));

        cacheView = tmp;
    }

    /**
     * 根据类别名称获取Loader
     *
     * @param name
     * @return
     * @throws SQLException
     */
    public static CacheLoader getLoader(String name) {
        for (CacheLoader loader : CacheLoader.values()) {
            if (loader.type.equals(name)) {
                return loader;
            }
        }
        throw new UnsupportedOperationException("No found cache type of name " + name);
    }

    private String type;

    CacheLoader(String type) {
        this.type = type;
    }

    /**
     * 刷新缓存逻辑
     *
     * @param cacheInfo
     */
    abstract void fresh(CacheInfo cacheInfo);

    /**
     * 刷新缓存对外暴露接口
     *
     * @param cacheName
     */
    public static void fresh(String cacheName) {
        CacheLoader loader = getLoader(cacheName);
        CacheInfo cacheInfo = cacheView.get(cacheName);
        loader.fresh(cacheInfo);
        // 设置缓存过期时间
        int expiredTime = (int) (System.currentTimeMillis() / 1000) + cacheInfo.getTtl();
        cacheExpiredView.put(cacheName, expiredTime);
    }
}
