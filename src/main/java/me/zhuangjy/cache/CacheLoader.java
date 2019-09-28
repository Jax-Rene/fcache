package me.zhuangjy.cache;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import me.zhuangjy.bean.CacheFile;
import me.zhuangjy.bean.CacheInfo;
import me.zhuangjy.cache.loader.CacheDBInfoLoader;
import me.zhuangjy.cache.loader.CacheDiskFileLoader;
import me.zhuangjy.cache.loader.CacheViewLoader;
import me.zhuangjy.cache.strategy.Strategy;
import me.zhuangjy.cache.strategy.StrategySelector;
import org.apache.commons.collections.MapUtils;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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

    private static volatile CacheLoader instance;

    private CacheViewLoader cacheViewLoader = new CacheViewLoader();
    private CacheDBInfoLoader cacheDBInfoLoader = new CacheDBInfoLoader();
    private CacheDiskFileLoader cacheDiskFileLoader = new CacheDiskFileLoader();

    /**
     * 记录缓存过期时间,若缓存过期则触发更新
     */
    private Map<String, Integer> cacheExpiredView = new ConcurrentHashMap<>();

    public static CacheLoader getInstance() {
        if (instance == null) {
            synchronized (CacheLoader.class) {
                if (instance == null) {
                    CacheLoader loader = new CacheLoader();
                    loader.init();
                    instance = loader;
                }
            }
        }
        return instance;
    }

    /**
     * 定义初始化
     */
    private void init() {
        try {
            refresh();
            ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("fresh-task-%d").build();
            ScheduledThreadPoolExecutor poolExecutor = new ScheduledThreadPoolExecutor(1, factory);
            poolExecutor.scheduleAtFixedRate(() -> {
                try {
                    refresh();
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
     * 定义缓存信息刷新动作
     */
    private void refresh() throws Exception {
        cacheViewLoader.refreshView();
        cacheDBInfoLoader.refreshView();
        cacheDiskFileLoader.refreshView();
        freshAllExpired();
    }


    private CacheLoader() {

    }


    /**
     * 刷新缓存对外暴露接口
     *
     * @param cacheName
     */
    public void freshCache(String cacheName) throws Exception {
        CacheInfo cacheInfo = getCacheInfo(cacheName);
        Strategy strategy = StrategySelector.getStrategy(cacheInfo.getType());
        strategy.fresh(cacheInfo);

        // 设置缓存过期时间
        int expiredTime = (int) (System.currentTimeMillis() / 1000) + cacheInfo.getTtl();
        cacheExpiredView.put(cacheName, expiredTime);
    }

    /**
     * 刷新所有过期缓存，若缓存不存在直接刷新
     * 提交刷新任务到线程池进行异步刷新
     */
    public void freshAllExpired() {
        getAllCacheName()
                .stream()
                .filter(name -> System.currentTimeMillis() / 1000 > MapUtils.getIntValue(cacheExpiredView, name, 0))
                .forEach(FreshTaskPool::submit);
    }

    /**
     * 根据Cache Name 获取对应Cache任务信息
     *
     * @param cacheName
     * @return
     */
    public CacheInfo getCacheInfo(String cacheName) {
        return Preconditions.checkNotNull(cacheViewLoader.getCacheView().get(cacheName));
    }

    /**
     * 获取数据库连接地址信息
     *
     * @param dbName 数据库名称
     * @return
     */
    public String getDatabaseInfo(String dbName) {
        return Preconditions.checkNotNull(cacheDBInfoLoader.getCacheDBInfoView().get(dbName));
    }

    /**
     * 根据缓存名获取实际缓存文件信息
     *
     * @param cacheName
     * @return
     */
    public Optional<CacheFile> getCacheFile(String cacheName) {
        return Optional.ofNullable(cacheDiskFileLoader.getCacheDiskFileView().get(cacheName));
    }

    /**
     * 获取所有缓存名称
     *
     * @return
     */
    public Set<String> getAllCacheName() {
        return cacheViewLoader.getCacheView().keySet();
    }
}
