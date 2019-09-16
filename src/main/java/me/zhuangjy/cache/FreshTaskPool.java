package me.zhuangjy.cache;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import me.zhuangjy.util.ConfigUtil;

import java.util.Set;
import java.util.concurrent.*;

/**
 * 过期任务重新加载任务池
 *
 * @author zhuangjy
 * @create 2019-09-14 08:13
 */
@Slf4j
public class FreshTaskPool {

    private static Set<String> onDoingTasks;
    private static ThreadPoolExecutor pool;

    static {
        ThreadFactory factory = new ThreadFactoryBuilder()
                .setNameFormat("fresh-task-%d").build();
        onDoingTasks = new CopyOnWriteArraySet<>();
        pool = new ThreadPoolExecutor(ConfigUtil.getConfiguration().getInt("task.fresh.pool.core.size"),
                ConfigUtil.getConfiguration().getInt("task.fresh.pool.max.size"),
                0L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(), factory);
    }

    /**
     * 提交更新任务
     *
     * @param cacheName 缓存名称
     * @return true:提交成功 false:提交失败
     */
    public static synchronized boolean submit(String cacheName) {
        if (onDoingTasks.contains(cacheName)) {
            return false;
        }
        pool.submit(new FreshTask(cacheName));
        return true;
    }


    /**
     * 缓存刷新任务
     */
    private static class FreshTask implements Runnable {

        private String cacheName;

        FreshTask(String cacheName) {
            this.cacheName = cacheName;
        }

        @Override
        public void run() {
            try {
                CacheLoader.getInstance().freshCache(cacheName);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                onDoingTasks.remove(cacheName);
            }
        }
    }

}
