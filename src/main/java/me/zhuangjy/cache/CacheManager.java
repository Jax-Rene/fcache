package me.zhuangjy.cache;

import me.zhuangjy.util.ConfigUtil;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

/**
 * 缓存控制类
 *
 * @author zhuangjy
 * @create 2019-09-13 23:24
 */
public class CacheManager {

    private static final String JDBC_URL = ConfigUtil.getConfiguration().getString("jdbc.mysql.url");
    private static final String JDBC_USERNAME = ConfigUtil.getConfiguration().getString("jdbc.mysql.username");
    private static final String JDBC_PASSWORD = ConfigUtil.getConfiguration().getString("jdbc.mysql.password");

    static {
        ScheduledThreadPoolExecutor poolExecutor = new ScheduledThreadPoolExecutor(1);
    }

}
