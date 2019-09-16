package me.zhuangjy.cache.strategy;

import me.zhuangjy.bean.CacheInfo;

/**
 * Created by zhuangjy on 2019-09-15.
 */
public interface Strategy {

    /**
     * 定义策略刷新逻辑
     *
     * @param cacheInfo
     * @throws Exception
     */
    void fresh(CacheInfo cacheInfo) throws Exception;

}
