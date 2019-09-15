package me.zhuangjy.cache.strategy;

import me.zhuangjy.bean.CacheInfo;

/**
 * Created by zhuangjy on 2019-09-15.
 */
public interface Strategy {

    void fresh(CacheInfo cacheInfo) throws Exception;

}
