package me.zhuangjy.cache.loader;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import me.zhuangjy.bean.CacheInfo;
import me.zhuangjy.util.DatabasePoolUtil;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Data
public class CacheViewLoader implements Loader {

    /**
     * 维护 CacheName -> CacheType
     **/
    private Map<String, CacheInfo> cacheView = Collections.emptyMap();

    /**
     * 加载缓存任务配置信息
     *
     * @throws SQLException
     */
    @Override
    public void refreshView() throws Exception {
        String sql = "SELECT name,type,content,ttl,database from fcache.cache_task";

        Map<String, CacheInfo> tmp = new HashMap<>(cacheView.size());
        List<Map<String, Object>> list = DatabasePoolUtil.getResult(DatabasePoolUtil.getDS(), sql);
        list.stream()
                .map(CacheInfo::convertFromMap)
                .forEach(c -> tmp.put(c.getName(), c));

        cacheView = tmp;
    }

}
