package me.zhuangjy.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.MapUtils;

import java.util.Map;

/**
 * Cache缓存信息
 *
 * @author zhuangjy
 * @create 2019-09-14 08:24
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CacheInfo {

    private String name;
    private String type;
    private String content;
    private int ttl;
    private String database;

    public static CacheInfo convertFromMap(Map<String, Object> map) {
        String name = (String) map.get("name");
        String type = (String) map.get("type");
        String content = (String) map.get("content");
        int ttl = MapUtils.getIntValue(map, "ttl", 60);
        String database = (String) map.get("database");
        return new CacheInfo(name, type, content, ttl, database);
    }

}
