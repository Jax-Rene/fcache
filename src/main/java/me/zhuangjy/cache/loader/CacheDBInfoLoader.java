package me.zhuangjy.cache.loader;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import me.zhuangjy.util.DatabasePoolUtil;
import org.apache.commons.collections.MapUtils;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Data
public class CacheDBInfoLoader implements Loader {

    /**
     * 数据库信息 name -> db_info(json)
     */
    private Map<String, String> cacheDBInfoView = Collections.emptyMap();

    /**
     * 加载数据库地址信息
     *
     * @throws SQLException
     */
    @Override
    public void refreshView() throws Exception {
        log.info("start refresh database info");
        String sql = "SELECT name,db_info FROM source_db_info";
        Map<String, String> tmp = new HashMap<>(cacheDBInfoView.size());
        List<Map<String, Object>> list = DatabasePoolUtil.getResult(DatabasePoolUtil.getDS(), sql);
        list.forEach(c -> tmp.put(
                MapUtils.getString(c, "name"),
                MapUtils.getString(c, "db_info")));
        cacheDBInfoView = tmp;
        log.info("refresh database info suc. size:{}", cacheDBInfoView.size());
    }

}
