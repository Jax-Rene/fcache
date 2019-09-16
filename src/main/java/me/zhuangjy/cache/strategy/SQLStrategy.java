package me.zhuangjy.cache.strategy;

import lombok.extern.slf4j.Slf4j;
import me.zhuangjy.bean.CacheInfo;
import me.zhuangjy.cache.CacheLoader;
import me.zhuangjy.util.ConfigUtil;
import me.zhuangjy.util.DatabasePoolUtil;
import org.apache.commons.collections.MapUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * SQL 缓存加载策略
 *
 * @author zhuangjy
 * @create 2019-09-15 15:12
 */
@Slf4j
public class SQLStrategy implements Strategy {

    @Override
    public void fresh(CacheInfo cacheInfo) throws Exception {
        String cacheName = cacheInfo.getName();
        String database = cacheInfo.getDatabase();
        String sql = cacheInfo.getContent();
        String databaseInfo = CacheLoader.getInstance().getDatabaseInfo(database);

        // 1. 执行SQL生成数据
        // 2. 按列落地至临时目录
        // 3. 比较fileSize & md5Sum 确定是否需要更新
        List<Map<String, Object>> resultDatas = DatabasePoolUtil.getResult(DatabasePoolUtil.getDS(database, databaseInfo), sql);
        if (resultDatas.size() > 0) {
            Set<String> columns = resultDatas.get(0).keySet();
            Map<String, StringBuilder> fileContent = new HashMap<>(columns.size(), 1);

            for (String column : columns) {
                StringBuilder sb = new StringBuilder();
                fileContent.put(column, sb);

                for (Map<String, Object> data : resultDatas) {
                    sb.append(MapUtils.getString(data, column, ""))
                            .append("\n");
                }
                sb.deleteCharAt(sb.length() - 1);
            }

            String tmpDir = ConfigUtil.getConfiguration().getString("tmp.dirs");
            for (Map.Entry<String, StringBuilder> entry : fileContent.entrySet()) {
                String column = entry.getKey();
                StringBuilder sb = entry.getValue();
            }
        } else {
            log.warn("cache:{} get empty!", cacheName);
        }
    }


}
