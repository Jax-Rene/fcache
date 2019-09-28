package me.zhuangjy.cache.strategy;

import lombok.extern.slf4j.Slf4j;
import me.zhuangjy.bean.CacheFile;
import me.zhuangjy.bean.CacheInfo;
import me.zhuangjy.cache.CacheLoader;
import me.zhuangjy.util.ConfigUtil;
import me.zhuangjy.util.DatabasePoolUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;


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

        long now = System.currentTimeMillis();
        log.info("starting to cache:{}", cacheName);
        String databaseInfo = CacheLoader.getInstance().getDatabaseInfo(database);

        // 1. 执行SQL生成数据缓存在内存中
        // 2. 尝试更新文件
        // 3. 若文件不存在直接写入
        // 4. 否则更新文件
        // 5. 更新缓存信息 (不需要,因为会定时刷新)
        List<Map<String, Object>> resultDatas = DatabasePoolUtil.getResult(DatabasePoolUtil.getDS(database, databaseInfo), sql);
        if (resultDatas.size() > 0) {
            Set<String> columns = resultDatas.get(0).keySet().stream().map(String::toLowerCase).collect(Collectors.toSet());
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

            // 更新文件
            Optional<CacheFile> cacheFile = CacheLoader.getInstance().getCacheFile(cacheName);
            String cacheDir = ConfigUtil.getConfiguration().getString("cache.dir");

            if (!cacheFile.isPresent()) {
                Path destDir = Paths.get(cacheDir, cacheName);
                if (destDir.toFile().mkdirs()) {
                    // 按列写入子目录
                    for (Map.Entry<String, StringBuilder> entry : fileContent.entrySet()) {
                        Path path = Paths.get(cacheDir, cacheName, entry.getKey());
                        FileUtils.write(path.toFile(), entry.getValue());
                    }
                }
            } else {
                // TODO 清理无效的列文件
                for (Map.Entry<String, StringBuilder> entry : fileContent.entrySet()) {
                    Path path = Paths.get(cacheDir, cacheName, entry.getKey());
                    CacheFile cacheRoot = cacheFile.get();

                    for (CacheFile file : cacheRoot.getRealFiles()) {
                        // 查找更新文件
                        if (file.getFilePath().equalsIgnoreCase(path.toString())) {
                            file.update(entry.getValue().toString());
                            break;
                        }
                    }
                }
            }
            log.info("cache:{} suc. cost:{}", cacheName, System.currentTimeMillis() - now);
        } else {
            log.warn("cache:{} get empty!", cacheName);
        }
    }
}
