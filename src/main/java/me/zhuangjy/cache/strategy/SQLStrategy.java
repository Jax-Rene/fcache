package me.zhuangjy.cache.strategy;

import lombok.extern.slf4j.Slf4j;
import me.zhuangjy.bean.CacheFile;
import me.zhuangjy.bean.CacheInfo;
import me.zhuangjy.cache.CacheLoader;
import me.zhuangjy.util.ConfigUtil;
import me.zhuangjy.util.DatabasePoolUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;


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

            String tmpDir = ConfigUtil.getConfiguration().getString("tmp.dir");
            Path path = Paths.get(tmpDir, cacheName);
            File file = path.toFile();
            if (!file.exists()) {
                log.info("file:{} not exit. mkdirs success:{}", file.getName(), file.mkdirs());
            }

            Optional<CacheFile> cacheFile = CacheLoader.getInstance().getCacheFile(cacheName);
            for (Map.Entry<String, StringBuilder> entry : fileContent.entrySet()) {
                String column = entry.getKey();
                path = Paths.get(tmpDir, cacheName, column);
                file = path.toFile();

                // 直接写入临时文件,然后和现有缓存文件做对比,有产生变化则更新
                StringBuilder content = fileContent.get(column);
                FileUtils.write(file, content);
            }

            // 目标文件不存在直接复制
            // 文件存在则对比是否需要更新
            if (!cacheFile.isPresent()) {
                String cacheDir = ConfigUtil.getConfiguration().getString("cache.dir");
                Path srcDir = Paths.get(tmpDir, cacheName);
                Path destDir = Paths.get(cacheDir, cacheName);
                FileUtils.moveDirectory(srcDir.toFile(), destDir.toFile());
            } else {

            }

        } else {
            log.warn("cache:{} get empty!", cacheName);
        }
    }
}
