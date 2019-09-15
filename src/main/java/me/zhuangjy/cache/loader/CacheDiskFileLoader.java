package me.zhuangjy.cache.loader;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import me.zhuangjy.bean.CacheFile;
import me.zhuangjy.bean.CacheInfo;
import me.zhuangjy.util.ConfigUtil;
import me.zhuangjy.util.DatabasePoolUtil;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.*;

@Slf4j
@Data
public class CacheDiskFileLoader implements Loader {

    /**
     * 缓存目录文件信息
     */
    private Map<String, Map<String, List<CacheFile>>> cacheDiskFileView = Collections.emptyMap();

    /**
     * 加载磁盘文件缓存信息
     */
    @Override
    public void refreshView() throws Exception {
        // <dir, <cacheName,[CacheFile]>>
        Map<String, Map<String, List<CacheFile>>> map = new HashMap<>();

        String[] dirs = ConfigUtil.getConfiguration().getString("cache.dirs").split(",");
        for (String dir : dirs) {
            for (File file : Objects.requireNonNull(new File(dir).listFiles())) {
                if (!map.containsKey(dir)) {
                    map.put(dir, new HashMap<>());
                }

                Map<String, List<CacheFile>> cacheListMap = map.get(dir);
                String cacheName = file.getName();
                cacheListMap.put(cacheName, new ArrayList<>());

                // 若包含列文件则使用列式存储
                // 否则为整份文件存储
                List<CacheFile> cacheFiles = cacheListMap.get(cacheName);
                if (file.isDirectory()) {
                    for (File columnFile : Objects.requireNonNull(file.listFiles())) {
                        cacheFiles.add(getCacheFile(columnFile));
                    }
                } else {
                    cacheFiles.add(getCacheFile(file));
                }
            }
        }

        cacheDiskFileView = map;
    }

    private CacheFile getCacheFile(File file) throws IOException {
        try (InputStream inputStream = new FileInputStream(file.getAbsolutePath())) {
            return CacheFile.builder()
                    .filePath(file.getAbsolutePath())
                    .md5Sum(DigestUtils.md5Hex(inputStream)).build();
        }
    }


}
