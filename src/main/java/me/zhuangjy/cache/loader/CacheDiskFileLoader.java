package me.zhuangjy.cache.loader;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import me.zhuangjy.bean.CacheFile;
import me.zhuangjy.util.ConfigUtil;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * @author zhuangjy
 */
@Slf4j
@Data
public class CacheDiskFileLoader implements Loader {

    /**
     * 缓存目录文件信息
     * <CacheName,[RealFile,RealFile...]>
     * or cache 整文件缓存
     * <CacheName,[RealFile,RealFile...]>
     */
    private Map<String, CacheFile> cacheDiskFileView = Collections.emptyMap();

    /**
     * 加载磁盘文件缓存信息
     */
    @Override
    public void refreshView() throws Exception {
        Map<String, CacheFile> map = new HashMap<>(16);

        String dir = ConfigUtil.getConfiguration().getString("cache.dir");
        for (File file : Objects.requireNonNull(new File(dir).listFiles())) {
            String cacheName = file.getName();
            map.put(cacheName, getCacheFile(file));

            // 若包含列文件则使用列式存储
            // 否则为整份文件存储
            CacheFile cacheRoot = map.get(cacheName);
            File cacheFile = new File(cacheRoot.getFilePath());

            if (cacheFile.isDirectory()) {
                for (File columnFile : Objects.requireNonNull(cacheFile.listFiles())) {
                    cacheRoot.getRealFiles().add(getCacheFile(columnFile));
                }
            }
        }

        cacheDiskFileView = map;
    }

    private CacheFile getCacheFile(File file) throws IOException {
        try (InputStream inputStream = new FileInputStream(file.getAbsolutePath())) {
            return new CacheFile(file.getAbsolutePath(),DigestUtils.md5Hex(inputStream));
        }
    }


}
