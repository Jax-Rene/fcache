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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

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
        File[] files = new File(dir).listFiles();
        if (files == null) {
            log.info("{} has nothing file.", dir);
            return;
        }

        for (File file : files) {
            String cacheName = file.getName().toLowerCase();
            boolean isDir = file.isDirectory();
            map.put(cacheName, getCacheFile(file));

            // 若包含列文件则使用列式存储
            // 否则为整份文件存储
            CacheFile cacheRoot = map.get(cacheName);
            File cacheFile = new File(cacheRoot.getFilePath());

            if (isDir) {
                StringBuilder parentMd5Sum = new StringBuilder();
                for (File columnFile : Objects.requireNonNull(cacheFile.listFiles())) {
                    CacheFile innerFile = getCacheFile(columnFile);
                    parentMd5Sum.append(innerFile.getMd5Sum());
                    cacheRoot.getRealFiles().add(innerFile);
                }
                cacheRoot.setMd5Sum(DigestUtils.md5Hex(parentMd5Sum.toString()));
            }
        }

        cacheDiskFileView = map;
    }

    /**
     * 获取 CacheFile 对象，若是目录暂时无记录MD5Sum
     *
     * @param file
     * @return
     * @throws IOException
     */
    private CacheFile getCacheFile(File file) throws IOException {
        if (file.isDirectory()) {
            return new CacheFile(file.getAbsolutePath(), null);
        } else {
            try (InputStream inputStream = new FileInputStream(file.getAbsolutePath())) {
                return new CacheFile(file.getAbsolutePath(), DigestUtils.md5Hex(inputStream));
            }
        }
    }


}
