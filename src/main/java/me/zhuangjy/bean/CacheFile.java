package me.zhuangjy.bean;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 记录真实缓存文件信息
 */
@Slf4j
@Data
public class CacheFile {

    /**
     * 文件md5Sum存储对应内容md5,目录则使用所有子文件md5sum拼接结果
     **/
    private String md5Sum;
    private String filePath;
    private List<CacheFile> realFiles;
    private ReentrantReadWriteLock lock;

    public CacheFile(String filePath, String md5Sum) {
        this.filePath = filePath;
        this.md5Sum = md5Sum;
        this.realFiles = new ArrayList<>();
        this.lock = new ReentrantReadWriteLock();
    }


    /**
     * 根据新的缓存内容更新文件
     * 1. 计算新的缓存MD5值,若产生变化则进入下一步否则直接退出
     * 2. 获取写锁,更新缓存文件以及对应信息
     *
     * @param content
     * @return
     */
    public boolean update(String content) {
        String newMd5Sum = DigestUtils.md5Hex(content);
        if (newMd5Sum.equalsIgnoreCase(md5Sum)) {
            log.info("update {},but cache not modified.nothing change.", filePath);
            return false;
        }

        // 更新操作期间获取写锁，保证和下载不冲突
        lock.writeLock().lock();
        try {
            long now = System.currentTimeMillis();
            this.md5Sum = newMd5Sum;
            FileUtils.write(new File(filePath), content);
            log.info("update:{} suc! cost:{} ms", filePath, System.currentTimeMillis() - now);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        lock.writeLock().unlock();
        return true;
    }
}
