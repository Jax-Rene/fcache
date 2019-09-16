package me.zhuangjy.bean;

import lombok.Builder;
import lombok.Data;
import java.util.List;

/**
 * 记录真实缓存文件信息
 */
@Data
@Builder
public class CacheFile {

    private String filePath;
    private String md5Sum;
    private long fileSize;
    private List<CacheFile> realFiles;

}
