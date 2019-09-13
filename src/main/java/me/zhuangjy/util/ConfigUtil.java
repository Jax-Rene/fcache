package me.zhuangjy.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * 配置文件加载工具类
 *
 * @author zhuangjy
 * @create 2019-09-13 22:44
 */
@Slf4j
public class ConfigUtil {

    private static PropertiesConfiguration configuration = new PropertiesConfiguration();

    static {
        try (InputStream inputStream = ConfigUtil.class.getResourceAsStream("/config.properties")) {
            configuration.load(inputStream);
        } catch (ConfigurationException | IOException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public static void add(String filePath, boolean override) throws ConfigurationException {
        File file = new File(filePath);
        if (file.exists() && file.isFile()) {
            if (override) {
                PropertiesConfiguration tmp = new PropertiesConfiguration(file);
                tmp.getKeys().forEachRemaining(i -> configuration.setProperty(i, tmp.getProperty(i)));
            } else {
                configuration.append(new PropertiesConfiguration(file));
            }
        } else {
            log.error("{} not a readable file.nothing to be add.", filePath);
        }
    }

    public static PropertiesConfiguration getConfiguration() {
        return configuration;
    }
}
