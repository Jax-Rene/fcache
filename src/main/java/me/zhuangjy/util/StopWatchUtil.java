package me.zhuangjy.util;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StopWatchUtil {

    public static long start(String msg) {
        log.info(msg);
        return System.currentTimeMillis();
    }

    public static void end(long startTime, String msg) {
        log.info(msg + " cost:{}ms", System.currentTimeMillis() - startTime);
    }

}
