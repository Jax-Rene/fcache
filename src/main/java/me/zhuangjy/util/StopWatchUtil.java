package me.zhuangjy.util;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StopWatchUtil {

    public static long start(String msg) {
        if (msg != null)  {
                log.info(msg);
        }
        return System.currentTimeMillis();
    }

    public static long start() {
        return start(null);
    }

    public static void end(long startTime, String msg) {
        log.info(msg + " cost:{}ms", System.currentTimeMillis() - startTime);
    }

}
