package me.zhuangjy.cache.strategy;

import me.zhuangjy.util.ConstantUtil;

import java.sql.SQLException;

/**
 * 策略选择器
 *
 * @author zhuangjy
 * @create 2019-09-15 15:16
 */
public enum StrategySelector {

    SQL(ConstantUtil.SQL_STRATEGY, new SQLStrategy());

    private String type;
    private Strategy strategy;

    StrategySelector(String type, Strategy strategy) {
        this.type = type;
        this.strategy = strategy;
    }

    /**
     * 根据类别名称获取策略
     *
     * @param name
     * @return
     * @throws SQLException
     */
    public static Strategy getStrategy(String name) {
        for (StrategySelector selector : StrategySelector.values()) {
            if (selector.type.equals(name)) {
                return selector.strategy;
            }
        }
        throw new UnsupportedOperationException("No found cache type of name " + name);
    }

}
