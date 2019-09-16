package me.zhuangjy.cache.loader;


interface Loader {

    /**
     * 定义加载器加载逻辑
     *
     * @throws Exception
     */
    void refreshView() throws Exception;

}
