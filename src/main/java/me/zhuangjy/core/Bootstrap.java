package me.zhuangjy.core;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;
import me.zhuangjy.cache.CacheLoader;
import me.zhuangjy.util.StopWatchUtil;

@Slf4j
public class Bootstrap {

    private static final int PORT = 8897;

    public static void main(String[] args) throws Exception {

        long ts = StopWatchUtil.start("Loading expired now.");
        CacheLoader.getInstance().freshAllExpired();
        StopWatchUtil.end(ts, "Loaded suc.");

        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new HttpStaticFileServerInitializer());

            Channel ch = b.bind(PORT).sync().channel();
            log.info("Open your web browser and navigate to http://127.0.0.1:{}/", PORT);
            ch.closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
