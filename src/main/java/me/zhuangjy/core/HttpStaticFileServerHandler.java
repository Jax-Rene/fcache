package me.zhuangjy.core;

import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import lombok.extern.slf4j.Slf4j;
import me.zhuangjy.bean.CacheFile;
import me.zhuangjy.cache.CacheLoader;
import me.zhuangjy.util.StopWatchUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.tuple.Triple;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_0;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;


/**
 * A Netty File Download Server
 *
 * <h1>Http Cache 机制</h1>
 * 1. Response Cache-Control: max-age=X sec
 * 设置客户端超时时间，客户端收到请求后，会记录该时间（now + max-age），若再次请求时时间早于该时间则不发起请求继续使用之前的缓存。
 * 2. If-None-Match
 * 客户端请求时Header会带上缓存的具体Md5值，Server检查若：
 *  （1）所有Md5都不变，返回304状态码，不返回任何数据
 *  （2）至少一个列Md5改变，返回200状态码，以及对应改变列的新数据
 *
 * <h2>Zero Copy</h2>
 * 使用Netty零拷贝技术实现文件流式下载
 *
 * <h3>短连接&长连接支持</h3>
 * 对于短连接下载后直接关闭连接
 */
@Slf4j
public class HttpStaticFileServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final int HTTP_CACHE_SECONDS = 60;
    private FullHttpRequest request;

    @Override
    public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        long startTime = StopWatchUtil.start();
        this.request = request;
        if (!request.decoderResult().isSuccess()) {
            sendMessage(ctx, BAD_REQUEST, "decode faild!");
            return;
        }

        if (!GET.equals(request.method())) {
            this.sendMessage(ctx, METHOD_NOT_ALLOWED, "only support get method");
            return;
        }

        // 判断缓存是否存在
        String cacheName = request.uri().replaceAll("/", "").toLowerCase();
        if (cacheName.contains("?")) {
            cacheName = cacheName.substring(0, cacheName.indexOf('?'));
        }
        Optional<CacheFile> cacheFileOpt = CacheLoader.getInstance().getCacheFile(cacheName);

        if (!cacheFileOpt.isPresent()) {
            this.sendMessage(ctx, NOT_FOUND, "no found message: " + cacheName);
            return;
        }

        // TODO 单独一份数据（非列存储）支持
        // 获取Etag信息
        Map<String, String> paramMap = Arrays.stream(request.headers()
                .get(HttpHeaderNames.IF_NONE_MATCH, "")
                .split("&"))
                .map(m -> m.split("="))
                .collect(Collectors.toMap(m -> m[0], m -> m.length > 1 ? m[1] : ""));

        if (MapUtils.isEmpty(paramMap)) {
            this.sendMessage(ctx, BAD_REQUEST, "no target column set!");
            return;
        }

        // 若整份缓存MD5不变则直接返回304
        // 否则返回修改的列数据
        CacheFile cacheFile = cacheFileOpt.get();
        Map<String, CacheFile> columnMap = new HashMap<>(cacheFile.getRealFiles().size());
        cacheFile.getRealFiles()
                .forEach(colFile -> columnMap.put(Paths.get(colFile.getFilePath()).getFileName().toString().toLowerCase(), colFile));

        Map<String, String> remainColMap = new HashMap<>(paramMap);
        for (Map.Entry<String, String> entry : paramMap.entrySet()) {
            String column = entry.getKey();
            String md5 = entry.getValue();
            if (!columnMap.containsKey(column)) {
                String msg = "no found column: " + column + " of cache:" + cacheName;
                this.sendMessage(ctx, NOT_FOUND, msg);
                return;
            }

            if (columnMap.get(column).getMd5Sum().equals(md5)) {
                remainColMap.remove(column);
            }
        }

        // 所有缓存都未过期直接返回304
        if (remainColMap.size() == 0) {
            this.sendMessage(ctx, NOT_MODIFIED, "");
            return;
        }

        // write contents
        long contentLen = 0L;
        List<Triple<String, ReentrantReadWriteLock.ReadLock,RandomAccessFile>> triples = new ArrayList<>();
        for (String col : remainColMap.keySet()) {
            String startMark = "---###" + col;
            CacheFile colFile = columnMap.get(col);
            // 操作过程要上锁,防止有缓存更新
            ReentrantReadWriteLock.ReadLock readLock = colFile.getLock().readLock();
            readLock.lock();

            RandomAccessFile raf = new RandomAccessFile(new File(colFile.getFilePath()), "r");
            contentLen += raf.length();
            triples.add(Triple.of(startMark, readLock, raf));
        }

        boolean keepAlive = HttpUtil.isKeepAlive(request);
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        HttpUtil.setContentLength(response, contentLen);

        StringBuilder sb = new StringBuilder();
        for (String col : remainColMap.keySet()) {
            CacheFile colFile = columnMap.get(col);
            if (sb.length() != 0) {
                sb.append("&");
            }
            sb.append(col).append("=").append(colFile.getMd5Sum());
        }
        response.headers()
                .set(HttpHeaderNames.ACCESS_CONTROL_MAX_AGE, HTTP_CACHE_SECONDS)
                .set(HttpHeaderNames.ETAG, sb.toString())
                .set(HttpHeaderNames.CONTENT_TYPE, "text/plain");

        if (!keepAlive) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        } else if (request.protocolVersion().equals(HTTP_1_0)) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        }

        ctx.write(response);

        for (Triple<String, ReentrantReadWriteLock.ReadLock, RandomAccessFile> triple : triples) {
            ReentrantReadWriteLock.ReadLock readLock = triple.getMiddle();

            try {
                RandomAccessFile raf = triple.getRight();
                ctx.write(new DefaultFileRegion(raf.getChannel(), 0, raf.length()));
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                sendMessage(ctx, INTERNAL_SERVER_ERROR, "Unknow Exception: " + e.getMessage());
            } finally {
                if (readLock != null) {
                    readLock.unlock();
                }
            }
        }

        ChannelFuture future = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
        GenericFutureListener<? extends Future<? super Void>> completeListen = f -> {
            for (Triple<String, ReentrantReadWriteLock.ReadLock, RandomAccessFile> triple : triples) {
                if (triple.getRight() != null) {
                    triple.getRight().close();
                }
            }
            StopWatchUtil.end(startTime, "Make a response suc for uri " + request.uri());
        };

        future.addListener(completeListen);
        if (!keepAlive) {
            future.addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error(cause.getMessage(), cause);
        if (ctx.channel().isActive()) {
            sendMessage(ctx, INTERNAL_SERVER_ERROR, cause.getMessage());
        }
    }

    private void sendMessage(ChannelHandlerContext ctx, HttpResponseStatus status, String message) {
        FullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1, status, Unpooled.copiedBuffer(message, CharsetUtil.UTF_8));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
        this.sendAndCleanupConnection(ctx, response);
    }


    /**
     * If Keep-Alive is disabled, attaches "Connection: close" header to the response
     * and closes the connection after the response being sent.
     */
    private void sendAndCleanupConnection(ChannelHandlerContext ctx, FullHttpResponse response) {
        final FullHttpRequest request = this.request;
        final boolean keepAlive = HttpUtil.isKeepAlive(request);
        HttpUtil.setContentLength(response, response.content().readableBytes());
        if (!keepAlive) {
            // We're going to close the connection as soon as the response is sent,
            // so we should also make it clear for the client.
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        } else if (request.protocolVersion().equals(HTTP_1_0)) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        }

        ChannelFuture flushPromise = ctx.writeAndFlush(response);

        if (!keepAlive) {
            // Close the connection as soon as the response is sent.
            flushPromise.addListener(ChannelFutureListener.CLOSE);
        }
    }
}