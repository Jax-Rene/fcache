package me.zhuangjy.core;

import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import lombok.extern.slf4j.Slf4j;
import me.zhuangjy.bean.CacheFile;
import me.zhuangjy.cache.CacheLoader;
import org.apache.commons.collections.MapUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_0;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;


/**
 * A simple handler that serves incoming HTTP requests to send their respective
 * HTTP responses.  It also implements {@code 'If-Modified-Since'} header to
 * take advantage of browser cache, as described in
 * <a href="http://tools.ietf.org/html/rfc2616#section-14.25">RFC 2616</a>.
 *
 * <h3>How Browser Caching Works</h3>
 * <p>
 * Web browser caching works with HTTP headers as illustrated by the following
 * sample:
 * <ol>
 * <li>Request #1 returns the content of {@code /file1.txt}.</li>
 * <li>Contents of {@code /file1.txt} is cached by the browser.</li>
 * <li>Request #2 for {@code /file1.txt} does not return the contents of the
 * file again. Rather, a 304 Not Modified is returned. This tells the
 * browser to use the contents stored in its cache.</li>
 * <li>The server knows the file has not been modified because the
 * {@code If-Modified-Since} date is the same as the file's last
 * modified date.</li>
 * </ol>
 *
 * <pre>
 * Request #1 Headers
 * ===================
 * GET /file1.txt HTTP/1.1
 *
 * Response #1 Headers
 * ===================
 * HTTP/1.1 200 OK
 * Date:               Tue, 01 Mar 2011 22:44:26 GMT
 * Last-Modified:      Wed, 30 Jun 2010 21:36:48 GMT
 * Expires:            Tue, 01 Mar 2012 22:44:26 GMT
 * Cache-Control:      private, max-age=31536000
 *
 * Request #2 Headers
 * ===================
 * GET /file1.txt HTTP/1.1
 * If-Modified-Since:  Wed, 30 Jun 2010 21:36:48 GMT
 *
 * Response #2 Headers
 * ===================
 * HTTP/1.1 304 Not Modified
 * Date:               Tue, 01 Mar 2011 22:44:28 GMT
 *
 * </pre>
 * <p>
 * 负责处理Http文件请求的Handler
 *
 * <h1>Http Cache机制</h1>
 * 1. Cache-Control: max-age=X sec
 * 设置客户端超时时间，客户端收到请求后，会记录该时间（now + max-age），若再次请求时时间早于该时间则不发起请求继续使用之前的缓存。
 * 2. If-None-Match
 * 客户端请求时会带上缓存的具体Md5值（可能细化到列的Md5值），Server检查若：
 * （1）所有Md5都不变，返回304状态码，不返回任何数据
 * （2）至少一个列Md5改变，返回200状态码，以及对应改变列的新数据
 */
@Slf4j
public class HttpStaticFileServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final int HTTP_CACHE_SECONDS = 60;
    private FullHttpRequest request;

    @Override
    public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
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

        // 返回修改过后的列
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
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
        ctx.write(response);

        for (String col : remainColMap.keySet()) {
            String startMark = "---###" + col;
            CacheFile colFile = columnMap.get(col);
            ReentrantReadWriteLock.ReadLock readLock = colFile.getLock().readLock();
            readLock.lock();

            try (RandomAccessFile raf = new RandomAccessFile(new File(colFile.getFilePath()), "r")) {
                long fileLength = raf.length();
                ctx.write(startMark);
                ctx.write(new DefaultFileRegion(raf.getChannel(), 0, fileLength));
            } catch (FileNotFoundException e) {
                sendMessage(ctx, NOT_FOUND, e.getMessage());
                return;
            } finally {
                readLock.unlock();
            }
        }
        ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
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