package org.mikezhu.zchain.net;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.DefaultEventExecutorChooserFactory;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.RejectedExecutionHandlers;
import io.netty.util.concurrent.ThreadPerTaskExecutor;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import java.net.InetSocketAddress;
import java.nio.channels.spi.SelectorProvider;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A factory for building Netty {@link Channel}s. Channels here are setup with a pipeline to participate
 * in the internode protocol handshake, either the inbound or outbound side as per the method invoked.
 */
public final class SocketFactory {
    private static final Logger logger = LoggerFactory.getLogger(SocketFactory.class);

    private static final int EVENT_THREADS = Runtime.getRuntime().availableProcessors();

    /**
     * TODO:
     * The default task queue used by {@code NioEventLoop} and {@code EpollEventLoop} is {@code MpscUnboundedArrayQueue},
     * provided by JCTools. While efficient, it has an undesirable quality for a queue backing an event loop: it is
     * not non-blocking, and can cause the event loop to busy-spin while waiting for a partially completed task
     * offer, if the producer thread has been suspended mid-offer.
     */
    enum Provider {
        NIO {
            @Override
            NioEventLoopGroup makeEventLoopGroup(int threadCount, ThreadFactory threadFactory) {
                return new NioEventLoopGroup(threadCount,
                        new ThreadPerTaskExecutor(threadFactory),
                        DefaultEventExecutorChooserFactory.INSTANCE,
                        SelectorProvider.provider(),
                        DefaultSelectStrategyFactory.INSTANCE,
                        RejectedExecutionHandlers.reject());
            }

            @Override
            ChannelFactory<NioSocketChannel> clientChannelFactory() {
                return NioSocketChannel::new;
            }

            @Override
            ChannelFactory<NioServerSocketChannel> serverChannelFactory() {
                return NioServerSocketChannel::new;
            }
        },
        EPOLL {
            @Override
            EpollEventLoopGroup makeEventLoopGroup(int threadCount, ThreadFactory threadFactory) {
                return new EpollEventLoopGroup(threadCount,
                        new ThreadPerTaskExecutor(threadFactory),
                        DefaultEventExecutorChooserFactory.INSTANCE,
                        DefaultSelectStrategyFactory.INSTANCE,
                        RejectedExecutionHandlers.reject());
            }

            @Override
            ChannelFactory<EpollSocketChannel> clientChannelFactory() {
                return EpollSocketChannel::new;
            }

            @Override
            ChannelFactory<EpollServerSocketChannel> serverChannelFactory() {
                return EpollServerSocketChannel::new;
            }
        };

        EventLoopGroup makeEventLoopGroup(int threadCount, String threadNamePrefix) {
            logger.debug("using netty {} event loop for pool prefix {}", name(), threadNamePrefix);
            return makeEventLoopGroup(threadCount, new DefaultThreadFactory(threadNamePrefix, true));
        }

        abstract EventLoopGroup makeEventLoopGroup(int threadCount, ThreadFactory threadFactory);

        abstract ChannelFactory<? extends Channel> clientChannelFactory();

        abstract ChannelFactory<? extends ServerChannel> serverChannelFactory();

        static Provider optimalProvider() {
            ;
            return Epoll.isAvailable() && System.getProperty("os.name").toLowerCase().contains("linux") ? EPOLL : NIO;
        }
    }

    /**
     * a useful addition for debugging; simply set to true to get more data in your logs
     */
    static final boolean WIRETRACE = false;

    static {
        if (WIRETRACE)
            InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);
    }

    private final Provider provider;
    private final EventLoopGroup acceptGroup;
    private final EventLoopGroup defaultGroup;
    // we need a separate EventLoopGroup for outbound streaming because sendFile is blocking
    private final EventLoopGroup outboundStreamingGroup;
    final ExecutorService synchronousWorkExecutor = Executors.newCachedThreadPool(new DefaultThreadFactory("Messaging-SynchronousWork"));

    SocketFactory() {
        this(Provider.optimalProvider());
    }

    SocketFactory(Provider provider) {
        this.provider = provider;
        this.acceptGroup = provider.makeEventLoopGroup(1, "Messaging-AcceptLoop");
        this.defaultGroup = provider.makeEventLoopGroup(EVENT_THREADS, "Messaging-EventLoop");
        this.outboundStreamingGroup = provider.makeEventLoopGroup(EVENT_THREADS, "Streaming-EventLoop");
    }

    Bootstrap newClientBootstrap(EventLoop eventLoop, int tcpUserTimeoutInMS) {
        if (eventLoop == null)
            throw new IllegalArgumentException("must provide eventLoop");

        Bootstrap bootstrap = new Bootstrap().group(eventLoop).channelFactory(provider.clientChannelFactory());

        if (provider == Provider.EPOLL)
            bootstrap.option(EpollChannelOption.TCP_USER_TIMEOUT, tcpUserTimeoutInMS);

        return bootstrap;
    }

    ServerBootstrap newServerBootstrap() {
        return new ServerBootstrap().group(acceptGroup, defaultGroup).channelFactory(provider.serverChannelFactory());
    }

    /**
     * Creates a new {@link SslHandler} from provided SslContext.
     *
     * @param peer enables endpoint verification for remote address when not null
     */
    static SslHandler newSslHandler(Channel channel, SslContext sslContext, @Nullable InetSocketAddress peer) {
        if (peer == null)
            return sslContext.newHandler(channel.alloc());

        logger.debug("Creating SSL handler for {}:{}", peer.getHostString(), peer.getPort());
        SslHandler sslHandler = sslContext.newHandler(channel.alloc(), peer.getHostString(), peer.getPort());
        SSLEngine engine = sslHandler.engine();
        SSLParameters sslParameters = engine.getSSLParameters();
        sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
        engine.setSSLParameters(sslParameters);
        return sslHandler;
    }

    EventLoopGroup defaultGroup() {
        return defaultGroup;
    }

    public EventLoopGroup outboundStreamingGroup() {
        return outboundStreamingGroup;
    }

    public void shutdownNow() {
        acceptGroup.shutdownGracefully(0, 2, SECONDS);
        defaultGroup.shutdownGracefully(0, 2, SECONDS);
        outboundStreamingGroup.shutdownGracefully(0, 2, SECONDS);
        synchronousWorkExecutor.shutdownNow();
    }
}
