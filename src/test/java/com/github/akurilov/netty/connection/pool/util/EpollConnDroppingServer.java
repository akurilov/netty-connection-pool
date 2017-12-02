package com.github.akurilov.netty.connection.pool.util;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.SocketChannel;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by andrey on 17.11.17.
 */
public class EpollConnDroppingServer
implements Closeable {

	private final EventLoopGroup dispatchGroup;
	private final EventLoopGroup workerGroup;
	private final ChannelFuture bindFuture;
	private final AtomicLong reqCounter = new AtomicLong(0);

	public EpollConnDroppingServer(final int port, final int dropEveryRequest)
	throws InterruptedException {
		dispatchGroup = new EpollEventLoopGroup();
		workerGroup = new EpollEventLoopGroup();
		final ServerBootstrap bootstrap = new ServerBootstrap()
			.group(dispatchGroup, workerGroup)
			.channel(EpollServerSocketChannel.class)
			.childHandler(
				new ChannelInitializer<SocketChannel>() {
					@Override
					public final void initChannel(final SocketChannel ch) {
						if(dropEveryRequest > 0) {
							ch.pipeline().addLast(
								new SimpleChannelInboundHandler<Object>() {
									@Override
									protected final void channelRead0(
										final ChannelHandlerContext ctx, final Object msg
									) throws Exception {
										if(0 == reqCounter.incrementAndGet() % dropEveryRequest) {
											final Channel conn = ctx.channel();
											System.out.println("Dropping the connection " + conn);
											conn.close();
										}
									}
								}
							);
						}
					}
				}
			);

		bindFuture = bootstrap.bind(port).sync();
	}

	@Override
	public final void close()
	throws IOException {
		bindFuture.channel().close();
		workerGroup.shutdownGracefully();
		dispatchGroup.shutdownGracefully();
	}
}
