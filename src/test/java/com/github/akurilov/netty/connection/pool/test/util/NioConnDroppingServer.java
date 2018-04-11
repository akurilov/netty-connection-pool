package com.github.akurilov.netty.connection.pool.test.util;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class NioConnDroppingServer
implements Closeable {

	private final EventLoopGroup dispatchGroup;
	private final EventLoopGroup workerGroup;
	private final ChannelFuture bindFuture;
	private final AtomicLong reqCounter = new AtomicLong(0);

	public NioConnDroppingServer(final int port, final int dropEveryRequest)
	throws InterruptedException {
		dispatchGroup = new NioEventLoopGroup();
		workerGroup = new NioEventLoopGroup();
		final var bootstrap = new ServerBootstrap()
			.group(dispatchGroup, workerGroup)
			.channel(NioServerSocketChannel.class)
			.childHandler(
				new ChannelInitializer<SocketChannel>() {
					@Override
					public final void initChannel(final SocketChannel ch) {
						ch.pipeline().addLast(
							new SimpleChannelInboundHandler<>() {
								@Override
								protected final void channelRead0(
									final ChannelHandlerContext ctx, final Object msg
								) throws Exception {
									if(0 == reqCounter.incrementAndGet() % dropEveryRequest) {
										final var conn = ctx.channel();
										System.out.println("Dropping the connection " + conn);
										conn.close();
									}
								}
							}
						);
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

