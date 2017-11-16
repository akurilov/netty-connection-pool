package com.github.akurilov.netty.connection.pool;

import com.github.akurilov.netty.connection.pool.mock.DummyChannelPoolHandler;
import com.github.akurilov.netty.connection.pool.mock.EpollConnDroppingServer;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledDirectByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.pool.ChannelPoolHandler;

import io.netty.channel.socket.SocketChannel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.Closeable;
import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Created by andrey on 16.11.17.
 */
public class ConnDropTest {

	private static final int CONCURRENCY = 10;
	private static final String[] NODES = new String[] { "127.0.0.1" };
	private static final ChannelPoolHandler CPH = new DummyChannelPoolHandler();
	private static final int DEFAULT_PORT = 9020;
	private static final int CONN_ATTEMPTS_LIMIT = 0;
	private static final long TEST_TIME_MILLIS = 10000;
	private static final ByteBuf PAYLOAD = Unpooled.directBuffer(0x1000).writeZero(0x1000);

	Closeable serverMock;
	NonBlockingConnPool connPool;
	EventLoopGroup group;

	@Before
	public void setUp()
	throws Exception {

		serverMock = new EpollConnDroppingServer(DEFAULT_PORT, 10);

		final Semaphore concurrencyThrottle = new Semaphore(CONCURRENCY);
		group = new EpollEventLoopGroup();
		final Bootstrap bootstrap = new Bootstrap()
			.group(group)
			.channel(EpollSocketChannel.class)
			.handler(
				new ChannelInitializer<SocketChannel>() {
					@Override
					protected final void initChannel(final SocketChannel conn)
					throws Exception {
						conn.pipeline().addLast(
							new SimpleChannelInboundHandler<Object>() {
								@Override
								protected final void channelRead0(final ChannelHandlerContext ctx, final Object msg)
								throws Exception {
								}
							}
						);
					}
				}
			)
			.option(ChannelOption.SO_KEEPALIVE, true)
			.option(ChannelOption.SO_REUSEADDR, true)
			.option(ChannelOption.TCP_NODELAY, true);
		connPool = new BasicMultiNodeConnPool(
			CONCURRENCY, concurrencyThrottle, NODES, bootstrap, CPH, DEFAULT_PORT, CONN_ATTEMPTS_LIMIT
		);
	}

	@After
	public void tearDown()
	throws Exception {
		connPool.close();
		group.shutdownGracefully();
		serverMock.close();
	}

	@Test
	public void test()
	throws Exception {
		final long tsStart = System.currentTimeMillis();
		final LongAdder connCounter = new LongAdder();
		final ExecutorService executor = Executors.newFixedThreadPool(CONCURRENCY);
		for(int i = 0; i < CONCURRENCY; i ++) {
			executor.submit(
				() -> {
					long tsNow = tsStart;
					Channel conn;
					while(tsNow - tsStart < TEST_TIME_MILLIS) {
						try {
							conn = connPool.lease();
							conn.writeAndFlush(PAYLOAD.retain()).sync();
							connPool.release(conn);
							connCounter.increment();
						} catch(final ConnectException ignore) {
						} catch(final InterruptedException e) {
							break;
						}
						tsNow = System.currentTimeMillis();
					}
				}
			);
		}
		executor.shutdown();
		executor.awaitTermination(TEST_TIME_MILLIS, TimeUnit.MILLISECONDS);
		final List<Runnable> tasks = executor.shutdownNow();
		assertEquals(0, tasks.size());
		System.out.println(connCounter.sum());
	}
}
