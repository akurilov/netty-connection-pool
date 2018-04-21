package com.github.akurilov.netty.connection.pool.test;

import com.github.akurilov.netty.connection.pool.BasicMultiNodeConnPool;
import com.github.akurilov.netty.connection.pool.NonBlockingConnPool;
import com.github.akurilov.netty.connection.pool.test.util.DummyChannelPoolHandler;
import com.github.akurilov.netty.connection.pool.test.util.DummyClientChannelHandler;
import com.github.akurilov.netty.connection.pool.test.util.EpollConnDroppingServer;
import com.github.akurilov.netty.connection.pool.test.util.PortTools;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.socket.SocketChannel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertEquals;

public class EpollConnLeakTest {

	private static final int CONCURRENCY = 100;
	private static final String[] NODES = new String[] { "127.0.0.1" };
	private static final ChannelPoolHandler CPH = new DummyChannelPoolHandler();
	private static final int DEFAULT_PORT = 9876;
	private static final long TEST_TIME_SECONDS = 30;
	private static final int FAIL_EVERY_CONN_ATTEMPT = 0;
	private static final ByteBuf PAYLOAD = Unpooled.directBuffer(0x1000).writeZero(0x1000);

	Closeable serverMock;
	NonBlockingConnPool connPool;
	EventLoopGroup group;

	@Before
	public void setUp()
	throws Exception {

		serverMock = new EpollConnDroppingServer(DEFAULT_PORT, FAIL_EVERY_CONN_ATTEMPT);

		// create
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
						conn.pipeline().addLast(new DummyClientChannelHandler());
					}
				}
			)
			.option(ChannelOption.SO_KEEPALIVE, true)
			.option(ChannelOption.SO_REUSEADDR, true)
			.option(ChannelOption.TCP_NODELAY, true);
		connPool = new BasicMultiNodeConnPool(
			concurrencyThrottle, NODES, bootstrap, CPH, DEFAULT_PORT, 0
		);
		connPool.preCreateConnections(CONCURRENCY);

		// use
		final ExecutorService executor = Executors.newFixedThreadPool(CONCURRENCY);
		for(int i = 0; i < CONCURRENCY; i ++) {
			executor.submit(
				(Runnable) () -> {
					Channel conn;
					while(true) {
						try {
							conn = connPool.lease();
							if(conn == null) {
								LockSupport.parkNanos(1);
								continue;
							}
							try {
								conn.writeAndFlush(PAYLOAD.retain()).sync();
							} finally {
								connPool.release(conn);
							}
						} catch(final Throwable cause) {
							cause.printStackTrace(System.err);
							break;
						}
					}
				}
			);
		}
		TimeUnit.SECONDS.sleep(TEST_TIME_SECONDS);

		// close
		executor.shutdownNow();
		connPool.close();
		group.shutdownGracefully();
		serverMock.close();
		TimeUnit.SECONDS.sleep(1);
	}

	@After
	public void tearDown()
	throws Exception {
	}

	@Test
	public void testNoConnectionsAreAfterPoolClosed()
	throws Exception {
		final int actualConnCount = PortTools.getConnectionCount("127.0.0.1:" + DEFAULT_PORT);
		assertEquals(
			"Connection count should be equal to 0 after pool has been closed, but got "
				+ actualConnCount, 0, actualConnCount
		);
	}
}
