package com.github.akurilov.netty.connection.pool.test.util;

import com.github.akurilov.netty.connection.pool.BasicMultiNodeConnPool;
import com.github.akurilov.netty.connection.pool.NonBlockingConnPool;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.pool.ChannelPoolHandler;

import java.util.concurrent.Semaphore;

/**
 Created by andrey on 12.05.17.
 */
public final class BasicMultiNodeConnPoolMock
extends BasicMultiNodeConnPool
implements NonBlockingConnPool {

	public BasicMultiNodeConnPoolMock(
		final Semaphore concurrencyThrottle, final String[] nodes, final Bootstrap bootstrap,
		final ChannelPoolHandler connPoolHandler, final int defaultPort,
		final int connFailSeqLenLimit
	) {
		super(
			concurrencyThrottle, nodes, bootstrap, connPoolHandler, defaultPort,
			connFailSeqLenLimit
		);
	}

	protected final Channel connect(final String addr) {
		final var c = new EmbeddedChannel();
		c.attr(ATTR_KEY_NODE).set(addr);
		return c;
	}
}
