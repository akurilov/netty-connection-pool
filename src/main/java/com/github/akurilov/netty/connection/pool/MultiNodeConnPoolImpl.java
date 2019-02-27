package com.github.akurilov.netty.connection.pool;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import java.util.stream.IntStream;

/**
 * Created by andrey on 23.01.17.
 * The simple multi-endpoint connection pool which is throttled externally by providing the semaphore.
 * The provided semaphore limits the count of the simultaneously used connections.
 * Based on netty.
 */
public class MultiNodeConnPoolImpl
implements NonBlockingConnPool {

	private final static Logger LOG = Logger.getLogger(MultiNodeConnPoolImpl.class.getName());

	private final String nodes[];
	private final int n;
	private final int connAttemptsLimit;
	private final long connectTimeOut;
	private final TimeUnit connectTimeUnit;
	private final Map<String, Bootstrap> bootstraps;
	private final Map<String, Queue<Channel>> allConns;
	private final Map<String, Queue<Channel>> availableConns;
	private final Map<String, AtomicInteger> connCounts;
	private final Map<String, AtomicInteger> failedConnAttemptCounts;
	private final Lock closeLock = new ReentrantLock();

	/**
	 * @param nodes the array of the endpoint nodes, any element may contain the port (followed after ":") to override the defaultPort argument
	 * @param bootstrap Netty's bootstrap instance
	 * @param connPoolHandler channel pool handler instance being notified upon new connection is created
	 * @param defaultPort default port used to connect (any node address from the nodes set may override this)
	 * @param connAttemptsLimit the max count of the subsequent connection failures to the node before the node will be excluded from the pool, 0 means no limit
	 */
	public MultiNodeConnPoolImpl(
		final String nodes[], final Bootstrap bootstrap,
		final ChannelPoolHandler connPoolHandler, final int defaultPort, final int connAttemptsLimit,
		final long connectTimeOut, final TimeUnit connectTimeUnit
	) {
		if(nodes.length == 0) {
			throw new IllegalArgumentException("Empty nodes array argument");
		}
		this.nodes = nodes;
		this.connAttemptsLimit = connAttemptsLimit;
		this.connectTimeOut = connectTimeOut;
		this.connectTimeUnit = connectTimeUnit;
		this.n = nodes.length;
		bootstraps = new HashMap<>(n);
		allConns = new ConcurrentHashMap<>(n);
		availableConns = new ConcurrentHashMap<>(n);
		connCounts = new ConcurrentHashMap<>(n);
		failedConnAttemptCounts = new ConcurrentHashMap<>(n);

		for(final String node : nodes) {
			final InetSocketAddress nodeAddr;
			if(node.contains(":")) {
				final String addrParts[] = node.split(":");
				nodeAddr = new InetSocketAddress(addrParts[0], Integer.parseInt(addrParts[1]));
			} else {
				nodeAddr = new InetSocketAddress(node, defaultPort);
			}
			bootstraps.put(
				node,
				bootstrap
					.clone()
					.remoteAddress(nodeAddr)
					.handler(
						new ChannelInitializer<>() {
							@Override
							protected final void initChannel(final Channel conn)
							throws Exception {
								if(!conn.eventLoop().inEventLoop()) {
									throw new AssertionError();
								}
								connPoolHandler.channelCreated(conn);
							}
						}
					)
			);
			availableConns.put(node, new ConcurrentLinkedQueue<>());
			connCounts.put(node, new AtomicInteger(0));
			failedConnAttemptCounts.put(node, new AtomicInteger(0));
		}
	}

	@Override
	public void preConnect(final int count)
	throws ConnectException, IllegalArgumentException, InterruptedException {
		if(count > 0) {
			final CountDownLatch latch = new CountDownLatch(count);
			for(int i = 0; i < count; i ++) {
				final var node = nodes[i % nodes.length];
				bootstraps
					.get(node)
					.connect()
					.addListener(
						(ChannelFutureListener) future -> {
							try {
								final var conn = future.channel();
								conn.closeFuture().addListener(new CloseChannelListener(node, conn));
								conn.attr(ATTR_KEY_NODE).set(node);
								allConns.computeIfAbsent(node, na -> new ConcurrentLinkedQueue<>()).add(conn);
								synchronized(connCounts) {
									connCounts.get(node).incrementAndGet();
								}
								if(connAttemptsLimit > 0) {
									// reset the connection failures counter if connected successfully
									failedConnAttemptCounts.get(node).set(0);
								}
								LOG.fine("New connection to " + node + " created");
								if(conn.isActive()) {
									final Queue<Channel> connQueue = availableConns.get(node);
									if(connQueue != null) {
										connQueue.add(conn);
									}
								} else {
									conn.close();
								}
							} finally {
								latch.countDown();
							}
						}
					);
			}
			if(latch.await(connectTimeOut, connectTimeUnit)) {
				LOG.info("Pre-created " + count + " connections");
			} else {
				LOG.warning("Pre-created " + (count - latch.getCount()) + " connections");
			}
		} else {
			throw new IllegalArgumentException("Connection count should be > 0, but got " + count);
		}
	}

	private final class CloseChannelListener
	implements ChannelFutureListener {

		private final String nodeAddr;
		private final Channel conn;

		private CloseChannelListener(final String nodeAddr, final Channel conn) {
			this.nodeAddr = nodeAddr;
			this.conn = conn;
		}

		@Override
		public final void operationComplete(final ChannelFuture future)
		throws Exception {
			LOG.fine("Connection to " + nodeAddr + " closed");
			closeLock.lock();
			try {
				synchronized(connCounts) {
					if(connCounts.containsKey(nodeAddr)) {
						connCounts.get(nodeAddr).decrementAndGet();
					}
				}
				synchronized(allConns) {
					final Queue<Channel> nodeConns = allConns.get(nodeAddr);
					if(nodeConns != null) {
						nodeConns.remove(conn);
					}
				}
			} finally {
				closeLock.unlock();
			}
		}
	}

	private Channel connectToAnyNode()
	throws ConnectException {

		Channel conn = null;

		// select the endpoint node having the minimum count of established connections
		String nodeAddr = null;
		String nextNodeAddr;
		int minConnsCount = Integer.MAX_VALUE;
		int nextConnsCount = 0;
		final int i = ThreadLocalRandom.current().nextInt(n);
		for(int j = i; j < n; j ++) {
			nextNodeAddr = nodes[j % n];
			nextConnsCount = connCounts.get(nextNodeAddr).intValue();
			if(nextConnsCount == 0) {
				nodeAddr = nextNodeAddr;
				break;
			} else if(nextConnsCount < minConnsCount) {
				minConnsCount = nextConnsCount;
				nodeAddr = nextNodeAddr;
			}
		}

		if(nodeAddr != null) {
			// connect to the selected endpoint node
			LOG.fine("New connection to \"" + nodeAddr + "\"");
			try {
				conn = connect(nodeAddr);
			} catch(final Exception e) {
				LOG.warning("Failed to create a new connection to " + nodeAddr + ": " + e.toString());
				if(connAttemptsLimit > 0) {
					final var connAttempts = failedConnAttemptCounts.get(nodeAddr).incrementAndGet();
					if(connAttempts > connAttemptsLimit) {
						LOG.warning(
							"Failed to connect to the node \"" + nodeAddr + "\" " + connAttempts
								+ " times successively, excluding the node from the connection pool forever"
						);
						// the node having virtually Integer.MAX_VALUE established connections
						// will never be selected by the algorithm
						connCounts.get(nodeAddr).set(Integer.MAX_VALUE);
						boolean allNodesExcluded = true;
						for(final String node : nodes) {
							if(connCounts.get(node).get() < Integer.MAX_VALUE) {
								allNodesExcluded = false;
								break;
							}
						}
						if(allNodesExcluded) {
							LOG.severe("No endpoint nodes left in the connection pool!");
						}
					}
				}

				if(e instanceof ConnectException) {
					throw (ConnectException) e;
				} else {
					throw new ConnectException(e.getMessage());
				}
			}
		}

		if(conn != null) {
			conn.closeFuture().addListener(new CloseChannelListener(nodeAddr, conn));
			conn.attr(ATTR_KEY_NODE).set(nodeAddr);
			allConns.computeIfAbsent(nodeAddr, na -> new ConcurrentLinkedQueue<>()).add(conn);
			synchronized(connCounts) {
				connCounts.get(nodeAddr).incrementAndGet();
			}
			if(connAttemptsLimit > 0) {
				// reset the connection failures counter if connected successfully
				failedConnAttemptCounts.get(nodeAddr).set(0);
			}
			LOG.fine("New connection to " + nodeAddr + " created");
		}

		return conn;
	}

	protected Channel connect(final String addr)
	throws Exception {
		Channel conn = null;
		final Bootstrap bootstrap = bootstraps.get(addr);
		if(bootstrap != null) {
			final ChannelFuture connFuture = bootstrap.connect();
			if(connectTimeOut > 0) {
				if(connFuture.await(connectTimeOut, connectTimeUnit)) {
					conn = connFuture.channel();
				}
			} else {
				conn = connFuture.sync().channel();
			}
		}
		return conn;
	}

	protected Channel poll() {
		final int i = ThreadLocalRandom.current().nextInt(n);
		Queue<Channel> connQueue;
		Channel conn = null;
		for(int j = i; j < i + n; j ++) {
			connQueue = availableConns.get(nodes[j % n]);
			if(connQueue != null) {
				conn = connQueue.poll();
				if(conn != null && conn.isOpen()) {
					break;
				}
			}
		}
		return conn;
	}

	@Override
	public final Channel lease()
	throws ConnectException {
		Channel conn;
		if(null == (conn = poll())) {
			conn = connectToAnyNode();
		}
		if(conn == null) {
			throw new ConnectException();
		}
		return conn;
	}
	
	@Override
	public final int lease(final List<Channel> conns, final int count)
	throws ConnectException {
		Channel conn;
		for(int i = 0; i < count; i++) {
			if(null == (conn = poll())) {
				conn = connectToAnyNode();
			}
			if(conn == null) {
				throw new ConnectException();
			} else {
				conns.add(conn);
			}
		}
		return count;
	}

	@Override
	public final void release(final Channel conn) {
		final String nodeAddr = conn.attr(ATTR_KEY_NODE).get();
		if(conn.isActive()) {
			final Queue<Channel> connQueue = availableConns.get(nodeAddr);
			if(connQueue != null) {
				connQueue.add(conn);
			}
		} else {
			conn.close();
		}
	}
	
	@Override
	public final void release(final List<Channel> conns) {
		String nodeAddr;
		Queue<Channel> connQueue;
		for(final Channel conn : conns) {
			nodeAddr = conn.attr(ATTR_KEY_NODE).get();
			if(conn.isActive()) {
				connQueue = availableConns.get(nodeAddr);
				connQueue.add(conn);
			} else {
				conn.close();
			}
		}
	}

	@Override
	public void close()
	throws IOException {
		closeLock.lock();
		int closedConnCount = 0;
		try {
			for(final String nodeAddr: availableConns.keySet()) {
				for(final Channel conn: availableConns.get(nodeAddr)) {
					if(conn.isOpen()) {
						conn.close();
						closedConnCount ++;
					}
				}
			}
			availableConns.clear();
			for(final String nodeAddr: allConns.keySet()) {
				for(final Channel conn: allConns.get(nodeAddr)) {
					if(conn.isOpen()) {
						conn.close();
						closedConnCount ++;
					}
				}
			}
			allConns.clear();
			bootstraps.clear();
			connCounts.clear();
		} finally {
			closeLock.unlock();
		}
		LOG.fine("Closed " + closedConnCount + " connections");
	}
}
