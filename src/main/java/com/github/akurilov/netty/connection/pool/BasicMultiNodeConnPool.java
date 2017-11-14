package com.github.akurilov.netty.connection.pool;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.pool.ChannelPoolHandler;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

/**
 Created by andrey on 23.01.17.
 The simple multi-endpoint connection pool which is throttled externally by providing the semaphore.
 The provided semaphore limits the count of the simultaneously used connections.
 Based on netty.
 */
public class BasicMultiNodeConnPool
implements NonBlockingConnPool {

	private final static Logger LOG = Logger.getLogger(BasicMultiNodeConnPool.class.getName());

	private final Semaphore concurrencyThrottle;
	private final String nodes[];
	private final int n;
	private final int concurrencyLevel;
	private final int connAttemptsLimit;
	private final Map<String, Bootstrap> bootstraps;
	private final Map<String, List<Channel>> allConns;
	private final Map<String, Queue<Channel>> availableConns;
	private final Object2IntMap<String> connCounts;
	private final Object2IntMap<String> failedConnAttemptCounts;

	/**
	 *
	 * @param concurrencyLevel the maximum concurrency level (the maximum count of the connections used simultaneously)
	 * @param concurrencyThrottle the throttle for the concurrency level control
	 * @param nodes the array of the endpoint nodes, any element may contain the port (followed after ":") to override the defaultPort argument
	 * @param bootstrap Netty's bootstrap instance
	 * @param connPoolHandler channel pool handler instance being notified upon new connection is created
	 * @param defaultPort default port used to connect (any node address from the nodes set may override this)
	 * @param connAttemptsLimit the max count of the subsequent connection failures to the node before the node will be excluded from the pool, 0 means no limit
	 */
	public BasicMultiNodeConnPool(
		final int concurrencyLevel, final Semaphore concurrencyThrottle,  final String nodes[],
		final Bootstrap bootstrap, final ChannelPoolHandler connPoolHandler, final int defaultPort,
		final int connAttemptsLimit
	) {
		this.concurrencyLevel = concurrencyLevel;
		this.concurrencyThrottle = concurrencyThrottle;
		if(nodes.length == 0) {
			throw new IllegalArgumentException("Empty nodes array argument");
		}
		this.nodes = nodes;
		this.connAttemptsLimit = connAttemptsLimit;
		this.n = nodes.length;
		bootstraps = new HashMap<>(n);
		allConns = new HashMap<>(n);
		availableConns = new HashMap<>(n);
		connCounts = new Object2IntOpenHashMap<>(n);
		failedConnAttemptCounts = new Object2IntOpenHashMap<>(n);

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
						new ChannelInitializer<Channel>() {
							@Override
							protected final void initChannel(final Channel conn)
							throws Exception {
								assert conn.eventLoop().inEventLoop();
								connPoolHandler.channelCreated(conn);
							}
						}
					)
			);
			availableConns.put(node, new ConcurrentLinkedQueue<>());
			connCounts.put(node, 0);
			failedConnAttemptCounts.put(node, 0);
		}
	}

	@Override
	public void preCreateConnections()
	throws ConnectException {
		if(concurrencyLevel > 0) {
			for(int i = 0; i < concurrencyLevel; i ++) {
				final Channel conn = connectToAnyNode();
				if(conn == null) {
					throw new ConnectException("Failed to pre-create the connections to the target nodes");
				}
				final String nodeAddr = conn.attr(ATTR_KEY_NODE).get();
				if(conn.isActive()) {
					final Queue<Channel> connQueue = availableConns.get(nodeAddr);
					if(connQueue != null) {
						connQueue.add(conn);
					}
				} else {
					disconnect(nodeAddr, conn);
				}
			}
			LOG.info("Pre-created " + concurrencyLevel + " connections");
		}
	}

	private Channel connectToAnyNode()
	throws ConnectException {

		Channel conn = null;
		String selectedNodeAddr = null;

		synchronized(connCounts) {

			// select the endpoint node having the minimum count of established connections
			int minConnsCount = Integer.MAX_VALUE, nextConnsCount = 0;
			String nextNodeAddr;
			final int i = ThreadLocalRandom.current().nextInt(n);
			for(int j = i; j < n; j ++) {
				nextNodeAddr = nodes[j % n];
				nextConnsCount = connCounts.getInt(nextNodeAddr);
				if(nextConnsCount == 0) {
					selectedNodeAddr = nextNodeAddr;
					break;
				} else if(nextConnsCount < minConnsCount) {
					minConnsCount = nextConnsCount;
					selectedNodeAddr = nextNodeAddr;
				}
			}

			if(selectedNodeAddr != null) {
				// connect to the selected endpoint node
				LOG.fine("New connection to \"" + selectedNodeAddr + "\"");
				try {
					conn = connect(selectedNodeAddr);
					conn.closeFuture().addListener(
						(ChannelFutureListener) future -> {
							final String nodeAddr = future.channel().attr(ATTR_KEY_NODE).get();

							synchronized(connCounts) {
								connCounts.put(nodeAddr, connCounts.get(nodeAddr) - 1);
							}

							// concurrencyThrottle.release() can be called twice in case of close()
							// is called after a successful release()
							// so we need to keep track of available permits
							if(concurrencyThrottle.availablePermits() < concurrencyLevel) {
								concurrencyThrottle.release();
							}
						}
					);
					conn.attr(ATTR_KEY_NODE).set(selectedNodeAddr);
					allConns.computeIfAbsent(selectedNodeAddr, sna -> new ArrayList<>()).add(conn);
					connCounts.put(selectedNodeAddr, nextConnsCount + 1);
					if(connAttemptsLimit > 0) {
						// reset the connection failures counter if connected successfully
						failedConnAttemptCounts.put(selectedNodeAddr, 0);
					}
				} catch(final Exception e) {
					LOG.warning("Failed to create a new connection to " + selectedNodeAddr + ": " + e.toString());
					if(connAttemptsLimit > 0) {
						final int selectedNodeFailedConnAttemptsCount = failedConnAttemptCounts
							.getInt(selectedNodeAddr) + 1;
						failedConnAttemptCounts.put(
							selectedNodeAddr, selectedNodeFailedConnAttemptsCount
						);
						if(selectedNodeFailedConnAttemptsCount > connAttemptsLimit) {
							LOG.warning(
								"Failed to connect to the node \"" + selectedNodeAddr + "\" "
									+ selectedNodeFailedConnAttemptsCount + " times successively, "
									+ "excluding the node from the connection pool forever"
							);
							// the node having virtually Integer.MAX_VALUE established connections
							// will never be selected by the algorithm
							connCounts.put(selectedNodeAddr, Integer.MAX_VALUE);
							boolean allNodesExcluded = true;
							for(final String node : nodes) {
								if(connCounts.getInt(node) < Integer.MAX_VALUE) {
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
		}

		return conn;
	}

	protected Channel connect(final String addr)
	throws Exception {
		return bootstraps.get(addr).connect().sync().channel();
	}

	private void disconnect(final String addr, final Channel conn) {
		synchronized(connCounts) {
			connCounts.put(addr, connCounts.getInt(addr) - 1);
		}
		synchronized(allConns) {
			allConns.get(addr).remove(conn);
		}
		conn.close();
	}
	
	protected Channel poll() {
		final int i = ThreadLocalRandom.current().nextInt(n);
		Queue<Channel> connQueue;
		Channel conn;
		for(int j = i; j < i + n; j ++) {
			connQueue = availableConns.get(nodes[j % n]);
			conn = connQueue.poll();
			if(conn != null && conn.isActive()) {
				return conn;
			}
		}
		return null;
	}

	@Override
	public final Channel lease()
	throws ConnectException {
		Channel conn = null;
		if(concurrencyThrottle.tryAcquire()) {
			if(null == (conn = poll())) {
				conn = connectToAnyNode();
			}
			if(conn == null) {
				concurrencyThrottle.release();
				throw new ConnectException();
			}
		}
		return conn;
	}
	
	@Override
	public final int lease(final List<Channel> conns, final int maxCount)
	throws ConnectException {
		int availableCount = concurrencyThrottle.drainPermits();
		if(availableCount == 0) {
			return availableCount;
		}
		if(availableCount > maxCount) {
			concurrencyThrottle.release(availableCount - maxCount);
			availableCount = maxCount;
		}
		
		Channel conn;
		for(int i = 0; i < availableCount; i ++) {
			if(null == (conn = poll())) {
				conn = connectToAnyNode();
			}
			if(conn == null) {
				concurrencyThrottle.release(availableCount - i);
				throw new ConnectException();
			} else {
				conns.add(conn);
			}
		}
		return availableCount;
	}

	@Override
	public final void release(final Channel conn) {
		final String nodeAddr = conn.attr(ATTR_KEY_NODE).get();
		if(conn.isActive()) {
			final Queue<Channel> connQueue = availableConns.get(nodeAddr);
			if(connQueue != null) {
				connQueue.add(conn);
			}
			concurrencyThrottle.release();
		} else {
			disconnect(nodeAddr, conn);
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
				concurrencyThrottle.release();
			} else {
				disconnect(nodeAddr, conn);
			}
		}
	}

	@Override
	public void close()
	throws IOException {
		availableConns.clear();
		bootstraps.clear();
		int closedConnCount = 0;
		synchronized(allConns) {
			for(final String nodeAddr : allConns.keySet()) {
				for(final Channel conn : allConns.get(nodeAddr)) {
					conn.close();
					closedConnCount ++;
				}
			}
			allConns.clear();
		}
		synchronized(connCounts) {
			connCounts.clear();
		}
		LOG.fine("Closed all " +closedConnCount + " connections");
	}
}
