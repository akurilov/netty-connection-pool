package com.github.akurilov.netty.connection.pool.test;

import static com.github.akurilov.netty.connection.pool.NonBlockingConnPool.ATTR_KEY_NODE;

import com.github.akurilov.netty.connection.pool.NonBlockingConnPool;
import com.github.akurilov.netty.connection.pool.test.util.BasicMultiNodeConnPoolMock;
import com.github.akurilov.netty.connection.pool.test.util.DummyChannelPoolHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 Created by andrey on 12.05.17.
 */
@RunWith(Parameterized.class)
public class BasicMultiNodeConnPoolTest {

	private static final int TEST_STEP_TIME_SECONDS = 50;
	private static final int BATCH_SIZE = 0x1000;

	private int concurrencyLevel;
	private int nodeCount;
	private ConcurrentMap<String, LongAdder> nodeFreq = new ConcurrentHashMap<>();

	@Parameterized.Parameters
	public static Collection<Object[]> generateData() {
		return Arrays.asList(
			new Object[][] {
				{1, 1},
				{10, 1}, {10, 2}, {10, 5}, {10, 10},
				{100, 1}, {100, 2}, {100, 5}, {100, 10},
				{1000, 1}, {1000, 2}, {1000, 5}, {1000, 10}
			}
		);
	}

	public BasicMultiNodeConnPoolTest(final int concurrencyLevel, final int nodeCount) {
		this.concurrencyLevel = concurrencyLevel;
		this.nodeCount = nodeCount;
		final String[] nodes = new String[nodeCount];
		for(int i = 0; i < nodeCount; i ++) {
			nodes[i] = Integer.toString(i);
		}
		try(
			final NonBlockingConnPool connPool = new BasicMultiNodeConnPoolMock(
				new Semaphore(concurrencyLevel), nodes, new Bootstrap(),
				new DummyChannelPoolHandler(), 12345, 0
			)
		) {
			final int coreCount = Runtime.getRuntime().availableProcessors();
			final ExecutorService poolLoader = Executors.newFixedThreadPool(coreCount);
			for(int i = 0; i < coreCount; i ++) {
				poolLoader.submit(
					() -> {
						final Thread currThread = Thread.currentThread();
						final List<Channel> connBuff = new ArrayList<>(BATCH_SIZE);
						int j, k;
						Channel c;
						try {
							while(!currThread.isInterrupted()) {
								for(j = 0; j < BATCH_SIZE; j ++) {
									c = connPool.lease();
									if(c == null) {
										break;
									}
									nodeFreq
										.computeIfAbsent(
											c.attr(ATTR_KEY_NODE).get(), n -> new LongAdder()
										)
										.increment();
									connBuff.add(c);
								}
								for(k = 0; k < j; k ++) {
									connPool.release(connBuff.get(k));
								}
								connBuff.clear();
							}
						} catch(final Exception ignored) {
						}
					}
				);
			}
			poolLoader.shutdown();
			try {
				poolLoader.awaitTermination(TEST_STEP_TIME_SECONDS, TimeUnit.SECONDS);
			} catch(final InterruptedException e) {
				e.printStackTrace();
			}
			poolLoader.shutdownNow();
		} catch(final Throwable t) {
			t.printStackTrace(System.err);
		} finally {
			final long connCountSum = nodeFreq.values().stream().mapToLong(LongAdder::sum).sum();
			System.out.println(
				"concurrency = " + concurrencyLevel + ", nodes = " + nodeCount + " -> rate: " +
					connCountSum / TEST_STEP_TIME_SECONDS
			);
		}
	}

	@Test
	public void test() {
		if(nodeCount > 1) {
			final long connCountSum = nodeFreq.values().stream().mapToLong(LongAdder::sum).sum();
			final long avgConnCountPerNode = connCountSum / nodeCount;
			for(final String nodeAddr: nodeFreq.keySet()) {
				assertTrue(nodeFreq.get(nodeAddr).sum() > 0);
				assertEquals(
					"Node count: " + nodeCount + ", node: \"" + nodeAddr
						+ "\", expected connection count: " + avgConnCountPerNode + ", actual: "
						+ nodeFreq.get(nodeAddr).sum(),
					avgConnCountPerNode, nodeFreq.get(nodeAddr).sum(), 1.5 * avgConnCountPerNode
				);
			}
		} else {
			assertTrue(true);
		}
	}
}
