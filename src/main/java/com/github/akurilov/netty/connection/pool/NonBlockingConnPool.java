package com.github.akurilov.netty.connection.pool;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

import java.io.Closeable;
import java.net.ConnectException;
import java.util.List;

/**
 Created by andrey on 23.01.17.
 */
public interface NonBlockingConnPool
extends Closeable {

	AttributeKey<String> ATTR_KEY_NODE = AttributeKey.valueOf("node");

	/**
	 Prepare the connections
	 @throws ConnectException if failed to connect
	 @throws IllegalArgumentException if count is less than 1
	 */
	void preConnect(final int count)
	throws ConnectException, IllegalArgumentException, InterruptedException;

	/**
	 Get the connection immediately (don't block) or null. The caller should decide whether to fail,
	 to sleep or to block if no connection is available at the moment.
	 @throws ConnectException if no connections are in the pool and was unable to create new connection
	 */
	Channel lease()
	throws ConnectException;
	
	/**
	 Get multiple connections immediately (don't block).
	 @param conns The output buffer to store the leased connections
	 @param maxCount The count limit
	 @return the actual count of the connections leased successfully, 0 if no connections available
	 @throws ConnectException if no connections are in the pool and was unable to create new connection
	 */
	int lease(final List<Channel> conns, final int maxCount)
	throws ConnectException;

	/** Release the connection back into the pool */
	void release(final Channel conn);
	
	/** Release the connections back into the pool */
	void release(final List<Channel> conns);
}
