High-performance non-blocking, multi-endpoint connection pool

# Introduction

Currently Netty doesn't have a connection pool implementation which
would allow to get a connection to any endpoint from the specified set.
This connection pool implementation solves that problem using
round-robin endpoint selection for each connection leased from the pool.

Also, this connection pool implementation supports the batch mode
operation (leasing/releasing many connections at once).

# Usage

## Gradle

```groovy
compile group: 'com.github.akurilov', name: 'netty-connection-pool', version: '0.1.2'
```

## Code Example

```java

final int concurrencyLevel = 100;
final Semaphore concurrencyThrottle = new Semaphore(concurrencyLevel, true);

final Bootstrap bootstrap = new Bootstrap();
// configure the bootstrap instance here

// your custom channel pool handler
final ChannelPoolHandler cph = ...

final NonBlockingConnPool connPool = new BasicMultiNodeConnPool(
    concurrencyThrottle, storageNodeAddrs, bootstrap, cph,
    storageNodePort, connAttemptsLimit
);

// optional
connPool.preCreateConnections(concurrencyLevel);

// use the pool
final Channel conn = connPool.lease();
...
connPool.release(conn);

// don't forget to clean up
connPool.close();
```

## Batch Mode

```java
...

// try to get up to 4096 connections per time
final int maxConnCount = 0x1000;
final List<Channel> conns = new ArrayList<>(maxConnCount);
final int availConnCount = connPool.lease(conns, maxConnCount);

// use the obtained connections
Channel conn;
for(int i = 0; i < availConnCount; i ++) {
    conn = conns.get(i);
    // use the connection
}

// release all connections back into the pool
connPool.lease(conns);
```
