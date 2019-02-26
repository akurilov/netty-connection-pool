module com.github.akurilov.netty.connection.pool {

	requires io.netty.common;
	requires io.netty.transport;
	requires java.logging;

	exports com.github.akurilov.netty.connection.pool;
	exports com.github.akurilov.netty.connection.pool.mock;
}
