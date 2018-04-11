module com.github.akurilov.netty.connection.pool {

	requires com.github.akurilov.commons;
	requires com.github.akurilov.concurrent;
	requires io.netty.common;
	requires io.netty.transport;
	requires java.base;
	requires java.logging;

	exports com.github.akurilov.netty.connection.pool;
}