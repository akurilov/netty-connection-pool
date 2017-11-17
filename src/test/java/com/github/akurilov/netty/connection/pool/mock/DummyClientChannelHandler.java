package com.github.akurilov.netty.connection.pool.mock;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class DummyClientChannelHandler
extends SimpleChannelInboundHandler<Object> {

	@Override
	protected final void channelRead0(final ChannelHandlerContext ctx, final Object msg)
	throws Exception {
	}

	@Override
	public final void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause)
	throws Exception {
		super.exceptionCaught(ctx, cause);
		cause.printStackTrace(System.err);
	}
}
