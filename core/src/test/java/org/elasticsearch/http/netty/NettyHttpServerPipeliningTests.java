/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.http.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.netty.pipelining.HttpPipelinedRequest;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.elasticsearch.http.netty.NettyHttpClient.returnHttpResponseBodies;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * This test just tests, if he pipelining works in general with out any connection the elasticsearch handler
 */
public class NettyHttpServerPipeliningTests extends ESTestCase {
    private NetworkService networkService;
    private ThreadPool threadPool;
    private MockBigArrays bigArrays;
    private CustomNettyHttpServerTransport httpServerTransport;

    @Before
    public void setup() throws Exception {
        networkService = new NetworkService(Settings.EMPTY);
        threadPool = new TestThreadPool("test");
        bigArrays = new MockBigArrays(Settings.EMPTY, new NoneCircuitBreakerService());
    }

    @After
    public void shutdown() throws Exception {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
        if (httpServerTransport != null) {
            httpServerTransport.close();
        }
    }

    public void testThatHttpPipeliningWorksWhenEnabled() throws Exception {
        Settings settings = Settings.builder()
                               .put("http.pipelining", true)
                               .put("http.port", "0")
                               .build();
        httpServerTransport = new CustomNettyHttpServerTransport(settings);
        httpServerTransport.start();
        InetSocketTransportAddress transportAddress = (InetSocketTransportAddress) randomFrom(httpServerTransport.boundAddress().boundAddresses());

        List<String> requests = Arrays.asList("/firstfast", "/slow?sleep=500", "/secondfast", "/slow?sleep=1000", "/thirdfast");
        try (NettyHttpClient nettyHttpClient = new NettyHttpClient()) {
            Collection<FullHttpResponse> responses = nettyHttpClient.get(transportAddress.address(), requests.toArray(new String[]{}));
            Collection<String> responseBodies = returnHttpResponseBodies(responses);
            assertThat(responseBodies, contains("/firstfast", "/slow?sleep=500", "/secondfast", "/slow?sleep=1000", "/thirdfast"));
        }
    }

    public void testThatHttpPipeliningCanBeDisabled() throws Exception {
        Settings settings = Settings.builder()
                                .put("http.pipelining", false)
                                .put("http.port", "0")
                                .build();
        httpServerTransport = new CustomNettyHttpServerTransport(settings);
        httpServerTransport.start();
        InetSocketTransportAddress transportAddress = (InetSocketTransportAddress) randomFrom(httpServerTransport.boundAddress().boundAddresses());

        List<String> requests = Arrays.asList("/slow?sleep=1000", "/firstfast", "/secondfast", "/thirdfast", "/slow?sleep=500");
        try (NettyHttpClient nettyHttpClient = new NettyHttpClient()) {
            Collection<FullHttpResponse> responses = nettyHttpClient.get(transportAddress.address(), requests.toArray(new String[]{}));
            List<String> responseBodies = new ArrayList<>(returnHttpResponseBodies(responses));
            // we cannot be sure about the order of the fast requests, but the slow ones should have to be last
            assertThat(responseBodies, hasSize(5));
            assertThat(responseBodies.get(3), is("/slow?sleep=500"));
            assertThat(responseBodies.get(4), is("/slow?sleep=1000"));
        }
    }

    class CustomNettyHttpServerTransport extends NettyHttpServerTransport {

        private final ExecutorService executorService;

        public CustomNettyHttpServerTransport(Settings settings) {
            super(settings, NettyHttpServerPipeliningTests.this.networkService,
                NettyHttpServerPipeliningTests.this.bigArrays, NettyHttpServerPipeliningTests.this.threadPool
            );
            this.executorService = Executors.newFixedThreadPool(5);
        }

        @Override
        public ChannelHandler configureServerChannelHandler() {
            return new CustomHttpChannelHandler(this, executorService, NettyHttpServerPipeliningTests.this.threadPool.getThreadContext());
        }

        @Override
        public HttpServerTransport stop() {
            executorService.shutdownNow();
            return super.stop();
        }
    }

    private class CustomHttpChannelHandler extends NettyHttpServerTransport.HttpChannelHandler {

        private final ExecutorService executorService;

        public CustomHttpChannelHandler(NettyHttpServerTransport transport, ExecutorService executorService, ThreadContext threadContext) {
            super(transport, randomBoolean(), threadContext);
            this.executorService = executorService;
        }

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            super.initChannel(ch);
            ch.pipeline().replace("handler", "handler", new PossiblySlowUpstreamHandler(executorService));
        }
    }

    class PossiblySlowUpstreamHandler extends SimpleChannelInboundHandler<Object> {

        private final ExecutorService executorService;

        public PossiblySlowUpstreamHandler(ExecutorService executorService) {
            this.executorService = executorService;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            executorService.submit(new PossiblySlowRunnable(ctx, msg));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
            logger.info("Caught exception", e.getCause());
            ctx.channel().close();
        }
    }

    class PossiblySlowRunnable implements Runnable {

        private ChannelHandlerContext ctx;
        private HttpPipelinedRequest pipelinedRequest;
        private FullHttpRequest fullHttpRequest;

        public PossiblySlowRunnable(ChannelHandlerContext ctx, Object object) {
            this.ctx = ctx;
            if (object instanceof HttpPipelinedRequest) {
                this.pipelinedRequest = (HttpPipelinedRequest) object;
            } else if (object instanceof FullHttpRequest) {
                this.fullHttpRequest = (FullHttpRequest) object;
            }
        }

        @Override
        public void run() {
            String uri;
            if (pipelinedRequest != null && pipelinedRequest.getRequest() instanceof FullHttpRequest) {
                uri = ((FullHttpRequest) pipelinedRequest.getRequest()).uri();
            } else {
                uri = fullHttpRequest.uri();
            }

            ByteBuf buffer = Unpooled.copiedBuffer(uri, StandardCharsets.UTF_8);

            DefaultFullHttpResponse httpResponse = new DefaultFullHttpResponse(HTTP_1_1, OK, buffer);
            httpResponse.headers().add(CONTENT_LENGTH, buffer.readableBytes());

            QueryStringDecoder decoder = new QueryStringDecoder(uri);

            final int timeout = uri.startsWith("/slow") && decoder.parameters().containsKey("sleep") ?
                    Integer.valueOf(decoder.parameters().get("sleep").get(0)) : 0;
            if (timeout > 0) {
                try {
                    Thread.sleep(timeout);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e1);
                }
            }

            if (pipelinedRequest!= null) {
                ctx.writeAndFlush(pipelinedRequest.createHttpResponse(httpResponse, ctx.channel().newPromise()));
            } else {
                ctx.writeAndFlush(httpResponse);
            }
        }
    }
}
