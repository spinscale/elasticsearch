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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.netty.pipelining.HttpPipelinedRequest;

/**
 *
 */
@ChannelHandler.Sharable
public class HttpRequestHandler extends SimpleChannelInboundHandler<Object> {

    private final NettyHttpServerTransport serverTransport;
    private final boolean httpPipeliningEnabled;
    private final boolean detailedErrorsEnabled;
    private final ThreadContext threadContext;

    public HttpRequestHandler(NettyHttpServerTransport serverTransport, boolean detailedErrorsEnabled, ThreadContext threadContext) {
        this.serverTransport = serverTransport;
        this.httpPipeliningEnabled = serverTransport.pipelining;
        this.detailedErrorsEnabled = detailedErrorsEnabled;
        this.threadContext = threadContext;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object message) throws Exception {
        FullHttpRequest request;
        HttpPipelinedRequest pipelinedRequest = null;
        if (this.httpPipeliningEnabled && message instanceof HttpPipelinedRequest) {
            pipelinedRequest = (HttpPipelinedRequest) message;
            request = (FullHttpRequest) pipelinedRequest.getRequest();
        } else {
            request = (FullHttpRequest) message;
        }

        threadContext.copyHeaders(request.headers());
        // the netty HTTP handling always copy over the buffer to its own buffer, either in NioWorker internally
        // when reading, or using a cumulation buffer
        NettyHttpRequest httpRequest = new NettyHttpRequest(request, ctx.channel());

        serverTransport.dispatchRequest(httpRequest, new NettyHttpChannel(serverTransport, httpRequest, pipelinedRequest,
                detailedErrorsEnabled));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable t) {
        serverTransport.exceptionCaught(ctx, t);
    }
}
