package org.elasticsearch.http.netty.pipelining;

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

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.LastHttpContent;

import java.util.PriorityQueue;
import java.util.Queue;

/**
 * Implements HTTP pipelining ordering, ensuring that responses are completely served in the same order as their
 * corresponding requests.
 *
 * Based on https://github.com/typesafehub/netty-http-pipelining - which uses netty 3
 */
public class HttpPipeliningHandler extends ChannelDuplexHandler {

    public static final int INITIAL_EVENTS_HELD = 3;

    private final int maxEventsHeld;
    private final Queue<HttpPipelinedResponse> holdingQueue;

    private int sequence = 0;
    private int nextRequiredSequence = 0;


    /**
     * @param maxEventsHeld the maximum number of channel events that will be retained prior to aborting the channel
     *                      connection. This is required as events cannot queue up indefinitely; we would run out of
     *                      memory if this was the case.
     */
    public HttpPipeliningHandler(final int maxEventsHeld) {
        this.maxEventsHeld = maxEventsHeld;
        this.holdingQueue = new PriorityQueue<>(INITIAL_EVENTS_HELD);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof LastHttpContent) {
            super.channelRead(ctx, new HttpPipelinedRequest((LastHttpContent) msg, sequence++));
        }
    }


    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof HttpPipelinedResponse) {
            boolean channelShouldClose = false;

            synchronized (holdingQueue) {
                if (holdingQueue.size() < maxEventsHeld) {

                    final HttpPipelinedResponse currentEvent = (HttpPipelinedResponse) msg;
                    holdingQueue.add(currentEvent);

                    while (!holdingQueue.isEmpty()) {
                        final HttpPipelinedResponse queuedPipelinedResponse = holdingQueue.peek();

                        if (queuedPipelinedResponse.getSequenceId() != nextRequiredSequence) {
                            break;
                        }
                        holdingQueue.remove();
                        super.write(ctx, queuedPipelinedResponse.getResponse(), queuedPipelinedResponse.getPromise());
                        nextRequiredSequence++;
                    }
                } else {
                    channelShouldClose = true;
                }
            }

            if (channelShouldClose) {
                ctx.close();
            }
        } else {
            super.write(ctx, msg, promise);
        }
    }
}