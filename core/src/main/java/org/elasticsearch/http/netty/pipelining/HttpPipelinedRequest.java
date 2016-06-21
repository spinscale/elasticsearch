package org.elasticsearch.http.netty.pipelining;

import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;

public class HttpPipelinedRequest {

    private final LastHttpContent request;
    private final int sequenceId;

    public HttpPipelinedRequest(LastHttpContent request, int sequenceId) {
        this.request = request;
        this.sequenceId = sequenceId;
    }

    public LastHttpContent getRequest() {
        return request;
    }

    public HttpPipelinedResponse createHttpResponse(HttpResponse response, ChannelPromise promise) {
        return new HttpPipelinedResponse(response, promise, sequenceId);
    }
}