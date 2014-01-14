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
package org.elasticsearch.rest.action.admin.indices.alias.get;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.Strings.isAllOrWildcard;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 */
@Deprecated
public class RestGetIndicesAliasesAction extends BaseRestHandler {

    @Inject
    public RestGetIndicesAliasesAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_aliases", this);
        controller.registerHandler(GET, "/{index}/_aliases", this);
        controller.registerHandler(GET, "/{index}/_aliases/{name}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final String[] aliasNamesFromParameter = Strings.splitStringByCommaToArray(request.param("name"));
        final String[] aliases = isAllOrWildcard(aliasNamesFromParameter) ? Strings.EMPTY_ARRAY : aliasNamesFromParameter;

        GetAliasesRequest aliasesRequest = new GetAliasesRequest().indices(indices).aliases(aliases).listenerThreaded(false);
        client.admin().indices().getAliases(aliasesRequest, new ActionListener<GetAliasesResponse>() {
            @Override
            public void onResponse(GetAliasesResponse getAliasesResponse) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject();

                    for (ObjectObjectCursor<String, List<AliasMetaData>> aliasCursor :  getAliasesResponse.getAliases()) {
                        builder.startObject(aliasCursor.key, XContentBuilder.FieldCaseConversion.NONE);
                        builder.startObject("aliases");

                        for (AliasMetaData aliasMetaData : aliasCursor.value) {
                            AliasMetaData.Builder.toXContent(aliasMetaData, builder, ToXContent.EMPTY_PARAMS);
                        }
                        builder.endObject();
                        builder.endObject();
                    }

                    builder.endObject();
                    channel.sendResponse(new XContentRestResponse(request, OK, builder));
                } catch (Throwable e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }

}