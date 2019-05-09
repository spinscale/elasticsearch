/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.core.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.core.watcher.actions.ActionStatus.AckStatus.State;
import org.elasticsearch.xpack.core.watcher.actions.ActionWrapper;
import org.elasticsearch.xpack.core.watcher.actions.ActionWrapperResult;
import org.elasticsearch.xpack.core.watcher.actions.ExecutableAction;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;
import org.elasticsearch.xpack.watcher.actions.logging.LoggingAction;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.condition.NeverCondition;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ActionWrapperTests extends ESTestCase {

    private ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
    private Watch watch = mock(Watch.class);
    @SuppressWarnings("unchecked")
    private ExecutableAction<Action> executableAction = mock(ExecutableAction.class);
    private ActionWrapper actionWrapper = new ActionWrapper("_action", null, NeverCondition.INSTANCE, null, executableAction, null);

    public void testThatUnmetActionConditionResetsAckStatus() throws Exception {
        WatchStatus watchStatus = new WatchStatus(now, Collections.singletonMap("_action", createActionStatus(State.ACKED)));
        when(watch.status()).thenReturn(watchStatus);

        ActionWrapperResult result = actionWrapper.execute(mockExecutionContent(watch));
        assertThat(result.condition().met(), is(false));
        assertThat(result.action().status(), is(Action.Result.Status.CONDITION_FAILED));
        assertThat(watch.status().actionStatus("_action").ackStatus().state(), is(State.AWAITS_SUCCESSFUL_EXECUTION));
    }

    public void testOtherActionsAreNotAffectedOnActionConditionReset() throws Exception {
        Map<String, ActionStatus> statusMap = new HashMap<>();
        statusMap.put("_action", createActionStatus(State.ACKED));
        State otherState = randomFrom(State.ACKABLE, State.AWAITS_SUCCESSFUL_EXECUTION);
        statusMap.put("other", createActionStatus(otherState));

        WatchStatus watchStatus = new WatchStatus(now, statusMap);
        when(watch.status()).thenReturn(watchStatus);

        actionWrapper.execute(mockExecutionContent(watch));
        assertThat(watch.status().actionStatus("other").ackStatus().state(), is(otherState));
    }

    public void testThatMultipleResultsCanBeReturned() throws Exception {
        ActionWrapper wrapper = new ActionWrapper("_action", null, InternalAlwaysCondition.INSTANCE, null, executableAction, "my_path");
        WatchExecutionContext ctx = mockExecutionContent(watch);
        Payload.Simple payload = new Payload.Simple(Map.of("my_path",
            List.of(
                Map.of("key", "first"),
                Map.of("key", "second"),
                Map.of("key", "third")
            )));
        when(ctx.payload()).thenReturn(payload);
        when(executableAction.logger()).thenReturn(logger);

        final Action.Result firstResult = new Action.Result.Failure("MY_TYPE", "first reason");
        final Payload firstPayload = new Payload.Simple(Map.of("key", "first"));
        when(executableAction.execute(eq("_action"), eq(ctx), eq(firstPayload))).thenReturn(firstResult);

        final Action.Result secondResult = new Action.Result.Failure("MY_TYPE", "second reason");
        final Payload secondPayload = new Payload.Simple(Map.of("key", "second"));
        when(executableAction.execute(eq("_action"), eq(ctx), eq(secondPayload))).thenReturn(secondResult);

        final Action.Result thirdResult = new Action.Result.Failure("MY_TYPE", "third reason");
        final Payload thirdPayload = new Payload.Simple(Map.of("key", "third"));
        when(executableAction.execute(eq("_action"), eq(ctx), eq(thirdPayload))).thenReturn(thirdResult);

        ActionWrapperResult result = wrapper.execute(ctx);
        assertThat(result.action().status(), is(Action.Result.Status.SUCCESS));
        // check that action toXContent contains all the results
        try (XContentBuilder builder = jsonBuilder()) {
            result.toXContent(builder, ToXContent.EMPTY_PARAMS);
            final String json = Strings.toString(builder);
            final Map<String, Object> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, true);
            assertThat(map, hasKey("actions"));
            assertThat(map.get("actions"), instanceOf(List.class));
            List<Map<String, Object>> actions = (List) map.get("actions");
            assertThat(actions, hasSize(3));
        }
    }

    public void testThatPathElementIsntInstanceOfMap() throws Exception {
        ActionWrapper wrapper = new ActionWrapper("_action", null, InternalAlwaysCondition.INSTANCE, null, executableAction, "my_path");
        WatchExecutionContext ctx = mockExecutionContent(watch);
        Payload.Simple payload = new Payload.Simple(Map.of("my_path", List.of("first", "second", "third")));
        when(ctx.payload()).thenReturn(payload);
        when(executableAction.logger()).thenReturn(logger);

        final Action.Result actionResult = new Action.Result.Failure("MY_TYPE", "first reason");
        final Payload actionPayload = new Payload.Simple(Map.of("key", "first"));
        when(executableAction.execute(eq("_action"), eq(ctx), eq(actionPayload))).thenReturn(actionResult);

        ActionWrapperResult result = wrapper.execute(ctx);
        assertThat(result.action().status(), is(Action.Result.Status.FAILURE));
        assertThat(result.action(), instanceOf(Action.Result.FailureWithException.class));
        Action.Result.FailureWithException failureWithException = (Action.Result.FailureWithException) result.action();
        assertThat(failureWithException.getException().getMessage(), is("item in foreach [my_path] object was not a map"));
    }

    public void testThatSpecifiedPathIsACollection() {
        ActionWrapper wrapper = new ActionWrapper("_action", null, InternalAlwaysCondition.INSTANCE, null, executableAction, "my_path");
        WatchExecutionContext ctx = mockExecutionContent(watch);
        Payload.Simple payload = new Payload.Simple(Map.of("my_path", "not a map"));
        when(ctx.payload()).thenReturn(payload);
        when(executableAction.logger()).thenReturn(logger);

        ActionWrapperResult result = wrapper.execute(ctx);
        assertThat(result.action().status(), is(Action.Result.Status.FAILURE));
        assertThat(result.action(), instanceOf(Action.Result.FailureWithException.class));
        Action.Result.FailureWithException failureWithException = (Action.Result.FailureWithException) result.action();
        assertThat(failureWithException.getException().getMessage(),
            is("specified foreach object was not a an array/collection: [my_path]"));
    }

    public void testEmptyCollection() {
        ActionWrapper wrapper = new ActionWrapper("_action", null, InternalAlwaysCondition.INSTANCE, null, executableAction, "my_path");
        WatchExecutionContext ctx = mockExecutionContent(watch);
        Payload.Simple payload = new Payload.Simple(Map.of("my_path", Collections.emptyList()));
        when(ctx.payload()).thenReturn(payload);
        when(executableAction.logger()).thenReturn(logger);

        ActionWrapperResult result = wrapper.execute(ctx);
        assertThat(result.action().status(), is(Action.Result.Status.FAILURE));
        assertThat(result.action(), instanceOf(Action.Result.FailureWithException.class));
        Action.Result.FailureWithException failureWithException = (Action.Result.FailureWithException) result.action();
        assertThat(failureWithException.getException().getMessage(),
            is("foreach object [my_path] was an empty list, could not run any action"));
    }

    public void testPartialFailure() throws Exception {
        ActionWrapper wrapper = new ActionWrapper("_action", null, InternalAlwaysCondition.INSTANCE, null, executableAction, "my_path");
        WatchExecutionContext ctx = mockExecutionContent(watch);
        Payload.Simple payload = new Payload.Simple(Map.of("my_path",
            List.of(
                Map.of("key", "first"),
                Map.of("key", "second")
            )));
        when(ctx.payload()).thenReturn(payload);
        when(executableAction.logger()).thenReturn(logger);

        final Action.Result firstResult = new LoggingAction.Result.Success("log_message");;
        final Payload firstPayload = new Payload.Simple(Map.of("key", "first"));
        when(executableAction.execute(eq("_action"), eq(ctx), eq(firstPayload))).thenReturn(firstResult);

        final Action.Result secondResult = new Action.Result.Failure("MY_TYPE", "second reason");
        final Payload secondPayload = new Payload.Simple(Map.of("key", "second"));
        when(executableAction.execute(eq("_action"), eq(ctx), eq(secondPayload))).thenReturn(secondResult);

        ActionWrapperResult result = wrapper.execute(ctx);
        assertThat(result.action().status(), is(Action.Result.Status.PARTIAL_FAILURE));
    }

    private WatchExecutionContext mockExecutionContent(Watch watch) {
        WatchExecutionContext ctx = mock(WatchExecutionContext.class);
        when(watch.id()).thenReturn("watchId");
        when(ctx.watch()).thenReturn(watch);
        when(ctx.skipThrottling(eq("_action"))).thenReturn(true);
        return ctx;
    }

    private ActionStatus createActionStatus(State state) {
        ActionStatus.AckStatus ackStatus = new ActionStatus.AckStatus(now, state);
        ActionStatus.Execution execution = ActionStatus.Execution.successful(now);
        return new ActionStatus(ackStatus, execution, execution, null);
    }
}
