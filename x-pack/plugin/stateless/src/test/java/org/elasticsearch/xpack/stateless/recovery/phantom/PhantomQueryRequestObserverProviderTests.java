/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.phantom;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.plugins.spi.SPIClassIterator;
import org.elasticsearch.search.ShardQueryRequestObserver;
import org.elasticsearch.search.ShardQueryRequestObserverProvider;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.StatelessPlugin;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class PhantomQueryRequestObserverProviderTests extends ESTestCase {

    private ShardQueryRequestObserver create(Settings nodeSettings) {
        final var plugin = mock(StatelessPlugin.class);
        when(plugin.getPhantomQuerySamplers()).thenReturn(mock(PhantomQuerySamplers.class));
        when(plugin.getPhantomQueriesSettings()).thenReturn(mock(PhantomQueriesSettings.class));
        return new PhantomQueryRequestObserverProvider(plugin).create(
            nodeSettings,
            mock(ThreadPool.class),
            mock(ClusterService.class),
            TelemetryProvider.NOOP
        );
    }

    private static Settings roles(DiscoveryNodeRole... roles) {
        return Settings.builder()
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), Stream.of(roles).map(DiscoveryNodeRole::roleName).toList())
            .build();
    }

    public void testSearchNodesSampleRequests() {
        assertThat(create(roles(DiscoveryNodeRole.SEARCH_ROLE)), instanceOf(PhantomQueryRequestObserver.class));
    }

    public void testOtherNodesDoNotObserveRequests() {
        assertThat(create(roles(DiscoveryNodeRole.INDEX_ROLE)), sameInstance(ShardQueryRequestObserver.NOOP));
        assertThat(create(roles(DiscoveryNodeRole.MASTER_ROLE)), sameInstance(ShardQueryRequestObserver.NOOP));
        assertThat(create(roles()), sameInstance(ShardQueryRequestObserver.NOOP));
    }

    public void testTheCollaboratorsAreTakenFromThePluginWhenTheProviderIsBuilt() {
        final var plugin = mock(StatelessPlugin.class);
        when(plugin.getPhantomQuerySamplers()).thenReturn(mock(PhantomQuerySamplers.class));
        when(plugin.getPhantomQueriesSettings()).thenReturn(mock(PhantomQueriesSettings.class));

        final var provider = new PhantomQueryRequestObserverProvider(plugin);

        verify(plugin).getPhantomQuerySamplers();
        verify(plugin).getPhantomQueriesSettings();
        verifyNoMoreInteractions(plugin);
        assertThat(
            provider.create(
                roles(DiscoveryNodeRole.SEARCH_ROLE),
                mock(ThreadPool.class),
                mock(ClusterService.class),
                TelemetryProvider.NOOP
            ),
            instanceOf(PhantomQueryRequestObserver.class)
        );
        verifyNoMoreInteractions(plugin);
    }

    public void testTheNoArgConstructorExistsOnlyForSpiValidation() {
        expectThrows(IllegalStateException.class, PhantomQueryRequestObserverProvider::new);
    }

    /** The plugins service builds extensions through a constructor that takes the plugin, next to the no-arg one. */
    public void testTheProviderCanBeBuiltFromThePlugin() {
        final List<Class<?>> parameterTypes = new ArrayList<>();
        for (Constructor<?> constructor : PhantomQueryRequestObserverProvider.class.getConstructors()) {
            parameterTypes.add(constructor.getParameterCount() == 0 ? Void.class : constructor.getParameterTypes()[0]);
        }
        assertThat(parameterTypes, containsInAnyOrder(Void.class, StatelessPlugin.class));
    }

    public void testTheProviderIsRegisteredAsAServiceProvider() {
        final List<Class<?>> providers = new ArrayList<>();
        final var iterator = SPIClassIterator.get(ShardQueryRequestObserverProvider.class, getClass().getClassLoader());
        while (iterator.hasNext()) {
            providers.add(iterator.next());
        }
        assertThat(providers, equalTo(List.of(PhantomQueryRequestObserverProvider.class)));
    }
}
