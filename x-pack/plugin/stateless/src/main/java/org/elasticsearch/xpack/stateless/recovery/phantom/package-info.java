/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/// Phantom queries: a recovering search shard copy replays a sample of its shard's recent queries before it starts, so that its first
/// real searches do not hit a cold cache.
///
/// A peer copy that already serves the shard keeps the sample, because it is the one that receives the shard's queries. The sample is
/// bounded per recovering copy and per node, since a peer may sample for many recovering copies at once and holds the requests on the
/// heap.
///
/// The recovering copy drives the transfer: it asks its peer to start sampling when its recovery starts and collects the sample once its
/// engine is open, see [PhantomQuerySampleRequester] and [PhantomQuerySamplingPeer].
package org.elasticsearch.xpack.stateless.recovery.phantom;
