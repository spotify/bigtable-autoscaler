/*-
 * -\-\-
 * bigtable-autoscaler
 * --
 * Copyright (C) 2018 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.autoscaler;

import static com.google.api.client.util.Preconditions.checkNotNull;

import com.spotify.autoscaler.client.StackdriverClient;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.Database;
import com.spotify.autoscaler.filters.ClusterFilter;
import com.spotify.autoscaler.metric.AutoscalerMetrics;
import com.spotify.autoscaler.util.BigtableUtil;
import com.spotify.autoscaler.util.ErrorCode;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Autoscaler implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(Autoscaler.class);

  private final Database db;
  private final AutoscalerMetrics autoscalerMetrics;
  private final ClusterFilter filter;

  private final ExecutorService executorService;
  private final AutoscaleJob autoscaleJob;

  public Autoscaler(
      final AutoscaleJobFactory autoscaleJobFactory,
      final ExecutorService executorService,
      final StackdriverClient stackDriverClient,
      final Database database,
      final AutoscalerMetrics autoscalerMetrics,
      final ClusterFilter filter) {
    this.autoscaleJob =
        autoscaleJobFactory.createAutoscaleJob(
            () -> stackDriverClient, database, autoscalerMetrics);
    this.executorService = checkNotNull(executorService);
    this.db = checkNotNull(database);
    this.autoscalerMetrics = checkNotNull(autoscalerMetrics);
    this.filter = checkNotNull(filter);
  }

  @Override
  public void run() {
    /*
     * Without this horrible bit of horribleness,
     * any uncaught Exception would kill the whole autoscaler.
     */
    try {
      runUnsafe();
    } catch (final Exception t) {
      LOGGER.error("Unexpected Exception!", t);
    }
  }

  private void runUnsafe() {
    autoscalerMetrics.markHeartBeat();
    ConcurrentHashMap<String, Boolean> hasRun = new ConcurrentHashMap<>();
    final CompletableFuture[] futures =
        db.getCandidateClusters()
            .stream()
            // Order here is important - don't call updateLastChecked if a cluster is filtered.
            // That could lead to cluster starvation
            .filter(filter::match)
            .filter(db::updateLastChecked)
            .map(
                cluster ->
                    CompletableFuture.runAsync(
                        () -> runForCluster(cluster, hasRun), executorService))
            .toArray(CompletableFuture[]::new);

    CompletableFuture.allOf(futures).join();
  }

  private void runForCluster(
      final BigtableCluster cluster, final ConcurrentHashMap<String, Boolean> hasRun) {
    BigtableUtil.pushContext(cluster);
    LOGGER.info("Autoscaling cluster!");
    try {
      if (hasRun.putIfAbsent(cluster.clusterName(), true) == null) {
        autoscaleJob.run(
            cluster,
            BigtableUtil.createSession(cluster.instanceId(), cluster.projectId()),
            Instant::now);
      } else {
        throw new RuntimeException("An autoscale job should only be run once!");
      }
    } catch (final Exception e) {
      final ErrorCode errorCode = ErrorCode.fromException(Optional.of(e));
      LOGGER.error("Failed to autoscale cluster!", e);
      db.increaseFailureCount(
          cluster.projectId(),
          cluster.instanceId(),
          cluster.clusterId(),
          Instant.now(),
          e.toString(),
          errorCode);
    }
    BigtableUtil.clearContext();
  }

  public void close() {
    executorService.shutdown();
  }
}
