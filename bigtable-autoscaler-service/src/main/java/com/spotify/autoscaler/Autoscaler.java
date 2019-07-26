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

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BulkOptions;
import com.google.cloud.bigtable.config.CallOptionsConfig;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.spotify.autoscaler.client.StackdriverClientImpl;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.Database;
import com.spotify.autoscaler.filters.ClusterFilter;
import com.spotify.autoscaler.metric.AutoscalerMetrics;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Autoscaler implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Autoscaler.class);

  private final StackdriverClientImpl stackDriverClient;
  private final Database database;
  private final AutoscalerMetrics autoscalerMetrics;
  private final ClusterFilter filter;

  private final ExecutorService executorService;

  public Autoscaler(
      final ExecutorService executorService,
      final StackdriverClientImpl stackDriverClient,
      final Database database,
      final AutoscalerMetrics autoscalerMetrics,
      final ClusterFilter filter) {
    this.executorService = checkNotNull(executorService);
    this.stackDriverClient = stackDriverClient;
    this.database = checkNotNull(database);
    this.autoscalerMetrics = checkNotNull(autoscalerMetrics);
    this.filter = checkNotNull(filter);
  }

  public AutoscaleJob makeAutoscaleJob(
      final StackdriverClientImpl stackDriverClient,
      final Database database,
      final AutoscalerMetrics autoscalerMetrics) {
    return new AutoscaleJob(stackDriverClient, database, autoscalerMetrics);
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
    final CompletableFuture[] futures =
        database
            .getCandidateClusters()
            .stream()
            // Order here is important - don't call updateLastChecked if a cluster is filtered.
            // That could lead to cluster starvation
            .filter(filter::match)
            .filter(database::updateLastChecked)
            .map(
                cluster ->
                    CompletableFuture.runAsync(() -> runForCluster(cluster), executorService))
            .toArray(CompletableFuture[]::new);

    CompletableFuture.allOf(futures).join();
  }

  private void runForCluster(final BigtableCluster cluster) {
    LoggerContext.pushContext(cluster);
    LOGGER.info("Autoscaling cluster!");
    try (final BigtableSession session =
        createSession(cluster.instanceId(), cluster.projectId(), Main.SERVICE_NAME)) {
      makeAutoscaleJob(stackDriverClient, database, autoscalerMetrics)
          .run(cluster, session, Instant::now);
    } catch (final Exception e) {
      final ErrorCode errorCode = ErrorCode.fromException(Optional.of(e));
      LOGGER.error("Failed to autoscale cluster!", e);
      database.increaseFailureCount(cluster, Instant.now(), e.toString(), errorCode);
    }
    LoggerContext.clearContext();
  }

  public void close() {
    executorService.shutdown();
  }

  private static final int SHORT_TIMEOUT = (int) Duration.ofSeconds(10).toMillis();
  private static final int LONG_TIMEOUT = (int) Duration.ofSeconds(60).toMillis();
  private static final boolean USE_TIMEOUT = true;

  private static BigtableSession createSession(
      final String instanceId, final String projectId, final String serviceName) {
    final BigtableInstanceName bigtableInstanceName =
        new BigtableInstanceName(projectId, instanceId);

    final BigtableOptions options =
        new BigtableOptions.Builder()
            .setDataChannelCount(64)
            .setProjectId(projectId)
            .setInstanceId(bigtableInstanceName.getInstanceId())
            .setUserAgent(serviceName)
            .setCallOptionsConfig(new CallOptionsConfig(USE_TIMEOUT, SHORT_TIMEOUT, LONG_TIMEOUT))
            .setBulkOptions(
                new BulkOptions.Builder()
                    .setMaxInflightRpcs(1000000)
                    .setMaxMemory(Long.MAX_VALUE)
                    .build())
            .build();

    try {
      return new BigtableSession(options);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
