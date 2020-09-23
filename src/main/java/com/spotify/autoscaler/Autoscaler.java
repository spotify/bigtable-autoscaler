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
import com.spotify.autoscaler.algorithm.Algorithm;
import com.spotify.autoscaler.client.StackdriverClient;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.Database;
import com.spotify.autoscaler.db.ErrorCode;
import com.spotify.autoscaler.filters.ClusterFilter;
import com.spotify.autoscaler.metric.AutoscalerMetrics;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Autoscaler implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Autoscaler.class);
  private static final int SHORT_TIMEOUT = (int) Duration.ofSeconds(10).toMillis();
  private static final int LONG_TIMEOUT = (int) Duration.ofSeconds(60).toMillis();
  private static final boolean USE_TIMEOUT = true;

  private final StackdriverClient stackDriverClient;
  private final Database database;
  private final AutoscalerMetrics autoscalerMetrics;
  private final ClusterFilter filter;
  private List<Algorithm> algorithms;
  private final ExecutorService executorService;

  @Inject
  public Autoscaler(
      final ExecutorService executorService,
      final StackdriverClient stackDriverClient,
      final Database database,
      final AutoscalerMetrics autoscalerMetrics,
      final ClusterFilter filter,
      final List<Algorithm> algorithms) {
    this.executorService = checkNotNull(executorService);
    this.stackDriverClient = stackDriverClient;
    this.database = checkNotNull(database);
    this.autoscalerMetrics = checkNotNull(autoscalerMetrics);
    this.filter = checkNotNull(filter);
    this.algorithms = algorithms;
  }

  public AutoscaleJob makeAutoscaleJob(
      final StackdriverClient stackDriverClient,
      final Database database,
      final AutoscalerMetrics autoscalerMetrics,
      final List<Algorithm> algorithms) {
    return new AutoscaleJob(stackDriverClient, database, autoscalerMetrics, algorithms);
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
    final BigtableCluster[] clusters =
        database
            .getCandidateClusters()
            .stream()
            // Order here is important - don't call updateLastChecked if a cluster is filtered.
            // That could lead to cluster starvation
            .filter(filter::match)
            .filter(database::updateLastChecked)
            .toArray(BigtableCluster[]::new);

    Map<String, List<BigtableCluster>> instanceMap = new HashMap<>();
    for (BigtableCluster cluster : clusters) {
        String instanceId = String.format("%s:%s", cluster.projectId(), cluster.instanceId());
        if (!instanceMap.containsKey(instanceId)) {
            instanceMap.put(instanceId, new ArrayList<>());
        }
        instanceMap.get(instanceId).add(cluster);
    }
    final CompletableFuture[] futures = instanceMap.values().stream()
            .map(
                clusterList ->
                    CompletableFuture.runAsync(() -> runForInstance(clusterList), executorService))
            .toArray(CompletableFuture[]::new);

    CompletableFuture.allOf(futures).join();
  }

  private void runForCluster(final BigtableCluster cluster) {
    LoggerContext.pushContext(cluster);
    LOGGER.info("Autoscaling cluster!");
    try (final BigtableSession session = createSession(cluster.instanceId(), cluster.projectId())) {
      makeAutoscaleJob(stackDriverClient, database, autoscalerMetrics, algorithms)
          .run(cluster, session, Instant::now);
    } catch (final Exception e) {
      final ErrorCode errorCode = ErrorCode.fromException(Optional.of(e));
      LOGGER.error("Failed to autoscale cluster!", e);
      database.increaseFailureCount(cluster, Instant.now(), e.toString(), errorCode);
    }
    LoggerContext.clearContext();
  }

  private void runForInstance(final List<BigtableCluster> clusters) {
    BigtableCluster cluster = clusters.get(0);
    LoggerContext.pushContext(cluster);
    LOGGER.info("Autoscaling cluster!");
    try (final BigtableSession session = createSession(cluster.instanceId(), cluster.projectId())) {
      makeAutoscaleJob(stackDriverClient, database, autoscalerMetrics, algorithms)
          .runInstance(clusters, session, Instant::now);
    } catch (final Exception e) {
      final ErrorCode errorCode = ErrorCode.fromException(Optional.of(e));
      LOGGER.error("Failed to autoscale instance!", e);
      database.increaseFailureCount(cluster, Instant.now(), e.toString(), errorCode);
    }
    LoggerContext.clearContext();
  }

  private static BigtableSession createSession(final String instanceId, final String projectId)
      throws IOException {
    final BigtableInstanceName bigtableInstanceName =
        new BigtableInstanceName(projectId, instanceId);

    final BigtableOptions options =
        BigtableOptions.builder()
            .setDataChannelCount(64)
            .setProjectId(projectId)
            .setInstanceId(bigtableInstanceName.getInstanceId())
            .setUserAgent(Application.SERVICE_NAME)
            .setCallOptionsConfig(
                CallOptionsConfig.builder()
                    .setUseTimeout(true)
                    .setShortRpcTimeoutMs(SHORT_TIMEOUT)
                    .setMutateRpcTimeoutMs(LONG_TIMEOUT)
                    .setReadRowsRpcTimeoutMs(LONG_TIMEOUT)
                    .build())
            .setBulkOptions(
                BulkOptions.builder()
                    .setMaxInflightRpcs(1000000)
                    .setMaxMemory(Long.MAX_VALUE)
                    .build())
            .build();

    return new BigtableSession(options);
  }

  public void close() throws Exception {
    database.close();
    stackDriverClient.close();
    executorService.shutdown();
  }
}
