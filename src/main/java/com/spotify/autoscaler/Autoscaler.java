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
import static com.spotify.autoscaler.Main.APP_PREFIX;

import com.google.cloud.bigtable.grpc.BigtableSession;
import com.spotify.autoscaler.client.StackdriverClient;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.Database;
import com.spotify.autoscaler.filters.ClusterFilter;
import com.spotify.autoscaler.util.BigtableUtil;
import com.spotify.metrics.core.SemanticMetricRegistry;
import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Autoscaler implements Runnable {

  public interface SessionProvider {
    BigtableSession apply(BigtableCluster in) throws IOException;
  }

  private static final Logger logger = LoggerFactory.getLogger(Autoscaler.class);
  static final int BATCH_SIZE = 10;

  private final SemanticMetricRegistry registry;
  private final Database db;
  private final ClusterStats clusterStats;
  private final ClusterFilter filter;

  private final SessionProvider sessionProvider;
  private final ExecutorService executorService;
  private final AutoscaleJobFactory autoscaleJobFactory;

  public Autoscaler(AutoscaleJobFactory autoscaleJobFactory,
                    ExecutorService executorService,
                    SemanticMetricRegistry registry,
                    Database db,
                    SessionProvider sessionProvider,
                    ClusterStats clusterStats,
                    ClusterFilter filter) {
    this.autoscaleJobFactory = checkNotNull(autoscaleJobFactory);
    this.executorService = checkNotNull(executorService);
    this.registry = checkNotNull(registry);
    this.db = checkNotNull(db);
    this.sessionProvider = checkNotNull(sessionProvider);
    this.clusterStats = checkNotNull(clusterStats);
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
    } catch (Exception t) {
      logger.error("Unexpected Exception!", t);
    }
  }

  private void runUnsafe() throws IOException {
    for (int i = 0; i < BATCH_SIZE; i++) {
      executorService.submit(() -> {
        try {
          db.getCandidateClusters()
              .stream()
              // Order here is important - don't call updateLastChecked if a cluster is filtered.
              // That could lead to cluster starvation
              .filter(filter::match)
              .filter(db::updateLastChecked)
              .findFirst()
              .flatMap(cluster -> {
            BigtableUtil.pushContext(cluster);
            logger.info("Autoscaling cluster!");
            try (BigtableSession session = sessionProvider.apply(cluster);
                 final AutoscaleJob job = autoscaleJobFactory.createAutoscaleJob(
                     session, new StackdriverClient(cluster), cluster, db, registry,
                     clusterStats, () -> Instant.now())) {
              job.run();
            } catch (Exception e) {
              logger.error("Failed to autoscale cluster!", e);
              db.increaseFailureCount(cluster.projectId(), cluster.instanceId(),
                  cluster.clusterId(), Instant.now(),
                  e.getMessage());
            }
            BigtableUtil.clearContext();

            return Optional.empty();
          });
        } catch (Exception e) {
          logger.error("Failed getting candidate cluster", e);
        }
      });
    }

    registry.meter(APP_PREFIX.tagged("what", "autoscale-heartbeat")).mark();
  }

  public void close() throws IOException {
    executorService.shutdown();
  }

}
