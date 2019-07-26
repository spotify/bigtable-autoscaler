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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.bigtable.admin.v2.Cluster;
import com.google.bigtable.admin.v2.GetClusterRequest;
import com.google.cloud.bigtable.grpc.BigtableInstanceClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.spotify.autoscaler.client.StackdriverClientImpl;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.PostgresDatabase;
import com.spotify.autoscaler.db.PostgresDatabaseTest;
import com.spotify.autoscaler.metric.AutoscalerMetrics;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.junit.After;
import org.mockito.Mock;

public class AutoscaleJobITBase {

  @Mock BigtableSession bigtableSession;

  @Mock protected BigtableInstanceClient bigtableInstanceClient;

  @Mock StackdriverClientImpl stackdriverClient;

  @Mock AutoscalerMetrics autoscalerMetrics;

  PostgresDatabase db;

  public void setupMocksFor(final FakeBTCluster fakeBTCluster) throws IOException {
    initMocks(this);
    db = initDatabase(fakeBTCluster);
    when(bigtableSession.getInstanceAdminClient()).thenReturn(bigtableInstanceClient);

    when(bigtableInstanceClient.getCluster(any()))
        .thenAnswer(
            invocationOnMock -> {
              final GetClusterRequest getClusterReq = invocationOnMock.getArgument(0);
              if (getClusterReq != null) {
                return Cluster.newBuilder()
                    .setName(fakeBTCluster.getCluster().clusterName())
                    .setServeNodes(fakeBTCluster.getNumberOfNodes())
                    .build();
              } else {
                return Cluster.newBuilder().build();
              }
            });

    when(bigtableInstanceClient.updateCluster(any()))
        .thenAnswer(
            invocationOnMock -> {
              final Cluster cluster = invocationOnMock.getArgument(0);
              final int newSize = cluster.getServeNodes();
              fakeBTCluster.setNumberOfNodes(newSize);
              return null;
            });
    fakeBTCluster.setNumberOfNodes(100);
    AutoscaleJobTestMocks.setCurrentDiskUtilization(stackdriverClient, 0.00001);
  }

  @After
  public void tearDown() {
    db.getBigtableClusters()
        .forEach(
            cluster ->
                db.deleteBigtableCluster(
                    cluster.projectId(), cluster.instanceId(), cluster.clusterId()));
    db.close();
  }

  private PostgresDatabase initDatabase(final FakeBTCluster fakeBTCluster) {
    final PostgresDatabase database = PostgresDatabaseTest.getPostgresDatabase();
    final TimeSupplier timeSupplier = (TimeSupplier) fakeBTCluster.getTimeSource();
    timeSupplier.setTime(fakeBTCluster.getFirstValidMetricsInstant());
    final BigtableCluster cluster = fakeBTCluster.getCluster();
    database.deleteBigtableCluster(cluster.projectId(), cluster.instanceId(), cluster.clusterId());
    database.insertBigtableCluster(cluster);
    return database;
  }

  protected void testThroughTime(
      final FakeBTCluster fakeBTCluster,
      final TimeSupplier timeSupplier,
      final Duration period,
      final int repetition,
      final Supplier<Double> cpuSupplier,
      final Supplier<Double> diskUtilSupplier,
      final Consumer<Void> assertionImmediatelyAfterAutoscaleJob,
      final Consumer<Void> assertionAfterTime)
      throws IOException {

    Instant now = timeSupplier.get();
    for (int i = 0; i < repetition; ++i) {
      now = now.plus(period);
      timeSupplier.setTime(now);
      assertionAfterTime.accept(null);
      AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, cpuSupplier.get());
      AutoscaleJobTestMocks.setCurrentDiskUtilization(stackdriverClient, diskUtilSupplier.get());

      final AutoscaleJob job = new AutoscaleJob(stackdriverClient, db, autoscalerMetrics);
      job.run(fakeBTCluster.getCluster(), bigtableSession, timeSupplier);
      assertionImmediatelyAfterAutoscaleJob.accept(null);
    }
  }
}
