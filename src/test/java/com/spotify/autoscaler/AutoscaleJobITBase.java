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

import com.codahale.metrics.Meter;
import com.google.bigtable.admin.v2.Cluster;
import com.google.bigtable.admin.v2.GetClusterRequest;
import com.google.cloud.bigtable.grpc.BigtableInstanceClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.spotify.autoscaler.client.StackdriverClient;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.PostgresDatabase;
import com.spotify.autoscaler.db.PostgresDatabaseTest;
import com.spotify.metrics.core.SemanticMetricRegistry;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mock;

public class AutoscaleJobITBase {

  @Mock BigtableSession bigtableSession;

  @Mock BigtableInstanceClient bigtableInstanceClient;

  @Mock StackdriverClient stackdriverClient;

  @Mock SemanticMetricRegistry registry;

  @Mock ClusterStats clusterStats;

  PostgresDatabase db;

  final FakeBTCluster fakeBTCluster;

  public AutoscaleJobITBase(final FakeBTCluster fakeBTCluster) {
    this.fakeBTCluster = fakeBTCluster;
  }

  @Before
  public void setUp() throws IOException {
    initMocks(this);
    when(registry.meter(any())).thenReturn(new Meter());
    db = initDatabase();
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
              int newSize = cluster.getServeNodes();
              fakeBTCluster.setNumberOfNodes(newSize);
              return null;
            });
    fakeBTCluster.setNumberOfNodes(100);
    AutoscaleJobTestMocks.setCurrentDiskUtilization(stackdriverClient, 0.00001);
  }

  @After
  public void tearDown() {
    db.getBigtableClusters()
        .stream()
        .forEach(
            cluster ->
                db.deleteBigtableCluster(
                    cluster.projectId(), cluster.instanceId(), cluster.clusterId()));
    db.close();
  }

  private PostgresDatabase initDatabase() {
    final PostgresDatabase database = PostgresDatabaseTest.getPostgresDatabase();
    final BigtableCluster cluster = fakeBTCluster.getCluster();
    database.deleteBigtableCluster(cluster.projectId(), cluster.instanceId(), cluster.clusterId());
    database.insertBigtableCluster(cluster);
    return database;
  }

  void testThroughTime(
      final TimeSupplier timeSupplier,
      final Duration period,
      final int repetition,
      final Supplier<Double> cpuSupplier,
      final Supplier<Double> diskUtilSupplier,
      final Consumer<Void> assertion)
      throws IOException {

    Instant now = timeSupplier.get();
    for (int i = 0; i < repetition; ++i) {
      now = now.plus(period);
      timeSupplier.setTime(now);
      assertion.accept(null);
      AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, cpuSupplier.get());
      AutoscaleJobTestMocks.setCurrentDiskUtilization(stackdriverClient, diskUtilSupplier.get());

      final AutoscaleJob job =
          new AutoscaleJob(
              bigtableSession,
              stackdriverClient,
              fakeBTCluster.getCluster(),
              db,
              registry,
              clusterStats,
              timeSupplier);
      job.run();
      assertion.accept(null);
    }
  }
}
