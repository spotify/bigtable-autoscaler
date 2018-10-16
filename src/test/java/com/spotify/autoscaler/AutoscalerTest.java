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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.codahale.metrics.Meter;
import com.google.cloud.bigtable.grpc.BigtableInstanceClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.common.util.concurrent.MoreExecutors;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.BigtableClusterBuilder;
import com.spotify.autoscaler.db.Database;
import com.spotify.autoscaler.filters.AllowAllClusterFilter;
import com.spotify.metrics.core.SemanticMetricRegistry;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class AutoscalerTest {

  @Mock
  private SemanticMetricRegistry registry;

  @Mock
  private BigtableSession bigtableSession;

  @Mock
  private BigtableInstanceClient bigtableInstanceClient;

  @Mock
  private Database database;

  @Mock
  private Autoscaler.SessionProvider sessionProvider;

  @Mock
  private ClusterStats clusterStats;

  @Mock
  private AutoscaleJobFactory autoscaleJobFactory;

  @Mock
  private AutoscaleJob autoscaleJob;

  ExecutorService executorService = MoreExecutors.newDirectExecutorService();

  BigtableCluster cluster1 = new BigtableClusterBuilder()
      .projectId("project").instanceId("instance1").clusterId("cluster1")
      .cpuTarget(0.8).maxNodes(500).minNodes(5).overloadStep(100).build();

  BigtableCluster cluster2 = new BigtableClusterBuilder()
      .projectId("project").instanceId("instance2").clusterId("cluster2")
      .cpuTarget(0.8).maxNodes(500).minNodes(5).overloadStep(100).build();

  @Before
  public void setUp() throws IOException {
    initMocks(this);
    when(registry.meter(any())).thenReturn(new Meter());
    when(sessionProvider.apply(any())).thenReturn(bigtableSession);
    when(bigtableSession.getInstanceAdminClient()).thenReturn(bigtableInstanceClient);
    when(autoscaleJobFactory.createAutoscaleJob(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(autoscaleJob);
  }

  @Test
  public void testJobsCreatedAndRun() {
    when(database.getCandidateCluster())
        .thenReturn(Optional.of(cluster1))
        .thenReturn(Optional.of(cluster2))
        .thenReturn(Optional.empty());

    Autoscaler autoscaler = new Autoscaler(
        autoscaleJobFactory, executorService, registry, database, sessionProvider, clusterStats,
        new AllowAllClusterFilter());

    autoscaler.run();

    verify(autoscaleJob, times(2)).run();
  }

  @Test
  public void testJobsFiltered() {
    when(database.getCandidateCluster())
        .thenReturn(Optional.of(cluster1))
        .thenReturn(Optional.of(cluster2))
        .thenReturn(Optional.empty());

    Autoscaler autoscaler = new Autoscaler(
        autoscaleJobFactory, executorService, registry, database, sessionProvider, clusterStats,
        cluster -> cluster.clusterId().equals("cluster1"));

    autoscaler.run();

    verify(autoscaleJob, times(1)).run();
  }
}
