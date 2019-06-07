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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.codahale.metrics.Meter;
import com.google.cloud.bigtable.grpc.BigtableInstanceClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.common.util.concurrent.MoreExecutors;
import com.spotify.autoscaler.client.StackdriverClient;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.BigtableClusterBuilder;
import com.spotify.autoscaler.db.Database;
import com.spotify.autoscaler.filters.AllowAllClusterFilter;
import com.spotify.autoscaler.filters.ClusterFilter;
import com.spotify.autoscaler.util.ErrorCode;
import com.spotify.metrics.core.SemanticMetricRegistry;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;

public class AutoscalerTest {

  @Mock private SemanticMetricRegistry registry;

  @Mock private StackdriverClient stackDriverClient;

  @Mock private BigtableSession bigtableSession;

  @Mock private BigtableInstanceClient bigtableInstanceClient;

  @Mock private Database database;

  @Mock private Autoscaler.SessionProvider sessionProvider;

  @Mock private ClusterStats clusterStats;

  @Mock private AutoscaleJobFactory autoscaleJobFactory;

  @Mock private AutoscaleJob autoscaleJob;

  ExecutorService executorService = MoreExecutors.newDirectExecutorService();

  BigtableCluster cluster1 =
      new BigtableClusterBuilder()
          .projectId("project")
          .instanceId("instance1")
          .clusterId("cluster1")
          .cpuTarget(0.8)
          .maxNodes(500)
          .minNodes(5)
          .overloadStep(100)
          .errorCode(Optional.of(ErrorCode.OK))
          .build();

  BigtableCluster cluster2 =
      new BigtableClusterBuilder()
          .projectId("project")
          .instanceId("instance2")
          .clusterId("cluster2")
          .cpuTarget(0.8)
          .maxNodes(500)
          .minNodes(5)
          .overloadStep(100)
          .errorCode(Optional.of(ErrorCode.OK))
          .build();

  private Autoscaler getAutoscaler(final ClusterFilter cluster) {
    return new Autoscaler(
        autoscaleJobFactory,
        executorService,
        registry,
        stackDriverClient,
        database,
        sessionProvider,
        clusterStats,
        cluster);
  }

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
  public void testTwoClustersFoundAndProcessed() throws IOException {
    // The main purpose of this test is to ensure that an Autoscale job can process multiple
    // clusters in the same invocation of Autoscaler.run()

    when(database.getCandidateClusters()).thenReturn(Arrays.asList(cluster1, cluster2));
    when(database.updateLastChecked(cluster1)).thenReturn(true).thenReturn(false);
    when(database.updateLastChecked(cluster2)).thenReturn(true).thenReturn(false);

    final Autoscaler autoscaler = getAutoscaler(new AllowAllClusterFilter());

    autoscaler.run();

    // Since each task will try to process [cluster1, cluster2], we will have multiple
    // calls to updateLastChecked for the same cluster, but most of them will return false
    // (as given above)
    verify(database).getCandidateClusters();
    verify(autoscaleJob, times(2)).run();
    verify(database).updateLastChecked(cluster1);
    verify(database).updateLastChecked(cluster2);

    // Clusters should be checked in order since the unit test uses DirectExecutor executorservice
    final InOrder inOrder = inOrder(autoscaleJobFactory);
    inOrder
        .verify(autoscaleJobFactory)
        .createAutoscaleJob(any(), any(), eq(cluster1), any(), any(), any(), any());
    inOrder
        .verify(autoscaleJobFactory)
        .createAutoscaleJob(any(), any(), eq(cluster2), any(), any(), any(), any());

    verifyNoMoreInteractions(database);
    verifyNoMoreInteractions(autoscaleJobFactory);
  }

  @Test
  public void testTwoClustersFoundOneProcessedOneTakenByAnotherHost() throws IOException {
    // The main purpose of this test is to ensure that an Autoscale job is only
    // created (and executed) for cluster1, since although cluster2 passed our filter,
    // another host "raced us first" and processed that cluster.

    when(database.getCandidateClusters()).thenReturn(Arrays.asList(cluster1, cluster2));
    when(database.updateLastChecked(cluster1))
        .thenReturn(false); // Simulate this cluster was "taken" by another host
    when(database.updateLastChecked(cluster2)).thenReturn(true).thenReturn(false);

    final Autoscaler autoscaler = getAutoscaler(new AllowAllClusterFilter());

    autoscaler.run();

    verify(autoscaleJob).run();
    verify(autoscaleJobFactory)
        .createAutoscaleJob(any(), any(), eq(cluster2), any(), any(), any(), any());
    verifyNoMoreInteractions(autoscaleJobFactory);
  }

  @Test
  public void testTwoClustersFoundOneProcessedOneFilteredOut() throws IOException {
    // The main purpose of this test is to ensure that
    // updateLastChecked is not run on a cluster that's filtered out

    when(database.getCandidateClusters()).thenReturn(Arrays.asList(cluster1, cluster2));
    when(database.updateLastChecked(cluster2)).thenReturn(true).thenReturn(false);

    final Autoscaler autoscaler = getAutoscaler(cluster -> cluster.clusterId().equals("cluster2"));

    autoscaler.run();

    verify(database).getCandidateClusters();
    verify(database).updateLastChecked(cluster2);

    verify(autoscaleJobFactory)
        .createAutoscaleJob(any(), any(), eq(cluster2), any(), any(), any(), any());
    verify(autoscaleJob).run();

    verifyNoMoreInteractions(database);
    verifyNoMoreInteractions(autoscaleJobFactory);
  }

  @Test
  public void testOneClusterThrowsException() throws IOException {
    // The main purpose of this test is to ensure that
    // in cluster fails, later clusters still finish.

    when(database.getCandidateClusters()).thenReturn(Arrays.asList(cluster1, cluster2));
    when(database.updateLastChecked(cluster1)).thenReturn(true).thenReturn(false);
    when(database.updateLastChecked(cluster2)).thenReturn(true).thenReturn(false);
    when(autoscaleJobFactory.createAutoscaleJob(
            any(), any(), eq(cluster1), any(), any(), any(), any()))
        .thenThrow(new RuntimeException("cluster1"));

    final Autoscaler autoscaler = getAutoscaler(new AllowAllClusterFilter());

    autoscaler.run();

    verify(autoscaleJobFactory)
        .createAutoscaleJob(any(), any(), eq(cluster1), any(), any(), any(), any());
    verify(autoscaleJobFactory)
        .createAutoscaleJob(any(), any(), eq(cluster2), any(), any(), any(), any());

    verifyNoMoreInteractions(autoscaleJobFactory);
  }
}
