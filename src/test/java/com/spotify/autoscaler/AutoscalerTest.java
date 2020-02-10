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
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.MoreExecutors;
import com.spotify.autoscaler.algorithm.Algorithm;
import com.spotify.autoscaler.algorithm.CPUAlgorithm;
import com.spotify.autoscaler.algorithm.StorageAlgorithm;
import com.spotify.autoscaler.client.StackdriverClient;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.BigtableClusterBuilder;
import com.spotify.autoscaler.db.Database;
import com.spotify.autoscaler.db.ErrorCode;
import com.spotify.autoscaler.filters.AllowAllClusterFilter;
import com.spotify.autoscaler.filters.ClusterFilter;
import com.spotify.autoscaler.metric.AutoscalerMetrics;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class AutoscalerTest {

  @Mock private StackdriverClient stackDriverClient;

  @Mock private Database database;

  @Mock private AutoscalerMetrics autoscalerMetrics;

  @Mock private AutoscaleJob autoscaleJob;

  private final ExecutorService executorService = MoreExecutors.newDirectExecutorService();

  private List<Algorithm> algorithms = null;

  private final BigtableCluster cluster1 =
      new BigtableClusterBuilder()
          .projectId("project")
          .instanceId("instance1")
          .clusterId("cluster1")
          .cpuTarget(0.8)
          .maxNodes(500)
          .minNodes(5)
          .overloadStep(100)
          .storageTarget(0.7)
          .errorCode(Optional.of(ErrorCode.OK))
          .build();

  private final BigtableCluster cluster2 =
      new BigtableClusterBuilder()
          .projectId("project")
          .instanceId("instance2")
          .clusterId("cluster2")
          .cpuTarget(0.8)
          .maxNodes(500)
          .minNodes(5)
          .overloadStep(100)
          .storageTarget(0.7)
          .errorCode(Optional.of(ErrorCode.OK))
          .build();

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    algorithms = new ArrayList<>();
    this.algorithms.add(new CPUAlgorithm(stackDriverClient, autoscalerMetrics));
    this.algorithms.add(new StorageAlgorithm(stackDriverClient, autoscalerMetrics));
  }

  private Autoscaler getAutoscaler(final ClusterFilter cluster) {

    final Autoscaler autoscaler =
        spy(
            new Autoscaler(
                executorService,
                stackDriverClient,
                database,
                autoscalerMetrics,
                cluster,
                algorithms));
    when(autoscaler.makeAutoscaleJob(stackDriverClient, database, autoscalerMetrics, algorithms))
        .thenReturn(autoscaleJob);
    return autoscaler;
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
    verify(database).updateLastChecked(cluster1);
    verify(database).updateLastChecked(cluster2);

    // Clusters should be checked in order since the unit test uses DirectExecutor executorservice
    final InOrder inOrder = inOrder(autoscaleJob);
    inOrder.verify(autoscaleJob).run(eq(cluster1), any(), any());
    inOrder.verify(autoscaleJob).run(eq(cluster2), any(), any());

    verifyNoMoreInteractions(database);
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

    verify(autoscaleJob, never()).run(eq(cluster1), any(), any());
    verify(autoscaleJob).run(eq(cluster2), any(), any());
    verifyNoMoreInteractions(autoscaleJob);
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
    verify(database, never()).updateLastChecked(cluster1);
    verify(database).updateLastChecked(cluster2);
    verifyNoMoreInteractions(database);

    verify(autoscaleJob, never()).run(eq(cluster1), any(), any());
    verify(autoscaleJob).run(eq(cluster2), any(), any());
  }

  @Test
  public void testOneClusterThrowsException() throws IOException {
    // The main purpose of this test is to ensure that
    // in cluster fails, later clusters still finish.

    when(database.getCandidateClusters()).thenReturn(Arrays.asList(cluster1, cluster2));
    when(database.updateLastChecked(cluster1)).thenReturn(true).thenReturn(false);
    when(database.updateLastChecked(cluster2)).thenReturn(true).thenReturn(false);

    doThrow(new RuntimeException("cluster1 exception"))
        .when(autoscaleJob)
        .run(eq(cluster1), any(), any());

    final Autoscaler autoscaler = getAutoscaler(new AllowAllClusterFilter());
    autoscaler.run();

    verify(autoscaleJob).run(eq(cluster1), any(), any());
    verify(autoscaleJob).run(eq(cluster2), any(), any());

    verify(database)
        .increaseFailureCount(
            eq(cluster1), any(), contains("cluster1 exception"), eq(ErrorCode.AUTOSCALER_INTERNAL));
  }
}
