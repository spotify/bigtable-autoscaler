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

package cucumber.steps;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.codahale.metrics.Meter;
import com.google.bigtable.admin.v2.Cluster;
import com.google.cloud.bigtable.grpc.BigtableInstanceClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.spotify.autoscaler.AutoscaleJob;
import com.spotify.autoscaler.AutoscaleJobTestMocks;
import com.spotify.autoscaler.ClusterStats;
import com.spotify.autoscaler.client.StackdriverClient;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.BigtableClusterBuilder;
import com.spotify.autoscaler.db.PostgresDatabase;
import com.spotify.autoscaler.db.PostgresDatabaseTest;
import com.spotify.autoscaler.util.ErrorCode;
import com.spotify.metrics.core.SemanticMetricRegistry;
import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class AutoscaleJobSteps {
  @Mock BigtableSession bigtableSession;

  @Mock BigtableInstanceClient bigtableInstanceClient;

  @Mock StackdriverClient stackdriverClient;

  @Mock SemanticMetricRegistry registry;

  @Mock ClusterStats clusterStats;

  PostgresDatabase db;
  AutoscaleJob job;
  BigtableCluster cluster;
  int newSize;

  @Before
  public void setUp() throws IOException {
    initMocks(this);
    initialSetup();
    cluster = getTestCluster();
    db = initDatabase(cluster, registry);
    job = getTestJob();
  }

  private void initialSetup() throws IOException {
    when(registry.meter(any())).thenReturn(new Meter());
    when(bigtableSession.getInstanceAdminClient()).thenReturn(bigtableInstanceClient);
    AutoscaleJobTestMocks.setCurrentDiskUtilization(stackdriverClient, 0.00001);
    when(bigtableInstanceClient.updateCluster(any()))
        .thenAnswer(
            invocationOnMock -> {
              newSize = ((Cluster) invocationOnMock.getArgument(0)).getServeNodes();
              AutoscaleJobTestMocks.setCurrentSize(bigtableInstanceClient, newSize);
              return null;
            });
  }

  private static PostgresDatabase initDatabase(
      BigtableCluster cluster, SemanticMetricRegistry registry) {
    PostgresDatabase database = PostgresDatabaseTest.getPostgresDatabase();
    database.deleteBigtableCluster(cluster.projectId(), cluster.instanceId(), cluster.clusterId());
    database.insertBigtableCluster(cluster);
    return database;
  }

  private AutoscaleJob getTestJob() {
    return new AutoscaleJob(
        bigtableSession,
        stackdriverClient,
        cluster,
        db,
        registry,
        clusterStats,
        () -> Instant.now());
  }

  private BigtableCluster getTestCluster() {
    return new BigtableClusterBuilder()
        .projectId("project")
        .instanceId("instance")
        .clusterId("cluster")
        .cpuTarget(0.8)
        .maxNodes(500)
        .minNodes(5)
        .overloadStep(100)
        .enabled(true)
        .errorCode(Optional.of(ErrorCode.OK))
        .build();
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
  // Tests

  @Given("that the current node count is {int}")
  public void setCurrentNodeCount(int nodeCount) {
    AutoscaleJobTestMocks.setCurrentSize(bigtableInstanceClient, nodeCount);
  }

  @And("the current load is {double}")
  public void setCurrentLoad(double load) throws IOException {
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, load);
    job.run();
  }

  @Then("the revised number of nodes should be {int}")
  public void finalNodeCount(int nodeCount) {
    assertEquals(nodeCount, newSize);
  }

  @And("the current disk utilization is {double}")
  public void setCurrentDiskUtilization(double diskUtilization) {
    AutoscaleJobTestMocks.setCurrentDiskUtilization(stackdriverClient, diskUtilization);
  }
}
