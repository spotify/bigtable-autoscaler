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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
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
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class AutoscaleJobSteps {
  // Default values:
  private static final String PROJECT_ID = "project";
  private static final String INSTANCE_ID = "instance";
  private static final String CLUSTER_ID = "cluster";
  private static final double CPU_TARGET = 0.8;
  private static final int MIN_NODES = 6;
  private static final int MAX_NODES = 500;
  private static final int OVERLOAD_STEP = 100;
  private static final boolean ENABLED = true;
  // Mocked Objects
  @Mock BigtableSession bigtableSessionMocked;
  @Mock BigtableInstanceClient bigtableInstanceClientMocked;
  @Mock StackdriverClient stackdriverClientMocked;
  @Mock SemanticMetricRegistry registryMocked;
  @Mock ClusterStats clusterStatsMocked;

  // May be non-default:
  private List<MetricId> metric;
  // Mocked
  private BigtableSession bigtableSession;
  private BigtableInstanceClient bigtableInstanceClient;
  private StackdriverClient stackdriverClient;
  private SemanticMetricRegistry registry;
  private ClusterStats clusterStats;
  // Non-mocked:
  private String projectId;
  private String instanceId;
  private String clusterId;
  private double cpuTarget;
  private int minNumberNodes;
  private int maxNumberNodes;
  private int overloadStep;
  private boolean enabled;

  private PostgresDatabase db;
  private AutoscaleJob job;
  private BigtableCluster cluster;
  private int newSize;
  private StringJoiner exceptionCaught;

  @Before
  public void setUp() throws IOException {
    initMocks(this);
    initialSetup();
    setDefaultValues();
    cluster = getTestCluster();
    db = initDatabase(cluster, registry);
    job = getTestJob();
    exceptionCaught = new StringJoiner(" ");
  }

  private void setDefaultValues() {
    // Mocked
    bigtableSession = bigtableSessionMocked;
    bigtableInstanceClient = bigtableInstanceClientMocked;
    stackdriverClient = stackdriverClientMocked;
    registry = registryMocked;
    clusterStats = clusterStatsMocked;
    // default values
    projectId = PROJECT_ID;
    instanceId = INSTANCE_ID;
    clusterId = CLUSTER_ID;
    cpuTarget = CPU_TARGET;
    minNumberNodes = MIN_NODES;
    maxNumberNodes = MAX_NODES;
    overloadStep = OVERLOAD_STEP;
    enabled = ENABLED;
  }

  private AutoscaleJob getTestJob() {
    return getMockJob(
        bigtableSession,
        stackdriverClient,
        cluster,
        db,
        registry,
        clusterStats,
        () -> Instant.now());
  }

  private static PostgresDatabase initDatabase(
      BigtableCluster cluster, SemanticMetricRegistry registry) {
    PostgresDatabase database = PostgresDatabaseTest.getPostgresDatabase();
    database.deleteBigtableCluster(cluster.projectId(), cluster.instanceId(), cluster.clusterId());
    database.insertBigtableCluster(cluster);
    return database;
  }

  private AutoscaleJob getMockJob(
      BigtableSession bigtableSession,
      StackdriverClient stackdriverClient,
      BigtableCluster cluster,
      PostgresDatabase db,
      SemanticMetricRegistry registry,
      ClusterStats clusterStats,
      Supplier<Instant> timeSource) {
    return new AutoscaleJob(
        bigtableSession, stackdriverClient, cluster, db, registry, clusterStats, timeSource);
  }

  private BigtableCluster getTestCluster() {
    return getMockCluster(
        projectId,
        instanceId,
        clusterId,
        cpuTarget,
        maxNumberNodes,
        minNumberNodes,
        overloadStep,
        enabled,
        Optional.of(ErrorCode.OK));
  }

  private void initialSetup() throws IOException {
    when(registryMocked.meter(any())).thenReturn(new Meter());
    when(bigtableSessionMocked.getInstanceAdminClient()).thenReturn(bigtableInstanceClientMocked);
    AutoscaleJobTestMocks.setCurrentDiskUtilization(stackdriverClientMocked, 0.00001);
    when(bigtableInstanceClientMocked.updateCluster(any()))
        .thenAnswer(
            invocationOnMock -> {
              newSize = ((Cluster) invocationOnMock.getArgument(0)).getServeNodes();
              AutoscaleJobTestMocks.setCurrentSize(bigtableInstanceClientMocked, newSize);
              return null;
            });
  }

  private BigtableCluster getMockCluster(
      String projectName,
      String instanceName,
      String clusterName,
      double cpuTarget,
      int maxNumberNodes,
      int minNumberNodes,
      int overloadStep,
      boolean enabled,
      Optional<ErrorCode> errorCode) {
    return new BigtableClusterBuilder()
        .projectId(projectName)
        .instanceId(instanceName)
        .clusterId(clusterName)
        .cpuTarget(cpuTarget)
        .maxNodes(maxNumberNodes)
        .minNodes(minNumberNodes)
        .overloadStep(overloadStep)
        .enabled(enabled)
        .errorCode(errorCode)
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

  @Given("^that the current node count is (.+)$")
  public void setCurrentNodeCount(int nodeCount) {
    AutoscaleJobTestMocks.setCurrentSize(bigtableInstanceClient, nodeCount);
  }

  @And("^the current load is (.+)$")
  public void setCurrentLoad(double load) {
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, load);
    System.out.println(load);
  }

  @Then("^the revised number of nodes should be (.+)$")
  public void finalNodeCount(int nodeCount) {
    assertEquals(nodeCount, newSize);
  }

  @And("^the current disk utilization is (.+)$")
  public void setCurrentDiskUtilization(double diskUtilization) {
    AutoscaleJobTestMocks.setCurrentDiskUtilization(stackdriverClient, diskUtilization);
  }

  @When("^the overload step is empty for the cluster$")
  public void setOverloadStepToEmpty() {
    cluster = BigtableClusterBuilder.from(this.cluster).overloadStep(Optional.empty()).build();
    job =
        getMockJob(
            bigtableSession,
            stackdriverClient,
            this.cluster,
            db,
            registry,
            clusterStats,
            Instant::now);
  }

  @When("^the job is executed (.+) times$")
  public void theJobIsExecutedTimes(int times) {
    for (int i = 0; i < times; i++) {
      try {
        job.run();
      } catch (IOException e) {
        exceptionCaught.add("IOException");
      } catch (RuntimeException e) {
        exceptionCaught.add("RuntimeException");
      }
    }
  }

  @Then("^a (.+) is expected.$")
  public void exceptionIsExpected(String exceptionTarget) {
    assertThat(
        exceptionCaught.toString().toUpperCase(), containsString(exceptionTarget.toUpperCase()));
  }

  @Given("a job configured with a new registry")
  public void jobConfiguredWithANewRegistry() {
    registry = new SemanticMetricRegistry();
    job =
        getMockJob(
            bigtableSession, stackdriverClient, cluster, db, registry, clusterStats, Instant::now);
  }

  @And("^the metric is created with filter (.+)$")
  public void theMetricIsCreatedWithFilterOverriddenDesiredNodeCount(String filter) {
    metric =
        registry
            .getMeters()
            .keySet()
            .stream()
            .filter(meter -> meter.getTags().containsValue(filter))
            .collect(Collectors.toList());
    System.out.println(metric.size());
  }

  @And("the maximum number of nodes of {int}")
  public void withMaximumNumberOfNodesOf(int maxNodes) {
    maxNumberNodes = maxNodes;
  }

  @And("the minimum number of nodes of {int}")
  public void theMinimumNumberOfNodesOf(int minNodes) {
    minNumberNodes = minNodes;
  }

  @Then("the metrics size should be {int}")
  public void theMetricsSizeShouldBe(int metricSize) {
    assertThat(metric.size(), is(equalTo(metricSize)));
  }

  @And("the following should match:")
  public void shouldMatch(Map<String, String> map) {
    Map<String, String> tags = metric.get(0).getTags();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      assertThat(entry.getKey(), is(equalTo(tags.get(entry.getValue()))));
    }
  }

  @And("the default values should match")
  public void theDefaultValuesShouldMatch() {
    Map<String, String> tags = metric.get(0).getTags();
    assertEquals(String.valueOf(minNumberNodes), tags.get("min-nodes"));
    assertEquals(String.valueOf(maxNumberNodes), tags.get("max-nodes"));
    assertEquals(projectId, tags.get("project-id"));
    assertEquals(clusterId, tags.get("cluster-id"));
    assertEquals(instanceId, tags.get("instance-id"));
  }

  @Given("^that the cluster had (.+) failures$")
  public void thatTheClusterAlreadyHadFailures(int failures) {
    cluster =
        BigtableClusterBuilder.from(cluster)
            .consecutiveFailureCount(failures)
            .lastFailure(Instant.now())
            .build();
  }

  @Then("^the job should (.*) an exponential backoff after (.+) seconds$")
  public void theJobShouldDoAnExponentialBackoffAfterSeconds(String shouldDo, int seconds) {
    boolean should = true;
    if (shouldDo.toUpperCase().contains("NOT")) {
      should = false;
    }
    job =
        new AutoscaleJob(
            bigtableSession,
            stackdriverClient,
            cluster,
            db,
            registry,
            clusterStats,
            () -> Instant.now().plusSeconds(seconds));
    assertThat(should, is(job.shouldExponentialBackoff()));
  }
}
