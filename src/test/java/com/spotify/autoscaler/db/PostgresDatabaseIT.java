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

package com.spotify.autoscaler.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.spotify.autoscaler.util.ErrorCode;
import com.spotify.metrics.core.SemanticMetricRegistry;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

public class PostgresDatabaseIT {
  SemanticMetricRegistry registry;
  PostgresDatabase db;
  String projectId = "test-project";
  String instanceId = "test-instance";
  String clusterId = "test-cluster";

  private BigtableCluster testCluster() {
    return new BigtableClusterBuilder()
        .projectId(projectId)
        .instanceId(instanceId)
        .clusterId(clusterId)
        .minNodes(10)
        .maxNodes(100)
        .cpuTarget(0.8)
        .overloadStep(10)
        .enabled(true)
        .errorCode(Optional.of(ErrorCode.OK))
        .build();
  }

  private BigtableCluster anotherTestCluster() {
    return new BigtableClusterBuilder()
        .projectId("another-project")
        .instanceId("another-instance")
        .clusterId("another-cluster")
        .minNodes(10)
        .maxNodes(100)
        .cpuTarget(0.8)
        .overloadStep(10)
        .enabled(true)
        .errorCode(Optional.of(ErrorCode.OK))
        .build();
  }

  @Before
  public void setup() throws SQLException, IOException {
    // insert a test cluster
    db = PostgresDatabaseTest.getPostgresDatabase();
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

  @Test
  public void testSetupAndConnection() {
    assertEquals(0, db.getBigtableClusters().size());
  }

  @Test
  public void setLastChangeTest() {
    db.insertBigtableCluster(testCluster());

    Instant now = Instant.now();
    db.setLastChange(projectId, instanceId, clusterId, now);
    Optional<BigtableCluster> btcluster = db.getBigtableCluster(projectId, instanceId, clusterId);
    assertTrue(btcluster.isPresent());
    Optional<Instant> alsoNow = btcluster.get().lastChange();
    assertTrue(alsoNow.isPresent());
    assertEquals(now, alsoNow.get());
  }

  @Test
  public void insert() {
    db.insertBigtableCluster(testCluster());
    assertEquals(ImmutableList.of(testCluster()), db.getBigtableClusters());
  }

  @Test
  public void update() {
    db.insertBigtableCluster(BigtableClusterBuilder.from(testCluster()).minNodes(5).build());
    assertNotEquals(ImmutableList.of(testCluster()), db.getBigtableClusters());
    db.updateBigtableCluster(testCluster());
    assertEquals(ImmutableList.of(testCluster()), db.getBigtableClusters());
  }

  @Test
  public void enable() {
    db.insertBigtableCluster(BigtableClusterBuilder.from(testCluster()).enabled(false).build());
    assertNotEquals(ImmutableList.of(testCluster()), db.getBigtableClusters());
    db.updateBigtableCluster(testCluster());
    assertEquals(ImmutableList.of(testCluster()), db.getBigtableClusters());
  }

  @Test
  public void delete() {
    db.insertBigtableCluster(testCluster());
    db.insertBigtableCluster(anotherTestCluster());
    db.deleteBigtableCluster(projectId, instanceId, clusterId);
    assertEquals(1, db.getBigtableClusters().size());
  }

  @Test
  public void getCandidateClusters() {
    BigtableCluster c1 =
        BigtableClusterBuilder.from(testCluster())
            .clusterId("c1")
            .lastCheck(Instant.ofEpochSecond(1000))
            .build();
    BigtableCluster c2 = BigtableClusterBuilder.from(testCluster()).clusterId("c2").build();
    BigtableCluster c3 =
        BigtableClusterBuilder.from(testCluster())
            .clusterId("c3")
            .lastCheck(Instant.now().minus(Duration.ofSeconds(3)))
            .build();
    BigtableCluster c4 =
        BigtableClusterBuilder.from(testCluster())
            .clusterId("c4")
            .lastCheck(Instant.ofEpochSecond(500))
            .build();
    BigtableCluster c5 =
        BigtableClusterBuilder.from(testCluster()).clusterId("c5").enabled(false).build();

    for (BigtableCluster cluster : Arrays.asList(c1, c2, c3, c4, c5)) {
      db.insertBigtableCluster(cluster);

      cluster
          .lastCheck()
          .ifPresent(
              lastCheck ->
                  db.setLastCheck(
                      cluster.projectId(), cluster.instanceId(), cluster.clusterId(), lastCheck));
    }

    // Verify that the order is as expected
    List<BigtableCluster> clusters = db.getCandidateClusters();
    assertEquals(3, clusters.size());
    assertEquals(c2, clusters.get(0));
    assertEquals(c4, clusters.get(1));
    assertEquals(c1, clusters.get(2));
  }

  @Test
  public void updateLastCheckedSetIfMatches() {
    BigtableCluster c1 =
        BigtableClusterBuilder.from(testCluster()).lastCheck(Instant.ofEpochSecond(1000)).build();
    db.insertBigtableCluster(testCluster()); // This doesn't set lastCheck!
    db.setLastCheck(c1.projectId(), c1.instanceId(), c1.clusterId(), c1.lastCheck().get());

    assertTrue(db.updateLastChecked(c1));

    // Verify that the database object actually got updated
    BigtableCluster updated =
        db.getBigtableCluster(c1.projectId(), c1.instanceId(), c1.clusterId()).get();
    assertTrue(updated.lastCheck().get().isAfter(c1.lastCheck().get()));
  }

  @Test
  public void updateLastCheckedSetIfFirstTime() {
    BigtableCluster c1 = BigtableClusterBuilder.from(testCluster()).build();
    db.insertBigtableCluster(testCluster()); // This doesn't set lastCheck!

    // Slight sanity check here
    BigtableCluster notUpdated =
        db.getBigtableCluster(c1.projectId(), c1.instanceId(), c1.clusterId()).get();
    assertFalse(notUpdated.lastCheck().isPresent());

    assertTrue(db.updateLastChecked(c1));

    // Verify that the database object actually got updated
    BigtableCluster updated =
        db.getBigtableCluster(c1.projectId(), c1.instanceId(), c1.clusterId()).get();
    assertTrue(updated.lastCheck().isPresent());
  }

  @Test
  public void updateLastCheckedNoSetIfNotMatches() {
    BigtableCluster c1 =
        BigtableClusterBuilder.from(testCluster()).lastCheck(Instant.ofEpochSecond(1000)).build();
    db.insertBigtableCluster(testCluster()); // This doesn't set lastCheck!
    db.setLastCheck(c1.projectId(), c1.instanceId(), c1.clusterId(), Instant.ofEpochSecond(2000));

    assertFalse(db.updateLastChecked(c1));

    // Verify that the database object did not get updated
    BigtableCluster updated =
        db.getBigtableCluster(c1.projectId(), c1.instanceId(), c1.clusterId()).get();
    assertEquals(Instant.ofEpochSecond(2000), updated.lastCheck().get());
  }

  @Test
  public void updateLastCheckedNoSetIfFirstTimeButNoMatch() {
    BigtableCluster c1 = BigtableClusterBuilder.from(testCluster()).build();
    db.insertBigtableCluster(testCluster()); // This doesn't set lastCheck!
    db.setLastCheck(c1.projectId(), c1.instanceId(), c1.clusterId(), Instant.ofEpochSecond(2000));

    assertFalse(db.updateLastChecked(c1));

    // Verify that the database object did not get updated
    BigtableCluster updated =
        db.getBigtableCluster(c1.projectId(), c1.instanceId(), c1.clusterId()).get();
    assertEquals(Instant.ofEpochSecond(2000), updated.lastCheck().get());
  }

  @Test
  public void testInsertedAndRetrievedClustersAreEquivalent() throws Exception {

    BigtableCluster cluster =
        BigtableClusterBuilder.from(testCluster()).overloadStep(Optional.ofNullable(null)).build();

    db.insertBigtableCluster(cluster);
    db.insertBigtableCluster(anotherTestCluster());

    BigtableCluster retrievedCluster =
        db.getBigtableCluster(cluster.projectId(), cluster.instanceId(), cluster.clusterId())
            .orElseThrow(() -> new RuntimeException("Inserted cluster not present!!"));

    assertEquals(cluster, retrievedCluster);
  }

  @Test
  public void testEffectiveMinNodesIsEqualToMinNodesWhenNoLoad() {

    db.insertBigtableCluster(testCluster());
    final int loadDelta = 0;
    final int currentNodeCount = 3;
    db.updateLoadDelta(
        testCluster().projectId(),
        testCluster().instanceId(),
        testCluster().clusterId(),
        loadDelta,
        currentNodeCount);

    BigtableCluster retrievedCluster =
        db.getBigtableCluster(
                testCluster().projectId(), testCluster().instanceId(), testCluster().clusterId())
            .orElseThrow(() -> new RuntimeException("Inserted cluster not present!!"));

    assertEquals(retrievedCluster.effectiveMinNodes(), retrievedCluster.minNodes());
    assertEquals(retrievedCluster.overriddenMinNodes(), Optional.empty());
  }

  @Test
  public void testEffectiveMinNodesWhenFirstLoadIsAdded() {

    db.insertBigtableCluster(testCluster());
    final int loadDelta = 10;
    final int currentNodeCount = 3;

    db.updateLoadDelta(
        testCluster().projectId(),
        testCluster().instanceId(),
        testCluster().clusterId(),
        loadDelta,
        currentNodeCount);

    BigtableCluster retrievedCluster =
        db.getBigtableCluster(
                testCluster().projectId(), testCluster().instanceId(), testCluster().clusterId())
            .orElseThrow(() -> new RuntimeException("Inserted cluster not present!!"));

    assertEquals(
        retrievedCluster.effectiveMinNodes(),
        retrievedCluster.overriddenMinNodes().get().longValue());
    assertEquals(retrievedCluster.overriddenMinNodes(), Optional.of(currentNodeCount + loadDelta));
  }

  @Test
  public void testEffectiveMinNodesWhenSecondLoadIsAdded() {

    db.insertBigtableCluster(testCluster());
    final int loadDelta = 10;
    final int currentNodeCount = 3;

    db.updateLoadDelta(
        testCluster().projectId(),
        testCluster().instanceId(),
        testCluster().clusterId(),
        loadDelta,
        currentNodeCount);

    final int newloadDelta = 10;
    final int newCurrentNodeCount = 13;

    db.updateLoadDelta(
        testCluster().projectId(),
        testCluster().instanceId(),
        testCluster().clusterId(),
        newloadDelta,
        newCurrentNodeCount);

    BigtableCluster retrievedCluster =
        db.getBigtableCluster(
                testCluster().projectId(), testCluster().instanceId(), testCluster().clusterId())
            .orElseThrow(() -> new RuntimeException("Inserted cluster not present!!"));

    assertEquals(
        retrievedCluster.effectiveMinNodes(),
        retrievedCluster.overriddenMinNodes().get().longValue());
    assertEquals(
        retrievedCluster.overriddenMinNodes(),
        Optional.of(newCurrentNodeCount + (newloadDelta - loadDelta)));
  }

  @Test
  public void testEffectiveMinNodesDoesNotChangeWhenSameLoadIsAdded() {

    db.insertBigtableCluster(testCluster());
    final int loadDelta = 10;
    final int initialNodeCount = 3;

    db.updateLoadDelta(
        testCluster().projectId(),
        testCluster().instanceId(),
        testCluster().clusterId(),
        loadDelta,
        initialNodeCount);

    final int newCurrentNodeCount = 13;
    db.updateLoadDelta(
        testCluster().projectId(),
        testCluster().instanceId(),
        testCluster().clusterId(),
        loadDelta,
        newCurrentNodeCount);

    BigtableCluster retrievedCluster =
        db.getBigtableCluster(
                testCluster().projectId(), testCluster().instanceId(), testCluster().clusterId())
            .orElseThrow(() -> new RuntimeException("Inserted cluster not present!!"));

    assertEquals(
        retrievedCluster.effectiveMinNodes(),
        retrievedCluster.overriddenMinNodes().get().longValue());
    assertEquals(retrievedCluster.overriddenMinNodes(), Optional.of(initialNodeCount + loadDelta));
  }

  @Test
  public void testEffectiveMinNodesWhenLoadDecrease() {

    db.insertBigtableCluster(testCluster());
    final int loadDelta = 10;
    final int initialNodeCount = 3;

    db.updateLoadDelta(
        testCluster().projectId(),
        testCluster().instanceId(),
        testCluster().clusterId(),
        loadDelta,
        initialNodeCount);

    final int newCurrentNodeCount = 13;
    final int newLoadDelta = 5;
    db.updateLoadDelta(
        testCluster().projectId(),
        testCluster().instanceId(),
        testCluster().clusterId(),
        newLoadDelta,
        newCurrentNodeCount);

    BigtableCluster retrievedCluster =
        db.getBigtableCluster(
                testCluster().projectId(), testCluster().instanceId(), testCluster().clusterId())
            .orElseThrow(() -> new RuntimeException("Inserted cluster not present!!"));

    assertEquals(
        retrievedCluster.effectiveMinNodes(),
        retrievedCluster.overriddenMinNodes().get().longValue());
    assertEquals(
        retrievedCluster.overriddenMinNodes(),
        Optional.of(newCurrentNodeCount + (newLoadDelta - loadDelta)));
  }

  @Test
  public void testEffectiveMinNodesWhenLoadDisappears() {

    db.insertBigtableCluster(testCluster());
    final int loadDelta = 10;
    final int initialNodeCount = 3;

    db.updateLoadDelta(
        testCluster().projectId(),
        testCluster().instanceId(),
        testCluster().clusterId(),
        loadDelta,
        initialNodeCount);

    final int newCurrentNodeCount = 13;
    final int newLoadDelta = 0;
    db.updateLoadDelta(
        testCluster().projectId(),
        testCluster().instanceId(),
        testCluster().clusterId(),
        newLoadDelta,
        newCurrentNodeCount);

    BigtableCluster retrievedCluster =
        db.getBigtableCluster(
                testCluster().projectId(), testCluster().instanceId(), testCluster().clusterId())
            .orElseThrow(() -> new RuntimeException("Inserted cluster not present!!"));

    assertEquals(retrievedCluster.effectiveMinNodes(), retrievedCluster.minNodes());
    assertEquals(retrievedCluster.overriddenMinNodes(), Optional.empty());
  }

  @Test
  public void testEffectiveMinNodesDoesNotExceedMaxNodes() {

    db.insertBigtableCluster(testCluster());
    final int loadDelta = testCluster().maxNodes();
    final int initialNodeCount = 3;

    db.updateLoadDelta(
        testCluster().projectId(),
        testCluster().instanceId(),
        testCluster().clusterId(),
        loadDelta,
        initialNodeCount);

    BigtableCluster retrievedCluster =
        db.getBigtableCluster(
                testCluster().projectId(), testCluster().instanceId(), testCluster().clusterId())
            .orElseThrow(() -> new RuntimeException("Inserted cluster not present!!"));

    assertEquals(retrievedCluster.effectiveMinNodes(), retrievedCluster.maxNodes());
    assertEquals(retrievedCluster.overriddenMinNodes(), Optional.of(initialNodeCount + loadDelta));
  }
}
