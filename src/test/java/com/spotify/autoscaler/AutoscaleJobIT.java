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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.BigtableClusterBuilder;
import com.spotify.autoscaler.util.ErrorCode;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.Random;
import java.util.function.Supplier;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class AutoscaleJobIT extends AutoscaleJobITBase {

  public AutoscaleJobIT(final FakeBTCluster fakeBTCluster) {
    super(fakeBTCluster);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    // load the files as you want
    final Collection<Object[]> data = new ArrayList<>();

    final BigtableCluster cluster =
        new BigtableClusterBuilder()
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

    data.add(new Object[] {new FakeBTCluster(new TimeSupplier(), cluster)});
    return data;
  }

  @Test
  public void testWeDontResizeTooOften() throws IOException {
    // To give the cluster a chance to settle in, don't resize too often

    // first time we get the last event from the DB we get nothing
    // then we get approximately 8 minutes

    AutoscaleJobTestMocks.setCurrentDiskUtilization(stackdriverClient, 0.00001);
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.7);

    final BigtableCluster cluster = fakeBTCluster.getCluster();
    runJobAndAssertNewSize(fakeBTCluster, cluster, 88, Instant::now);

    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.5);
    final BigtableCluster updatedCluster =
        db.getBigtableCluster(cluster.projectId(), cluster.instanceId(), cluster.clusterId()).get();

    runJobAndAssertNewSize(
        fakeBTCluster, updatedCluster, 88, () -> Instant.now().plus(Duration.ofMinutes(8)));
  }

  @Test
  public void testSmallResizesDontHappenTooOften() throws IOException {
    // To avoid oscillating, don't do small size changes too often
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.7);

    final BigtableCluster cluster = fakeBTCluster.getCluster();
    runJobAndAssertNewSize(fakeBTCluster, cluster, 88, Instant::now);

    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.78);
    final BigtableCluster updatedCluster =
        db.getBigtableCluster(cluster.projectId(), cluster.instanceId(), cluster.clusterId()).get();

    runJobAndAssertNewSize(
        fakeBTCluster, updatedCluster, 88, () -> Instant.now().plus(Duration.ofMinutes(100)));
  }

  @Test
  public void testSmallResizesHappenEventually() throws IOException {
    // To avoid oscillating, don't do small size changes too often
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.7);

    final BigtableCluster cluster = fakeBTCluster.getCluster();
    runJobAndAssertNewSize(fakeBTCluster, cluster, 88, Instant::now);

    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.78);
    final BigtableCluster updatedCluster =
        db.getBigtableCluster(cluster.projectId(), cluster.instanceId(), cluster.clusterId()).get();

    runJobAndAssertNewSize(
        fakeBTCluster, updatedCluster, 86, () -> Instant.now().plus(Duration.ofMinutes(480)));
  }

  @Test
  public void randomDataTest() throws IOException {
    // This test is useful to see that we don't get stuck at any point, for example
    // there is no Connection leak.
    final Random random = new Random();
    final Instant start = Instant.now();

    final TimeSupplier timeSupplier = new TimeSupplier();
    timeSupplier.setTime(start);

    testThroughTime(
        timeSupplier,
        Duration.ofSeconds(300),
        512,
        random::nextDouble,
        random::nextDouble,
        ignored -> assertTrue(true));
  }

  private void runJobAndAssertNewSize(
      final FakeBTCluster fakeBTCluster,
      final BigtableCluster cluster,
      final int expectedSize,
      final Supplier<Instant> timeSource)
      throws IOException {
    final AutoscaleJob job =
        new AutoscaleJob(
            bigtableSession,
            stackdriverClient,
            cluster,
            db,
            registry,
            autoscalerMetrics,
            timeSource);
    job.run();
    assertEquals(expectedSize, fakeBTCluster.getNumberOfNodes());
  }
}
