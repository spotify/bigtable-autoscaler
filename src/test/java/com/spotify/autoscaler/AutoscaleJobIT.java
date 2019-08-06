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
import com.spotify.autoscaler.db.ErrorCode;
import com.spotify.autoscaler.simulation.FakeBigtableCluster;
import com.spotify.autoscaler.simulation.FakeBigtableClusterLoader;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class AutoscaleJobIT extends AutoscaleJobITBase {

  static final BigtableClusterBuilder DEFAULTS =
      new BigtableClusterBuilder()
          .cpuTarget(0.8)
          .maxNodes(500)
          .minNodes(5)
          .overloadStep(100)
          .enabled(true)
          .errorCode(Optional.of(ErrorCode.OK));

  public static class SimpleLoader implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) {
      return Stream.of(
          FakeBigtableClusterLoader.one(FakeBigtableClusterLoader.SIMPLE, AutoscaleJobIT.DEFAULTS));
    }
  }

  // DONTLIKEIT do these need all the features of a simulation test?
  @ParameterizedTest
  @ArgumentsSource(SimpleLoader.class)
  public void testWeDontResizeTooOften(final FakeBigtableCluster fakeBigtableCluster)
      throws IOException {
    // To give the cluster a chance to settle in, don't resize too often

    // first time we get the last event from the DB we get nothing
    // then we get approximately 8 minutes
    setupMocksFor(fakeBigtableCluster);

    AutoscaleJobTestMocks.setCurrentDiskUtilization(stackdriverClient, 0.00001);
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.7);

    final BigtableCluster cluster = fakeBigtableCluster.getCluster();
    runJobAndAssertNewSize(fakeBigtableCluster, cluster, 88, Instant::now);

    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.5);
    final BigtableCluster updatedCluster =
        database
            .getBigtableCluster(cluster.projectId(), cluster.instanceId(), cluster.clusterId())
            .get();

    runJobAndAssertNewSize(
        fakeBigtableCluster, updatedCluster, 88, () -> Instant.now().plus(Duration.ofMinutes(8)));
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleLoader.class)
  public void testSmallResizesDontHappenTooOften(final FakeBigtableCluster fakeBigtableCluster)
      throws IOException {
    setupMocksFor(fakeBigtableCluster);
    // To avoid oscillating, don't do small size changes too often
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.7);

    final BigtableCluster cluster = fakeBigtableCluster.getCluster();
    runJobAndAssertNewSize(fakeBigtableCluster, cluster, 88, Instant::now);

    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.78);
    final BigtableCluster updatedCluster =
        database
            .getBigtableCluster(cluster.projectId(), cluster.instanceId(), cluster.clusterId())
            .get();

    runJobAndAssertNewSize(
        fakeBigtableCluster, updatedCluster, 88, () -> Instant.now().plus(Duration.ofMinutes(100)));
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleLoader.class)
  public void testSmallResizesHappenEventually(final FakeBigtableCluster fakeBigtableCluster)
      throws IOException {
    setupMocksFor(fakeBigtableCluster);
    // To avoid oscillating, don't do small size changes too often
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.7);

    final BigtableCluster cluster = fakeBigtableCluster.getCluster();
    runJobAndAssertNewSize(fakeBigtableCluster, cluster, 88, Instant::now);

    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.78);
    final BigtableCluster updatedCluster =
        database
            .getBigtableCluster(cluster.projectId(), cluster.instanceId(), cluster.clusterId())
            .get();

    runJobAndAssertNewSize(
        fakeBigtableCluster, updatedCluster, 86, () -> Instant.now().plus(Duration.ofMinutes(480)));
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleLoader.class)
  public void randomDataTest(final FakeBigtableCluster fakeBigtableCluster) throws IOException {
    setupMocksFor(fakeBigtableCluster);
    // This test is useful to see that we don't get stuck at any point, for example
    // there is no Connection leak.
    final Random random = new Random();
    final Instant start = Instant.now();

    final TimeSupplier timeSupplier = new TimeSupplier();
    timeSupplier.setTime(start);

    testThroughTime(
        fakeBigtableCluster,
        timeSupplier,
        Duration.ofSeconds(300),
        512,
        random::nextDouble,
        random::nextDouble,
        ignored -> assertTrue(true),
        ignored -> assertTrue(true));
  }

  private void runJobAndAssertNewSize(
      final FakeBigtableCluster fakeBigtableCluster,
      final BigtableCluster cluster,
      final int expectedSize,
      final Supplier<Instant> timeSource)
      throws IOException {
    final AutoscaleJob job = new AutoscaleJob(stackdriverClient, database, autoscalerMetrics);
    job.run(cluster, bigtableSession, timeSource);
    assertEquals(expectedSize, fakeBigtableCluster.getNumberOfNodes());
  }
}
