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

import static org.junit.Assert.assertTrue;

import com.google.bigtable.admin.v2.Cluster;
import com.spotify.autoscaler.db.BigtableCluster;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Stream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class SimulationIT extends AutoscaleJobITBase {

  private static final Logger logger = LoggerFactory.getLogger(SimulationIT.class);
  private static final double CRITICAL_ADDITIONAL_CPU_THRESHOLD = 0.6d;

  public SimulationIT(final FakeBTCluster fakeBTCluster) {
    super(fakeBTCluster);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {

    final Collection<Object[]> data = new ArrayList<>();

    try (final Stream<Path> list = Files.list(Paths.get(FakeBTCluster.METRICS_PATH))) {
      list.forEach(
          path -> {
            final BigtableCluster cluster =
                FakeBTCluster.getClusterBuilderForFilePath(path)
                    .minNodes(5)
                    .maxNodes(1000)
                    .cpuTarget(0.1)
                    .build();
            data.add(new Object[] {new FakeBTCluster(new TimeSupplier(), cluster)});
          });
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }

    return data;
  }

  @Test
  public void simulateCluster() throws IOException {

    final TimeSupplier timeSupplier = (TimeSupplier) fakeBTCluster.getTimeSource();
    timeSupplier.setTime(fakeBTCluster.getFirstValidMetricsInstant());

    final BigtableCluster cluster = fakeBTCluster.getCluster();
    final int initialNodeCount = fakeBTCluster.getMetricsForNow().nodeCount().intValue();
    fakeBTCluster.setNumberOfNodes(initialNodeCount);
    bigtableInstanceClient.updateCluster(
        Cluster.newBuilder()
            .setName(cluster.clusterName())
            .setServeNodes(initialNodeCount)
            .build());

    testThroughTime(
        timeSupplier,
        Duration.ofSeconds(60),
        1400,
        fakeBTCluster::getCPU,
        fakeBTCluster::getStorage,
        ignored -> {
          logger.warn(
              "Instant: {}, cpu: {}, nodeCount: {}, status: {}",
              timeSupplier.get().toString(),
              fakeBTCluster.getCPU(),
              fakeBTCluster.getNumberOfNodes(),
              fakeBTCluster.getCPU() > cluster.cpuTarget() + CRITICAL_ADDITIONAL_CPU_THRESHOLD
                  ? "CRITICAL"
                  : fakeBTCluster.getCPU() > cluster.cpuTarget() ? "HIGH" : "NORMAL");

          assertTrue(
              fakeBTCluster.getCPU() < cluster.cpuTarget() + CRITICAL_ADDITIONAL_CPU_THRESHOLD);
        });
  }
}
