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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Stream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SimulationIT extends AutoscaleJobITBase {

  public SimulationIT(final FakeBTCluster fakeBTCluster) {
    super(fakeBTCluster);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {

    final Collection<Object[]> data = new ArrayList<>();

    try (Stream<Path> list = Files.list(Path.of(FakeBTCluster.METRICS_PATH))) {
      list.forEach(
          path -> {
            final BigtableCluster cluster =
                FakeBTCluster.getClusterBuilderForFilePath(path)
                    .minNodes(5)
                    .maxNodes(1000)
                    .cpuTarget(0.8)
                    .build();
            data.add(new Object[] {new FakeBTCluster(new TimeSupplier(), cluster)});
          });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return data;
  }

  @Test
  public void simulateCluster() throws IOException {

    final TimeSupplier timeSupplier = (TimeSupplier) fakeBTCluster.getTimeSource();
    timeSupplier.setTime(fakeBTCluster.getFirstMetricsInstant());

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
        Duration.ofSeconds(300),
        280,
        fakeBTCluster::getCPU,
        fakeBTCluster::getStorage,
        ignored -> assertTrue(fakeBTCluster.getCPU() < cluster.cpuTarget() + 0.05d));
  }
}
