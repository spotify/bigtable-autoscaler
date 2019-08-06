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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.jupiter.api.Test;

public class BigtableClusterTest {

  private final BigtableClusterBuilder clusterBuilder =
      new BigtableClusterBuilder().projectId("").instanceId("").clusterId("");

  @Test
  public void testEffectiveMinNodesShouldNotBeGreaterThanMaxNodes() {
    final BigtableCluster cluster =
        clusterBuilder.minNodes(10).maxNodes(20).minNodesOverride(200).build();
    assertThat(cluster.effectiveMinNodes(), is(cluster.maxNodes()));
  }

  @Test
  public void testEffectiveMinNodesShouldOverrideMinNodes() {
    final BigtableCluster cluster =
        clusterBuilder.minNodes(10).maxNodes(20).minNodesOverride(15).build();
    assertThat(cluster.effectiveMinNodes(), is(15));
  }
}
