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

import io.norberg.automatter.AutoMatter;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

@AutoMatter
public interface ClusterResizeLog {

  Date timestamp();

  String projectId();

  String instanceId();

  String clusterId();

  int minNodes();

  int maxNodes();

  int loadDelta();

  double cpuTarget();

  Optional<Integer> overloadStep();

  int currentNodes();

  int targetNodes();

  double cpuUtilization();

  double storageUtilization();

  boolean success();

  @Nullable
  String resizeReason();

  List<String> resizeReasons();

  Optional<String> errorMessage();

  Optional<Integer> minNodesOverride();

  static ClusterResizeLogBuilder builder(final BigtableCluster cluster) {
    return new ClusterResizeLogBuilder()
        .timestamp(new Date())
        .projectId(cluster.projectId())
        .instanceId(cluster.instanceId())
        .clusterId(cluster.clusterId())
        .minNodes(cluster.minNodes())
        .maxNodes(cluster.maxNodes())
        .cpuTarget(cluster.cpuTarget())
        .overloadStep(cluster.overloadStep())
        .loadDelta(cluster.loadDelta())
        .minNodesOverride(cluster.minNodesOverride());
  }
}
