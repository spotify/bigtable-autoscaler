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

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public interface Database extends AutoCloseable {

  default List<BigtableCluster> getBigtableClusters() {
    return getBigtableClusters(null, null, null);
  }

  List<BigtableCluster> getBigtableClusters(String projectId, String instanceId, String clusterId);

  Optional<BigtableCluster> getBigtableCluster(
      String projectId, String instanceId, String clusterId);

  boolean insertBigtableCluster(BigtableCluster cluster);

  boolean updateBigtableCluster(BigtableCluster cluster);

  boolean deleteBigtableCluster(String projectId, String instanceId, String clusterId);

  boolean setLastChange(String projectId, String instanceId, String clusterId, Instant lastChange);

  List<BigtableCluster> getCandidateClusters();

  boolean updateLastChecked(BigtableCluster cluster);

  boolean clearFailureCount(BigtableCluster cluster);

  boolean increaseFailureCount(
      BigtableCluster cluster, Instant lastFailure, String lastFailureMessage, ErrorCode errorCode);

  default Set<String> getActiveClusterKeys() {
    return getBigtableClusters()
        .stream()
        .filter(BigtableCluster::enabled)
        .filter(BigtableCluster::exists)
        .map(BigtableCluster::clusterName)
        .collect(Collectors.toSet());
  }

  void logResize(ClusterResizeLog log);

  long getDailyResizeCount();

  void healthCheck();

  Collection<ClusterResizeLog> getLatestResizeEvents(
      String projectId, String instanceId, String clusterId);

  boolean setMinNodesOverride(
      String projectId, String instanceId, String clusterId, Integer minNodesOverride);

  int getTotalConnections();

  int deleteBigtableClusters(String projectId, String instanceId);

  int deleteBigtableClustersExcept(String projectId, String instanceId, Set<String> keySet);

  boolean reconcileBigtableCluster(final BigtableCluster cluster);

}
