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

package com.spotify.autoscaler.api.type;

import io.kubernetes.client.openapi.models.V1ObjectMeta;

public class BigtableAutoscaler {

  private final String apiVersion;
  private final String kind;
  private final V1ObjectMeta metadata;
  private final Spec spec;

  public BigtableAutoscaler(
      final String apiVersion, final String kind, final V1ObjectMeta metadata, final Spec spec) {
    this.apiVersion = apiVersion;
    this.kind = kind;
    this.metadata = metadata;
    this.spec = spec;
  }

  public String getApiVersion() {
    return apiVersion;
  }

  public String getKind() {
    return kind;
  }

  public V1ObjectMeta getMetadata() {
    return metadata;
  }

  public Spec getSpec() {
    return spec;
  }

  public static class Spec {
    private final String instanceId;
    private final Cluster[] cluster;

    public Spec(final String instanceId, final Cluster[] cluster) {
      this.instanceId = instanceId;
      this.cluster = cluster;
    }

    public String getInstanceId() {
      return instanceId;
    }

    public Cluster[] getCluster() {
      return cluster;
    }

    public static class Cluster {
      private final String clusterId;
      private final int minNodes;
      private final int maxNodes;
      private final double cpuTarget;

      Cluster(
          final String clusterId, final int minNodes, final int maxNodes, final double cpuTarget) {
        this.clusterId = clusterId;
        this.minNodes = minNodes;
        this.maxNodes = maxNodes;
        this.cpuTarget = cpuTarget;
      }

      public String getClusterId() {
        return clusterId;
      }

      public int getMinNodes() {
        return minNodes;
      }

      public int getMaxNodes() {
        return maxNodes;
      }

      public double getCpuTarget() {
        return cpuTarget;
      }
    }
  }
}
