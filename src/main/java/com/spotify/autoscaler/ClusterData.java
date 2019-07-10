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

import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.util.ErrorCode;
import java.util.Optional;

public class ClusterData {

  private BigtableCluster cluster;
  private int currentNodeCount;
  private int minNodeCount;
  private int maxNodeCount;
  private int effectiveMinNodeCount;
  private double cpuUtil;
  private int consecutiveFailureCount;
  private double storageUtil;
  private Optional<ErrorCode> lastErrorCode;

  public int getCurrentNodeCount() {
    return currentNodeCount;
  }

  public int getConsecutiveFailureCount() {
    return consecutiveFailureCount;
  }

  public double getCpuUtil() {
    return cpuUtil;
  }

  public Optional<ErrorCode> getLastErrorCode() {
    return lastErrorCode;
  }

  public void setCurrentNodeCount(final int currentNodeCount) {
    this.currentNodeCount = currentNodeCount;
  }

  public void setConsecutiveFailureCount(final int consecutiveFailureCount) {
    this.consecutiveFailureCount = consecutiveFailureCount;
  }

  public void setLastErrorCode(final Optional<ErrorCode> lastErrorCode) {
    this.lastErrorCode = lastErrorCode;
  }

  public double getStorageUtil() {
    return storageUtil;
  }

  public void setStorageUtil(double storageUtil) {
    this.storageUtil = storageUtil;
  }

  public void setCpuUtil(double cpuUtil) {
    this.cpuUtil = cpuUtil;
  }

  public BigtableCluster getCluster() {
    return cluster;
  }

  public ClusterData(
      final BigtableCluster cluster,
      final int currentNodeCount,
      final int consecutiveFailureCount,
      final Optional<ErrorCode> lastErrorCode) {
    this.currentNodeCount = currentNodeCount;
    this.minNodeCount = cluster.minNodes();
    this.maxNodeCount = cluster.maxNodes();
    this.effectiveMinNodeCount = cluster.effectiveMinNodes();
    this.cluster = cluster;
    this.consecutiveFailureCount = consecutiveFailureCount;
    this.lastErrorCode = lastErrorCode;
  }

  public int getMaxNodeCount() {
    return maxNodeCount;
  }

  public void setMaxNodeCount(int maxNodeCount) {
    this.maxNodeCount = maxNodeCount;
  }

  public int getMinNodeCount() {
    return minNodeCount;
  }

  public void setMinNodeCount(int minNodeCount) {
    this.minNodeCount = minNodeCount;
  }

  public int getEffectiveMinNodeCount() {
    return effectiveMinNodeCount;
  }

  public void setEffectiveMinNodeCount(int effectiveMinNodeCount) {
    this.effectiveMinNodeCount = effectiveMinNodeCount;
  }
}
