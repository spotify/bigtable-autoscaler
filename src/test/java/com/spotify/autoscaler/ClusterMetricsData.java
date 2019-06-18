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

final class ClusterMetricsData {

  public Double diskUtilization;
  public Double nodeCount;
  public Double receivedBytes;
  public Double sentBytes;
  public Double cpuLoad;
  public Double requestCount;
  public Double modifiedRows;
  public Double returnedRows;
  public Double errorCount;

  ClusterMetricsData() {}

  private ClusterMetricsData(
      Double diskUtilization,
      Double nodeCount,
      Double receivedBytes,
      Double sentBytes,
      Double cpuLoad,
      Double requestCount,
      Double modifiedRows,
      Double returnedRows,
      Double errorCount) {
    if (diskUtilization == null) {
      throw new NullPointerException("diskUtilization");
    } else if (nodeCount == null) {
      throw new NullPointerException("nodeCount");
    } else if (receivedBytes == null) {
      throw new NullPointerException("receivedBytes");
    } else if (sentBytes == null) {
      throw new NullPointerException("sentBytes");
    } else if (cpuLoad == null) {
      throw new NullPointerException("cpuLoad");
    } else if (requestCount == null) {
      throw new NullPointerException("requestCount");
    } else if (modifiedRows == null) {
      throw new NullPointerException("modifiedRows");
    } else if (returnedRows == null) {
      throw new NullPointerException("returnedRows");
    } else if (errorCount == null) {
      throw new NullPointerException("errorCount");
    } else {
      this.diskUtilization = diskUtilization;
      this.nodeCount = nodeCount;
      this.receivedBytes = receivedBytes;
      this.sentBytes = sentBytes;
      this.cpuLoad = cpuLoad;
      this.requestCount = requestCount;
      this.modifiedRows = modifiedRows;
      this.returnedRows = returnedRows;
      this.errorCount = errorCount;
    }
  }

  public Double diskUtilization() {
    return this.diskUtilization;
  }

  public Double nodeCount() {
    return this.nodeCount;
  }

  public Double receivedBytes() {
    return this.receivedBytes;
  }

  public Double sentBytes() {
    return this.sentBytes;
  }

  public Double cpuLoad() {
    return this.cpuLoad;
  }

  public Double requestCount() {
    return this.requestCount;
  }

  public Double modifiedRows() {
    return this.modifiedRows;
  }

  public Double returnedRows() {
    return this.returnedRows;
  }

  public Double errorCount() {
    return this.errorCount;
  }

  public static ClusterMetricsDataBuilder builder() {
    return new ClusterMetricsDataBuilder();
  }

  public static final class ClusterMetricsDataBuilder {

    private Double diskUtilization = 0.0d;
    private Double nodeCount = 0.0d;
    private Double receivedBytes = 0.0d;
    private Double sentBytes = 0.0d;
    private Double cpuLoad = 0.0d;
    private Double requestCount = 0.0d;
    private Double modifiedRows = 0.0d;
    private Double returnedRows = 0.0d;
    private Double errorCount = 0.0d;

    public ClusterMetricsDataBuilder() {}

    private ClusterMetricsDataBuilder(ClusterMetricsData v) {
      this.diskUtilization = v.diskUtilization();
      this.nodeCount = v.nodeCount();
      this.receivedBytes = v.receivedBytes();
      this.sentBytes = v.sentBytes();
      this.cpuLoad = v.cpuLoad();
      this.requestCount = v.requestCount();
      this.modifiedRows = v.modifiedRows();
      this.returnedRows = v.returnedRows();
      this.errorCount = v.errorCount();
    }

    private ClusterMetricsDataBuilder(ClusterMetricsDataBuilder v) {
      this.diskUtilization = v.diskUtilization;
      this.nodeCount = v.nodeCount;
      this.receivedBytes = v.receivedBytes;
      this.sentBytes = v.sentBytes;
      this.cpuLoad = v.cpuLoad;
      this.requestCount = v.requestCount;
      this.modifiedRows = v.modifiedRows;
      this.returnedRows = v.returnedRows;
      this.errorCount = v.errorCount;
    }

    public Double diskUtilization() {
      return this.diskUtilization;
    }

    public ClusterMetricsDataBuilder diskUtilization(Double diskUtilization) {
      if (diskUtilization == null) {
        throw new NullPointerException("diskUtilization");
      } else {
        this.diskUtilization = diskUtilization;
        return this;
      }
    }

    public Double nodeCount() {
      return this.nodeCount;
    }

    public ClusterMetricsDataBuilder nodeCount(Double nodeCount) {
      if (nodeCount == null) {
        throw new NullPointerException("nodeCount");
      } else {
        this.nodeCount = nodeCount;
        return this;
      }
    }

    public Double receivedBytes() {
      return this.receivedBytes;
    }

    public ClusterMetricsDataBuilder receivedBytes(Double receivedBytes) {
      if (receivedBytes == null) {
        throw new NullPointerException("receivedBytes");
      } else {
        this.receivedBytes = receivedBytes;
        return this;
      }
    }

    public Double sentBytes() {
      return this.sentBytes;
    }

    public ClusterMetricsDataBuilder sentBytes(Double sentBytes) {
      if (sentBytes == null) {
        throw new NullPointerException("sentBytes");
      } else {
        this.sentBytes = sentBytes;
        return this;
      }
    }

    public Double cpuLoad() {
      return this.cpuLoad;
    }

    public ClusterMetricsDataBuilder cpuLoad(Double cpuLoad) {
      if (cpuLoad == null) {
        throw new NullPointerException("cpuLoad");
      } else {
        this.cpuLoad = cpuLoad;
        return this;
      }
    }

    public Double requestCount() {
      return this.requestCount;
    }

    public ClusterMetricsDataBuilder requestCount(Double requestCount) {
      if (requestCount == null) {
        throw new NullPointerException("requestCount");
      } else {
        this.requestCount = requestCount;
        return this;
      }
    }

    public Double modifiedRows() {
      return this.modifiedRows;
    }

    public ClusterMetricsDataBuilder modifiedRows(Double modifiedRows) {
      if (modifiedRows == null) {
        throw new NullPointerException("modifiedRows");
      } else {
        this.modifiedRows = modifiedRows;
        return this;
      }
    }

    public Double returnedRows() {
      return this.returnedRows;
    }

    public ClusterMetricsDataBuilder returnedRows(Double returnedRows) {
      if (returnedRows == null) {
        throw new NullPointerException("returnedRows");
      } else {
        this.returnedRows = returnedRows;
        return this;
      }
    }

    public Double errorCount() {
      return this.errorCount;
    }

    public ClusterMetricsDataBuilder errorCount(Double errorCount) {
      if (errorCount == null) {
        throw new NullPointerException("errorCount");
      } else {
        this.errorCount = errorCount;
        return this;
      }
    }

    public ClusterMetricsData build() {
      return new ClusterMetricsData(
          this.diskUtilization,
          this.nodeCount,
          this.receivedBytes,
          this.sentBytes,
          this.cpuLoad,
          this.requestCount,
          this.modifiedRows,
          this.returnedRows,
          this.errorCount);
    }

    public static ClusterMetricsDataBuilder from(ClusterMetricsData v) {
      return new ClusterMetricsDataBuilder(v);
    }

    public static ClusterMetricsDataBuilder from(ClusterMetricsDataBuilder v) {
      return new ClusterMetricsDataBuilder(v);
    }
  }
}
