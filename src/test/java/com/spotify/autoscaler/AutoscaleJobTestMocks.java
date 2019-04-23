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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.when;

import com.google.bigtable.admin.v2.Cluster;
import com.google.cloud.bigtable.grpc.BigtableInstanceClient;
import com.spotify.autoscaler.client.StackdriverClient;
import java.time.Duration;
import org.mockito.internal.matchers.GreaterThan;
import org.mockito.internal.matchers.LessOrEqual;

public abstract class AutoscaleJobTestMocks {

  public static void setCurrentSize(BigtableInstanceClient client, int size) {
    Cluster cluster = Cluster.newBuilder().setServeNodes(size).build();
    when(client.getCluster(any())).thenReturn(cluster);
  }

  public static void setCurrentLoad(StackdriverClient client, double load) {
    when(client.getCpuLoad(any(), argThat(new GreaterThan<>(Duration.ZERO)))).thenReturn(load);
    when(client.getCpuLoad(any(), argThat(new LessOrEqual<>(Duration.ZERO))))
        .thenThrow(new RuntimeException("Negative duration!!!"));
  }

  public static void setCurrentDiskUtilization(StackdriverClient client, double diskUtil) {
    when(client.getDiskUtilization(any(), argThat(new GreaterThan<>(Duration.ZERO)))).thenReturn(diskUtil);
    when(client.getDiskUtilization(any(), argThat(new LessOrEqual<>(Duration.ZERO))))
        .thenThrow(new RuntimeException("Negative duration!!!"));
  }
}
