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

package com.spotify.autoscaler.di;

import com.spotify.autoscaler.algorithm.Algorithm;
import com.spotify.autoscaler.algorithm.CPUAlgorithm;
import com.spotify.autoscaler.algorithm.StorageAlgorithm;
import com.spotify.autoscaler.client.StackdriverClient;
import com.spotify.autoscaler.metric.AutoscalerMetrics;
import dagger.Module;
import dagger.Provides;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Module
public class AlgorithmModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(AlgorithmModule.class);

  @Provides
  public List<Algorithm> algorithm(
      final StackdriverClient stackdriverClient, final AutoscalerMetrics autoscalerMetrics) {
    final List<Algorithm> list = new ArrayList<>();
    list.add(new CPUAlgorithm(stackdriverClient, autoscalerMetrics));
    list.add(new StorageAlgorithm(stackdriverClient, autoscalerMetrics));
    return list;
  }
}
