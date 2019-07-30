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

import com.spotify.autoscaler.filters.AllowAllClusterFilter;
import com.spotify.autoscaler.filters.ClusterFilter;
import com.typesafe.config.Config;
import dagger.Module;
import dagger.Provides;
import java.lang.reflect.InvocationTargetException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Module
public class ClusterFilterModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterFilterModule.class);

  @Provides
  public ClusterFilter clusterFilter(final Config config) {
    ClusterFilter clusterFilter = new AllowAllClusterFilter();
    final String clusterFilterClass = config.getString("clusterFilter");
    if (clusterFilterClass != null && !clusterFilterClass.isEmpty()) {
      try {
        clusterFilter =
            (ClusterFilter)
                Class.forName(clusterFilterClass).getDeclaredConstructor().newInstance();
      } catch (final ClassNotFoundException
          | InstantiationException
          | IllegalAccessException
          | NoSuchMethodException
          | InvocationTargetException e) {
        LOGGER.warn("Failed to create new instance of cluster filter " + clusterFilterClass, e);
      }
    }
    return clusterFilter;
  }
}
