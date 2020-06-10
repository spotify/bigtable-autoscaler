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

import com.spotify.autoscaler.api.ClusterResources;
import com.spotify.autoscaler.api.DeclarativeAutoscalerController;
import com.spotify.autoscaler.api.Endpoint;
import com.spotify.autoscaler.api.HealthCheck;
import dagger.Binds;
import dagger.Module;
import dagger.multibindings.IntoSet;

@Module
public abstract class EndpointsModule {

  @Binds
  @IntoSet
  public abstract Endpoint clusterResources(ClusterResources clusterResources);

  @Binds
  @IntoSet
  public abstract Endpoint healthCheck(HealthCheck healthCheck);

  @Binds
  @IntoSet
  public abstract Endpoint declarativeAutoscalerController(
      DeclarativeAutoscalerController declarativeAutoscalerController);
}
