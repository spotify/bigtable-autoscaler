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

import dagger.Component;

@Component(
    modules = {
      ConfigModule.class,
      DatabaseModule.class,
      StackdriverModule.class,
      HttpServerModule.class,
      MetricsRegistryModule.class,
      AutoscalerMetricsModule.class,
      FastForwardReporterModule.class,
      ClusterFilterModule.class,
      AutoscalerModule.class,
    })
public interface AutoscalerComponent {
  ServiceInit configure();
}
