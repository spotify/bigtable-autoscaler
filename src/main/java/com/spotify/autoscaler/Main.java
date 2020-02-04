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

import com.spotify.autoscaler.di.AlgorithmModule;
import com.spotify.autoscaler.di.AutoscalerExecutorModule;
import com.spotify.autoscaler.di.ClusterFilterModule;
import com.spotify.autoscaler.di.ConfigModule;
import com.spotify.autoscaler.di.DatabaseModule;
import com.spotify.autoscaler.di.HttpServerModule;
import com.spotify.autoscaler.di.MetricsModule;
import com.spotify.autoscaler.di.ObjectMapperModule;
import com.spotify.autoscaler.di.OptionalsModule;
import com.spotify.autoscaler.di.StackdriverModule;
import dagger.Component;
import javax.inject.Singleton;
import org.slf4j.bridge.SLF4JBridgeHandler;

/** Application entry point. */
public final class Main {

  @Singleton
  @Component(
      modules = {
        ConfigModule.class,
        DatabaseModule.class,
        StackdriverModule.class,
        HttpServerModule.class,
        MetricsModule.class,
        ClusterFilterModule.class,
        AutoscalerExecutorModule.class,
        ObjectMapperModule.class,
        OptionalsModule.class,
        AlgorithmModule.class,
      })
  public interface AutoscalerComponent {
    Application configure();
  }

  /**
   * Runs the application.
   *
   * @param args command-line arguments
   */
  public static void main(final String... args) throws Exception {
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
    AutoscalerComponent autoscaler = DaggerMain_AutoscalerComponent.builder().build();
    autoscaler.configure().start();
  }
}
