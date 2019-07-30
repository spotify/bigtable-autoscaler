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

import com.spotify.autoscaler.client.StackdriverClient;
import dagger.Module;
import dagger.Provides;
import java.io.IOException;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Module
public class StackdriverModule {

  private static final Logger LOGGER = LoggerFactory.getLogger(StackdriverModule.class);

  @Provides
  @Singleton
  public static StackdriverClient stackdriverClient() {
    try {
      return new StackdriverClient();
    } catch (final IOException e) {
      LOGGER.error("Failed to initialize Stackdriver client", e);
      throw new RuntimeException(e);
    }
  }
}
