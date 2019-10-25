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

import com.typesafe.config.Config;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.grpc.ServerInterceptor;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Module
public class GrpcServerInterceptorsModule {

  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcServerInterceptorsModule.class);

  @Provides
  Set<ServerInterceptor> serverInterceptors(final Config config) {

    final Set<ServerInterceptor> serverInterceptors = new HashSet<>();
    final String interceptors =
        config.getConfig("grpc").getConfig("server").getString("interceptors");
    if (interceptors == null || interceptors.isEmpty()) {
      return serverInterceptors;
    }
    try {
      for (String className : interceptors.split(";")) {
        final Class<?> clazz = Class.forName(className);
        for (Method declaredMethod : clazz.getDeclaredMethods()) {
          if (declaredMethod.isAnnotationPresent(Provides.class)
              && declaredMethod.isAnnotationPresent(IntoSet.class)
              && ServerInterceptor.class.isAssignableFrom(declaredMethod.getReturnType())) {
            declaredMethod.setAccessible(true);
            serverInterceptors.add((ServerInterceptor) declaredMethod.invoke(null));
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error("Error while injecting server interceptors", e);
    }

    return serverInterceptors;
  }
}
