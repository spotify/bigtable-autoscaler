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

package com.spotify.autoscaler.db;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

public class PostgresDatabaseTest {

  /*
  Obs: By default database container is being stopped as soon as last connection is closed.
  So, we don't need to be worried about the containers closing, apparently.
  However, the db connection should be closed.
  */

  public static PostgresDatabase getPostgresDatabase() {
    JdbcDatabaseContainer container = new PostgreSQLContainer().withInitScript("schema.sql");
    container.start();
    Config config =
        ConfigFactory.empty()
            .withValue("jdbcUrl", ConfigValueFactory.fromAnyRef(container.getJdbcUrl()))
            .withValue("username", ConfigValueFactory.fromAnyRef(container.getUsername()))
            .withValue("password", ConfigValueFactory.fromAnyRef(container.getPassword()))
            .withValue("maxConnectionPool", ConfigValueFactory.fromAnyRef(1));

    return new PostgresDatabase(config);
  }
}
