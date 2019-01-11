/*-
 * -\-\-
 * bigtable-autoscaler
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
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

package com.spotify.autoscaler.util;

import io.grpc.StatusRuntimeException;
import java.util.Optional;

public enum ErrorCode {
  OK(0),
  GRPC_CANCELLED(1),
  GRPC_UNKNOWN(2),
  GRPC_INVALID_ARGUMENT(3),
  GRPC_DEADLINE_EXCEEDED(4),
  GRPC_NOT_FOUND(5),
  GRPC_ALREADY_EXISTS(6),
  GRPC_PERMISSION_DENIED(7),
  GRPC_RESOURCE_EXHAUSTED(8),
  GRPC_FAILED_PRECONDITION(9),
  GRPC_ABORTED(10),
  GRPC_OUT_OF_RANGE(11),
  GRPC_UNIMPLEMENTED(12),
  GRPC_INTERNAL(13),
  GRPC_UNAVAILABLE(14),
  GRPC_DATA_LOSS(15),
  GRPC_UNAUTHENTICATED(16),
  AUTOSCALER_INTERNAL(17);

  private final int value;
  private static final String GRPC_PREFIX = "GRPC_";

  ErrorCode(int val) {
    this.value = val;
  }

  public static ErrorCode fromException(Optional<Exception> e) {

    return e
        .map(ex -> {
          if (ex instanceof StatusRuntimeException) {
            return valueOf(GRPC_PREFIX + ((StatusRuntimeException) ex).getStatus().getCode());
          }

          return AUTOSCALER_INTERNAL;
        })
        .orElse(OK);
  }

}
