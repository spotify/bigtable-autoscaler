package com.spotify.autoscaler.util;

import com.google.bigtable.admin.v2.Cluster;
import com.google.cloud.bigtable.grpc.BigtableInstanceClient;
import com.google.longrunning.GetOperationRequest;
import com.google.longrunning.Operation;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigtableClusterUtilities {
  private static final Logger LOGGER = LoggerFactory.getLogger(BigtableClusterUtilities.class);

  /**
   * Update a cluster to have a specific size
   *
   * @param cluster The fully qualified clusterName
   * @throws InterruptedException thrown when interrupted while waiting for the cluster to resize
   */
  public static void updateClusterSize(BigtableInstanceClient client, Cluster cluster)
      throws InterruptedException {
    LOGGER.info("Updating cluster {} to size {}", cluster.getName(), cluster.getServeNodes());
    Operation operation = client.updateCluster(cluster);
    waitForOperation(client, operation.getName(), 60);
    LOGGER.info("Done updating cluster {}.", cluster.getName());
  }

  /**
   * Waits for an operation like cluster resizing to complete.
   *
   * @param operationName The fully qualified name of the operation
   * @param maxSeconds The maximum amount of seconds to wait for the operation to complete.
   * @throws InterruptedException if a user interrupts the process, usually with a ^C.
   */
  private static void waitForOperation(
      BigtableInstanceClient client, String operationName, int maxSeconds)
      throws InterruptedException {
    long endTimeMillis = TimeUnit.SECONDS.toMillis(maxSeconds) + System.currentTimeMillis();

    GetOperationRequest request = GetOperationRequest.newBuilder().setName(operationName).build();
    do {
      Thread.sleep(500);
      Operation response = client.getOperation(request);
      if (response.getDone()) {
        switch (response.getResultCase()) {
          case RESPONSE:
            return;
          case ERROR:
            throw new StatusRuntimeException(
                Status.fromCodeValue(response.getError().getCode())
                    .withDescription(response.getError().getMessage()));
          case RESULT_NOT_SET:
            throw new IllegalStateException(
                "System returned invalid response for Operation check: " + response);
        }
      }
    } while (System.currentTimeMillis() < endTimeMillis);

    throw new IllegalStateException(
        String.format("Waited %d seconds and cluster was not resized yet.", maxSeconds));
  }
}
