# REST API

### Clusters Resource

* **GET /clusters**
    * Expected query parameters: *None*
    * Returns a list of all clusters' autoscaling settings and latest events.
* **GET /clusters/logs**
    * Expected query parameters:
        * *projectId*: The cluster's Project ID.
        * *instanceId*: The cluster's Instance ID.
        * *clusterId*: The cluster's Cluster ID.
    * Returns a list of the cluster's latest resize events.
* **POST /clusters**
    * Expected query parameters:
        * *projectId*: The cluster's Project ID.
        * *instanceId*: The cluster's Instance ID.
        * *clusterId*: The cluster's Cluster ID.
        * *minNodes*: Minimum number of nodes. The autoscaler will not try to set the number of nodes below this number.
        * *maxNodes*: Maximum number of nodes. The autoscaler will not try to set the number of nodes above this number.
        * *cpuTarget*: Target CPU utilization. The autoscaler will try to set the number of nodes in order to have this CPU utilization.
        * *overloadStep* (optional): In case the cluster is hitting 90% CPU utilization we don't really know how big the resize should be. With this parameter you can set how aggressive the resize step should be in that case. This is an additive factor, not a multiplier to the number of nodes.
        * *enabled* (default=true): Whether to have autoscaling enabled or disabled for the cluster.
    * Adds a Bigtable cluster to be managed by the autoscaler.
* **PUT /clusters**
    * Expected query parameters:
        * *projectId*: The cluster's Project ID.
        * *instanceId*: The cluster's Instance ID.
        * *clusterId*: The cluster's Cluster ID.
        * *minNodes*: Minimum number of nodes. The autoscaler will not try to set the number of nodes below this number.
        * *maxNodes*: Maximum number of nodes. The autoscaler will not try to set the number of nodes above this number.
        * *cpuTarget*: Target CPU utilization. The autoscaler will try to set the number of nodes in order to have this CPU utilization.
        * *overloadStep* (optional): In case the cluster is hitting 90% CPU utilization we don't really know how big the resize should be. With this parameter you can set how aggressive the resize step should be in that case. This is an additive factor, not a multiplier to the number of nodes.
        * *enabled* (default=true): Whether to have autoscaling enabled or disabled for the cluster.
    * Updates all of the autoscaler settings for a Bigtable cluster.
* **PUT /clusters/override-min-nodes**
    * Expected query parameters:
        * *projectId*: The cluster's Project ID.
        * *instanceId*: The cluster's Instance ID.
        * *clusterId*: The cluster's Cluster ID.
        * *minNodesOverride*: Overrides the configured `minNodes`. The autoscaler will not allow less than `max(minNodes, minNodesOverride)` nodes.
    * Set the `minNodesOverride` setting for a Bigtable cluster. Intended use is short-lived needs for a substantially higher node minimum than `minNodes`.
* **GET /clusters/enabled**
    * Expected query parameters:
        * *projectId*: The cluster's Project ID.
        * *instanceId*: The cluster's Instance ID.
        * *clusterId*: The cluster's Cluster ID.
    * Returns whether autoscaler is enabled for the given cluster.  
* **DELETE /clusters**
    * Expected query parameters:
        * *projectId*: The cluster's Project ID.
        * *instanceId*: The cluster's Instance ID.
        * *clusterId*: The cluster's Cluster ID.
    * Deletes a Bigtable cluster from being managed by the autoscaler.

### Health Resource

* **GET /health**
    * Expected query parameters: *None*
    * Returns *Nothing*
    * Responds with a 5xx HTTP code if the Bigtable autoscaler is not able to autoscale. For example if it can't communicate with the database.

## Security
Vanilla bigtable-autoscaler does not provide any security mechanism. However, you can implement your custom [filter](https://javaee.github.io/javaee-spec/javadocs/javax/ws/rs/container/ContainerRequestFilter.html) to perform the 
desired security checks and [plug it in](README.md#registering-jersey-resources-and-providers-dynamically).
