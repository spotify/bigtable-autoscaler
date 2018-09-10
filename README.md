# bigtable-autoscaler

You have a Bigtable cluster and you would like to optimize its cost by having the
right size at any given time. Then you should consider using the Bigtable
autoscaler service!

## How does it work?

The Bigtable autoscaler is a backend service that periodically sends
resize commands to Bigtable clusters. It is backed by a PostgreSQL database for
keeping its state, like for example:

* number of nodes min/max boundaries
* target CPU utilization
* last resize event

The autoscaler checks the database every 30 seconds and decides if it should 
do something or not (there are time thresholds to not resize clusters too often). 
In case it's time to check a cluster, it fetches the current CPU utilization 
from the Bigtable API. If that is different from the target CPU utilization 
(also here there are thresholds) it calculates the adequate number of nodes 
and then it sends a resize request.

The autoscaler also provides an HTTP API to insert, update and delete Bigtable 
clusters from being autoscaled.

## How to run the service?

### Prerequisites

(at least) A BT cluster to autoscale???
Service account, with right permissions???
Java, maven???
Docker???
Postgres (optional)???

### Running

To run the bigtable-autoscaler service locally:

mvn clean install
???

## REST API

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

## Examples

???

## Development setup

This service needs to be run with a service account that has rights to
get and update cluster size.

The database schema is defined in schema.sql in this project.

## Code of conduct

This project adheres to the
[Open Code of Conduct](https://github.com/spotify/code-of-conduct/blob/master/code-of-conduct.md).
By participating, you are expected to honor this code.
