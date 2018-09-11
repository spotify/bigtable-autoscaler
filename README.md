# bigtable-autoscaler

You have a Bigtable cluster and you would like to optimize its cost by using the
right number of nodes at any given time. Then you should consider using the Bigtable
autoscaler service!

Many Bigtable clusters have uneven load over time. To avoid wasting capacity (and money), it's desirable to scale down the cluster during off-hours. The Bigtable autoscaler lets you do that without being hands-on.

## Getting started

### Prerequisites

* A production Bigtable cluster (or several) to autoscale.
* Service account JSON key that has relevant access to the Bigtable clusters to autoscale. See [Google's documentation](https://cloud.google.com/iam/docs/creating-managing-service-account-keys) on how to create a key.
    * If the autoscaler is running in the same GCP project as all the Bigtable clusters, the Compute Engine Default Service Account is sufficient.
    * The minimum permissions are:
        * Role **Bigtable Administrator**, in particular
            * bigtable.clusters.get
            * bigtable.clusters.update
        * Role **Monitoring Viewer**, in particular
            * monitoring.timeSeries.list
* Docker.
* PostgreSQL database (for production use; if only trying out ???).
* Java 8 (java 9+ can't run it).

### Building

Run these commands to build the project and create a docker image:

    mvn package
    docker build .

### Running

Start the service using a dockerized PostgreSQL instance:

    GOOGLE_APPLICATION_CREDENTIALS=<credentials json file> docker-compose -f quickstart.yml up

Register the Bigtable cluster that should be autoscaled in the service:

    curl -X POST "http://localhost:8080/clusters?projectId=<gcp-project-id>&instanceId=<bigtable-instance-id>&<bigtable-cluster-id>&minNodes=4&maxNodes=6&cpuTarget=0.8"

If the cluster was at 3 nodes, this will immediately rescale the cluster to 4 nodes as that's the
 minimum threshold. By generating some load to the cluster:

    TODO

It will soon autoscale up to 6 nodes.

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

## Does it enforce storage constraints?

Yes.

Since July 1st 2018 Google enforces storage limits on Bigtable nodes. In particular each Bigtable node will be able to handle at most 8Tb on HDD clusters and 2.5Tb on SSD clusters (for more info take a look here) Writes will fail until these conditions are not satisfied. The autoscaler will make sure that these constraints are respected and prefer those to the CPU target in that situation.

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

## Development setup

???

The database schema is defined in schema.sql in this project.

## Code of conduct

This project adheres to the
[Open Code of Conduct](https://github.com/spotify/code-of-conduct/blob/master/code-of-conduct.md).
By participating, you are expected to honor this code.
