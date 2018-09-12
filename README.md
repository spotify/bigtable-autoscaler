# bigtable-autoscaler

You have a Bigtable cluster and you would like to optimize its cost by using the
right number of nodes at any given time. Then you should consider using the Bigtable
autoscaler service!

Many Bigtable clusters have uneven load over time. To avoid wasting capacity (and money), it's 
desirable to scale down the cluster during off-hours. The Bigtable autoscaler lets you do that 
with no manual intervention.

## Getting started

### Prerequisites

* A production Bigtable cluster (or several) to autoscale (development clusters can't be scaled)
* Service account JSON key that has relevant access to the Bigtable clusters to autoscale. See [Google's documentation](https://cloud.google.com/iam/docs/creating-managing-service-account-keys) on how to create a key.
    * If the autoscaler is running in the same GCP project as all the Bigtable clusters, the Compute Engine Default Service Account is sufficient.
    * The minimum permissions are:
        * Role **Bigtable Administrator**, in particular the permissions
            * bigtable.clusters.get
            * bigtable.clusters.update
        * Role **Monitoring Viewer**, in particular the permissions
            * monitoring.timeSeries.list
* Docker
* PostgreSQL database (for production use; in this quickstart session we're using a postgres docker 
image)
* Java 8 (the service is not compatible with Java 9 or later at the moment)

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
 minimum threshold. If you generate some significant load to the cluster, it may scale up to 6 
 nodes.

### Using a Cloud SQL Postgres database as persistent storage

If you want to run this in production, consider using a Cloud SQL postgres database to store the 
state. We recommend connecting using the [JDBC socket factory](https://cloud.google.com/sql/docs/postgres/connect-external-app#java). You can specify the jdbcUrl either in a 
custom config file or as an environment variable. 
 
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

## FAQ

### Does it handle sudden load spikes, for instance Dataflow jobs reading/writing batch data?

Not well enough. Online requests may be slowed when the job starts. We are working on improving 
this.

### Does it enforce storage constraints?

Yes.

Since July 1st 2018 Google enforces storage limits on Bigtable nodes. In particular each Bigtable node will be able to handle at most 8Tb on HDD clusters and 2.5Tb on SSD clusters (for more info take a look here) Writes will fail until these conditions are not satisfied. The autoscaler will make sure that these constraints are respected and prefer those to the CPU target in that situation.

### Does it take project quotas into account?

No!

A resize command may fail if you don't have enough quota in the GCP project. This will be logged 
as an error.

## API

See the [API doc](api.md)

## Code of conduct

This project adheres to the
[Open Code of Conduct](https://github.com/spotify/code-of-conduct/blob/master/code-of-conduct.md).
By participating, you are expected to honor this code.
