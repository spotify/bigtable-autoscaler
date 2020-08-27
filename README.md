# bigtable-autoscaler

[![CircleCI](https://circleci.com/gh/spotify/bigtable-autoscaler.svg?style=svg)](https://circleci.com/gh/spotify/bigtable-autoscaler)

If you have a Bigtable cluster and you would like to optimize its cost-efficiency by using the
right number of nodes at any given time you should consider using this Bigtable
autoscaler service! The Bigtable autoscaler lets you do that
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
* Java 11 and maven
* (Optional) **PostgreSQL database** for production use. In this quickstart session we're using a postgres docker image
* (Optional) We have a [make-file](https://www.gnu.org/software/make) with local development helper methods.

### Building

Run this command to build the project and create a docker image:

    mvn package

### Running

First review and edit [.env](.env) with your Google cloud credentials.
Start the service with docker-compose using a dockerized local postgres:

    # source your environment
    . ./.env
    # start the service with docker compose
    make up

    # see service logs
    make logs

Register the Bigtable cluster that should be autoscaled in the service:

```
PROJECT_ID=<YOUR GCP PROJECT ID>
INSTANCE_ID=<YOUR INSTANCE ID>
CLUSTER_ID=<YOUR CLUSTER ID>

curl -v -X POST "http://localhost:8080/clusters?projectId=$PROJECT_ID&instanceId=$INSTANCE_ID&clusterId=$CLUSTER_ID&minNodes=4&maxNodes=6&cpuTarget=0.8"
```

If the cluster was at 3 nodes, this will immediately rescale the cluster to 4 nodes as that's the
minimum threshold. If you generate some significant load to the cluster, it may scale up to 6 nodes.

Stop docker-compose:

    make down

### Using a Cloud SQL Postgres database as persistent storage

If you want to run this in production, consider using a Cloud SQL postgres database to store the
state. We recommend connecting using the [JDBC socket factory](https://cloud.google.com/sql/docs/postgres/connect-external-app#java).

Just update [.env](.env) with your postgres url, user and password and then run:

    # source your environment
    . ./.env
    # start the service with docker compose
    make run

This runs the same bigtable-autoscaler image, doesn't run postgres, and points bigtable-autoscaler to the postgresql you provided.

In the same way you can see service logs (make logs) and then to stop the service:

    make stop

## Registering Jersey Resources and Providers Dynamically
You can register any additional JAX-RS resource, JAX-RS or Jersey contract provider or JAX-RS feature by editing the
[config file](/src/main/resources/bigtable-autoscaler.conf).
You can either
* add a package to `additionalPackages` for any resource to be discovered. For this to work, resources to be discovered should be annotated.
* add a fully qualified class name to `additionalClasses` (semicolon separated).

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

## Development Status

**Beta**: We are using Bigtable Autoscaler in production clusters at Spotify, and we are actively developing it.

## FAQ

### Does it handle sudden load spikes, for instance Dataflow jobs reading/writing batch data?

Not on its own. In order to not overwhelm Bigtable, you can PUT to the `/clusers/override-min-nodes/` endpoint, passing it a number that basically overrides the min nodes count that the autoscaler must immediately respect. The [official Google documentation](https://cloud.google.com/bigtable/docs/scaling) states that if you are doing big batch jobs, you should rescale in advance and wait up to 20 minutes before starting the actual job. Then, of course, reset it back to 0 once the job has completed.

We realize that this can be inconvenient and welcome any ideas on how to approach this problem better.

### Does it enforce storage constraints?

Yes.

Since July 1st 2018 Google enforces storage limits on Bigtable nodes. In particular each Bigtable node will be able to handle at most 8Tb on HDD clusters and 2.5Tb on SSD clusters (for more info take a look [here](https://cloud.google.com/bigtable/quotas#storage-per-node)). Writes will fail until these conditions are not satisfied. The autoscaler will make sure that these constraints are respected and prefer those to the CPU target in that situation.

### Does it take project quotas into account?

No!

A resize command may fail if you don't have enough quota in the GCP project. This will be logged
as an error.

### Can I add an additional logic to resize the number of nodes?

Yes!

We increased the project's modularity, so you can create your custom strategy in your project,
which uses the Bigtable Autoscaler as a dependency, and implement the class "Algorithm".
If you add the class path of your new custom strategy in the column `extra_enabled_algorithms`, it
will be considered for upscaling the cluster.

Note that the recommended number of nodes will be the higher between the strategies in this
project (CPU + Storage constraints), and your custom strategies.

## API

See the [API doc](api.md)

## Code of conduct

This project adheres to the
[Open Code of Conduct](https://github.com/spotify/code-of-conduct/blob/master/code-of-conduct.md).
By participating, you are expected to honor this code.
